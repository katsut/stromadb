//! The engine: write-append + read-merge over the changelog and fold (CAP-1, CAP-4).
//!
//! Writes are appended to the changelog (the version authority). A read merges the materialized
//! `base` fold with the bounded un-materialized tail on demand — so partial updates are not
//! re-written and the un-merged backlog stays `<= n_max` (the changelog applies backpressure at that
//! bound). `materialize()` folds the tail into the base and relieves it.

use crate::changelog::{Backpressure, Changelog, WriteKind};
use crate::fact::{FieldId, NodeId};
use crate::fold::{Fold, ObjKey, Snapshot};
use std::io;
use std::path::Path;
use std::sync::Arc;

pub struct Engine {
    changelog: Changelog,
    base: Fold,               // materialized fold of records [0, watermark)
    base_snap: Arc<Snapshot>, // observed view of `base`, refreshed incrementally at materialize
    watermark: u64,
}

impl Engine {
    /// `n_max` bounds the un-merged (appended-but-not-materialized) backlog.
    /// In-memory only — see [`Engine::open`] for the durable variant.
    pub fn new(n_max: usize) -> Self {
        Engine {
            changelog: Changelog::new(n_max),
            base: Fold::default(),
            base_snap: Arc::new(Snapshot::default()),
            watermark: 0,
        }
    }

    /// Open a durable engine backed by the WAL at `path`: recover the committed changelog prefix and
    /// rebuild the fold from it (the cold-start replay = recovery-time-objective path). Writes become
    /// durable via [`Engine::sync`]. A missing file starts empty.
    pub fn open(path: impl AsRef<Path>, n_max: usize) -> io::Result<Self> {
        let changelog = Changelog::open(path, n_max)?;
        let mut base = Fold::default();
        changelog.replay_into(&mut base); // fold the whole recovered log = cold-start rebuild
        let base_snap = Arc::new(base.observe());
        let watermark = changelog.head();
        Ok(Engine {
            changelog,
            base,
            base_snap,
            watermark,
        })
    }

    /// Durability commit point: fsync every appended-but-not-yet-durable write (group commit — the
    /// caller picks the boundary, typically per ETL chunk). No-op in in-memory mode.
    pub fn sync(&mut self) -> io::Result<()> {
        self.changelog.sync()
    }

    /// Seqno up to which writes are guaranteed durable (fsynced); `0` in in-memory mode.
    pub fn durable_head(&self) -> u64 {
        self.changelog.durable_head()
    }

    /// Append a write; returns its seqno or [`Backpressure`] when the un-merged backlog is full
    /// (call [`Engine::materialize`] to relieve it).
    pub fn write(&mut self, source: FieldId, kind: WriteKind) -> Result<u64, Backpressure> {
        self.changelog.append(source, kind)
    }

    /// Append a chunk of writes atomically (the ETL chunk receiver). Returns their seqnos.
    pub fn write_batch(
        &mut self,
        writes: Vec<(FieldId, WriteKind)>,
    ) -> Result<Vec<u64>, Backpressure> {
        self.changelog.append_batch(writes)
    }

    /// Retract a cardinality-Many edge by `(subject, predicate, object)`: the DB resolves the
    /// currently-observed OR-Set tags from the effective state and appends the observed-remove — so
    /// ETL says "remove this edge" without knowing OR-Set internals (the DB↔ETL diff-reflection
    /// resolver). Returns the seqno, or `None` if the edge isn't present.
    pub fn retract_edge(
        &mut self,
        source: FieldId,
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
    ) -> Result<Option<u64>, Backpressure> {
        let observed = self.effective_fold().live_tags(subject, predicate, &object);
        if observed.is_empty() {
            return Ok(None);
        }
        let seqno = self.changelog.append(
            source,
            WriteKind::RemoveMany {
                subject,
                predicate,
                observed,
            },
        )?;
        Ok(Some(seqno))
    }

    /// Fold the tail `[watermark, head)` into the base and advance the watermark (relieves
    /// backpressure). Also refreshes the cached observed snapshot incrementally — only the
    /// `(subject, predicate)` keys the tail touched are re-observed, so the fresh-view cost is
    /// O(tail), not O(state).
    pub fn materialize(&mut self) {
        let _ = self.materialize_tracked();
    }

    /// Like [`Engine::materialize`], but returns the `(subject, predicate)` keys the drained tail
    /// touched — the driver for incremental Live-Query maintenance (`incremental::Maintained`):
    /// re-check only the rules whose inputs changed instead of recomputing over the whole graph.
    pub fn materialize_tracked(&mut self) -> std::collections::BTreeSet<(NodeId, FieldId)> {
        let touched = self
            .changelog
            .replay_range_into_tracked(self.watermark, &mut self.base);
        if !touched.is_empty() {
            let snap = Arc::make_mut(&mut self.base_snap);
            for k in &touched {
                self.base.observe_key_into(k, snap);
            }
        }
        self.watermark = self.changelog.head();
        self.changelog.mark_materialized(self.watermark);
        touched
    }

    /// The effective fold: materialized base merged with the bounded un-materialized tail.
    fn effective_fold(&self) -> Fold {
        let mut eff = self.base.clone();
        self.changelog.replay_range_into(self.watermark, &mut eff);
        eff
    }

    /// Read-merge: base ∪ bounded tail, observed as a canonical snapshot. O(state) — prefer the
    /// fresh-loop path (`materialize` then [`Engine::snapshot_arc`]) on hot paths.
    pub fn snapshot(&self) -> Snapshot {
        self.effective_fold().observe()
    }

    /// The cached observed snapshot of the materialized base, shared O(1). The cheap fresh-read
    /// loop is `write → materialize() (O(tail)) → snapshot_arc()`: after materialize this equals
    /// [`Engine::snapshot`] (merged read ≡ post-materialize read) without the O(state) rebuild.
    /// Readers holding an old Arc keep a stable view; the next materialize copies-on-write.
    pub fn snapshot_arc(&self) -> Arc<Snapshot> {
        Arc::clone(&self.base_snap)
    }

    /// Number of appended-but-not-materialized records (the read-merge tail length, `<= n_max`).
    pub fn unmerged(&self) -> usize {
        (self.changelog.head() - self.watermark) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold::ObjKey;
    use crate::query;

    fn add(s: u64, p: FieldId, o: u64) -> WriteKind {
        WriteKind::AddMany {
            subject: s,
            predicate: p,
            object: ObjKey::Node(o),
        }
    }

    #[test]
    fn read_merge_reflects_unmaterialized_tail() {
        let mut e = Engine::new(64);
        e.write(0, add(1, 100, 20)).unwrap();
        e.write(0, add(1, 100, 21)).unwrap();
        // not materialized yet — read-merge still sees them
        assert_eq!(e.unmerged(), 2);
        assert_eq!(
            query::point_many(&e.snapshot(), 1, 100),
            [ObjKey::Node(20), ObjKey::Node(21)].into_iter().collect()
        );
        // materialize: same observation, tail drained
        e.materialize();
        assert_eq!(e.unmerged(), 0);
        assert_eq!(
            query::point_many(&e.snapshot(), 1, 100),
            [ObjKey::Node(20), ObjKey::Node(21)].into_iter().collect()
        );
    }

    #[test]
    fn backpressure_at_n_max_relieved_by_materialize() {
        let mut e = Engine::new(2);
        assert!(e.write(0, add(1, 100, 1)).is_ok());
        assert!(e.write(0, add(1, 100, 2)).is_ok());
        assert!(e.write(0, add(1, 100, 3)).is_err()); // backlog full
        e.materialize();
        assert!(e.write(0, add(1, 100, 3)).is_ok()); // relieved
        assert_eq!(query::point_many(&e.snapshot(), 1, 100).len(), 3);
    }

    #[test]
    fn materialized_and_merged_reads_agree() {
        let mut e = Engine::new(64);
        e.write(
            0,
            WriteKind::SetOne {
                subject: 1,
                predicate: 0,
                object: ObjKey::Node(10),
                valid_from: 0,
            },
        )
        .unwrap();
        let merged = e.snapshot();
        e.materialize();
        assert_eq!(merged, e.snapshot());
        assert_eq!(
            query::point_one(&e.snapshot(), 1, 0),
            Some(ObjKey::Node(10))
        );
    }

    #[test]
    fn write_batch_is_atomic_wrt_backpressure() {
        let mut e = Engine::new(2);
        // a chunk larger than the backlog bound is rejected whole
        assert!(
            e.write_batch(vec![
                (0, add(1, 100, 1)),
                (0, add(1, 100, 2)),
                (0, add(1, 100, 3))
            ])
            .is_err()
        );
        // a fitting chunk lands and returns seqnos
        let seqnos = e
            .write_batch(vec![(0, add(1, 100, 1)), (0, add(1, 100, 2))])
            .unwrap();
        assert_eq!(seqnos, vec![0, 1]);
        assert_eq!(query::point_many(&e.snapshot(), 1, 100).len(), 2);
    }

    #[test]
    fn snapshot_arc_matches_full_snapshot_after_materialize() {
        let mut e = Engine::new(1024);
        // mixed workload incl. supersession and removal so incremental observe hits every path
        e.write(0, add(1, 100, 20)).unwrap();
        e.write(0, add(1, 100, 21)).unwrap();
        e.materialize();
        let held = e.snapshot_arc(); // reader pins a view
        e.write(
            0,
            WriteKind::SetOne {
                subject: 2,
                predicate: 0,
                object: ObjKey::Node(9),
                valid_from: 0,
            },
        )
        .unwrap();
        e.retract_edge(0, 1, 100, ObjKey::Node(20)).unwrap();
        e.write(
            0,
            WriteKind::SetOne {
                subject: 2,
                predicate: 0,
                object: ObjKey::Node(10), // supersedes 9
                valid_from: 1,
            },
        )
        .unwrap();
        e.materialize();
        // incremental refresh ≡ full observe
        assert_eq!(*e.snapshot_arc(), e.snapshot());
        // pinned reader view unchanged (copy-on-write)
        assert_eq!(
            query::point_many(&held, 1, 100),
            [ObjKey::Node(20), ObjKey::Node(21)].into_iter().collect()
        );
        assert!(query::point_one(&held, 2, 0).is_none());
    }

    #[test]
    fn durable_engine_recovers_state_cold() {
        let path = std::env::temp_dir().join("stroma_engine_recover.log");
        let _ = std::fs::remove_file(&path);
        let expected = {
            let mut e = Engine::open(&path, 1024).unwrap();
            e.write_batch(vec![(0, add(1, 100, 20)), (0, add(1, 100, 21))])
                .unwrap();
            e.write(
                0,
                WriteKind::SetOne {
                    subject: 1,
                    predicate: 0,
                    object: ObjKey::Node(9),
                    valid_from: 0,
                },
            )
            .unwrap();
            e.sync().unwrap();
            assert_eq!(e.durable_head(), 3);
            e.snapshot()
        };
        // cold restart: rebuild from the WAL, state must match
        let e2 = Engine::open(&path, 1024).unwrap();
        assert_eq!(e2.unmerged(), 0);
        assert_eq!(e2.snapshot(), expected);
        assert_eq!(
            query::point_many(&e2.snapshot(), 1, 100),
            [ObjKey::Node(20), ObjKey::Node(21)].into_iter().collect()
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn retract_edge_resolves_or_set_tags() {
        let mut e = Engine::new(64);
        e.write(0, add(1, 100, 20)).unwrap();
        e.write(0, add(1, 100, 21)).unwrap();
        // ETL says "remove edge (1, 100, 20)"; the DB resolves the tag(s) and removes it.
        assert!(
            e.retract_edge(0, 1, 100, ObjKey::Node(20))
                .unwrap()
                .is_some()
        );
        assert_eq!(
            query::point_many(&e.snapshot(), 1, 100),
            [ObjKey::Node(21)].into_iter().collect()
        );
        // retracting an absent edge is a no-op (no observed tags)
        assert!(
            e.retract_edge(0, 1, 100, ObjKey::Node(99))
                .unwrap()
                .is_none()
        );
    }
}
