//! The engine: write-append + read-merge over the changelog and fold (CAP-1, CAP-4).
//!
//! Writes are appended to the changelog (the version authority). A read merges the materialized
//! `base` fold with the bounded un-materialized tail on demand — so partial updates are not
//! re-written and the un-merged backlog stays `<= n_max` (the changelog applies backpressure at that
//! bound). `materialize()` folds the tail into the base and relieves it.

use crate::changelog::{Backpressure, Changelog, WriteKind};
use crate::fact::{FieldId, NodeId};
use crate::fold::{Fold, ObjKey, Snapshot};

pub struct Engine {
    changelog: Changelog,
    base: Fold, // materialized fold of records [0, watermark)
    watermark: u64,
}

impl Engine {
    /// `n_max` bounds the un-merged (appended-but-not-materialized) backlog.
    pub fn new(n_max: usize) -> Self {
        Engine {
            changelog: Changelog::new(n_max),
            base: Fold::default(),
            watermark: 0,
        }
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

    /// Fold the tail `[watermark, head)` into the base and advance the watermark (relieves backpressure).
    pub fn materialize(&mut self) {
        self.changelog
            .replay_range_into(self.watermark, &mut self.base);
        self.watermark = self.changelog.head();
        self.changelog.mark_materialized(self.watermark);
    }

    /// The effective fold: materialized base merged with the bounded un-materialized tail.
    fn effective_fold(&self) -> Fold {
        let mut eff = self.base.clone();
        self.changelog.replay_range_into(self.watermark, &mut eff);
        eff
    }

    /// Read-merge: base ∪ bounded tail, observed as a canonical snapshot.
    pub fn snapshot(&self) -> Snapshot {
        self.effective_fold().observe()
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
