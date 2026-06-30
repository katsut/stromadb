//! The engine: write-append + read-merge over the changelog and fold (CAP-1, CAP-4).
//!
//! Writes are appended to the changelog (the version authority). A read merges the materialized
//! `base` fold with the bounded un-materialized tail on demand — so partial updates are not
//! re-written and the un-merged backlog stays `<= n_max` (the changelog applies backpressure at that
//! bound). `materialize()` folds the tail into the base and relieves it.

use crate::changelog::{Backpressure, Changelog, WriteKind};
use crate::fact::FieldId;
use crate::fold::{Fold, Snapshot};

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

    /// Fold the tail `[watermark, head)` into the base and advance the watermark (relieves backpressure).
    pub fn materialize(&mut self) {
        self.changelog
            .replay_range_into(self.watermark, &mut self.base);
        self.watermark = self.changelog.head();
        self.changelog.mark_materialized(self.watermark);
    }

    /// Read-merge: base ∪ bounded tail, observed as a canonical snapshot.
    pub fn snapshot(&self) -> Snapshot {
        let mut eff = self.base.clone();
        self.changelog.replay_range_into(self.watermark, &mut eff);
        eff.observe()
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
}
