//! The changelog: the append-only, version-authoritative write log (SoT).
//!
//! Every write is appended and assigned a monotonic `seqno` — the version authority. The seqno is
//! the basis of the fold's [`OrderKey`] (so deterministic replay reproduces the exact same state),
//! and the watermark other (derived) stores chase. Under overload the changelog returns explicit
//! [`Backpressure`] rather than stalling silently (CAP-1).
//!
//! This is the in-memory semantics layer. The durable backend (LSM: RocksDB/Speedb, rkyv zero-copy,
//! O_DIRECT, WAL) slots in behind the same append/replay/watermark contract in a later story.

use crate::catalog::Cardinality;
use crate::fact::{FieldId, NodeId};
use crate::fold::{Fold, ObjKey, Op, OrderKey};

/// A write submitted to the changelog — a diff *without* its order key (the changelog assigns it).
/// `RemoveMany` carries the order keys it observed (resolved by the ingest layer from current tags).
#[derive(Clone, Debug)]
pub enum WriteKind {
    SetOne {
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
        valid_from: i64,
    },
    CloseOne {
        subject: NodeId,
        predicate: FieldId,
        valid_from: i64,
    },
    AddMany {
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
    },
    RemoveMany {
        subject: NodeId,
        predicate: FieldId,
        observed: Vec<OrderKey>,
    },
    HardDelete {
        subject: NodeId,
        predicate: FieldId,
        cardinality: Cardinality,
    },
}

#[derive(Clone, Debug)]
struct Record {
    source: FieldId,
    kind: WriteKind,
}

/// Returned when the unmaterialized backlog would exceed the bound — apply backpressure upstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Backpressure {
    pub unmaterialized: usize,
    pub limit: usize,
}

/// Append-only changelog. `seqno` = index in the log = version authority.
pub struct Changelog {
    records: Vec<Record>,
    materialized: u64, // count of records a derived store has caught up to
    max_unmaterialized: usize,
}

fn record_to_op(seqno: u64, source: FieldId, kind: &WriteKind) -> Op {
    let ok = OrderKey {
        tx: seqno,
        source,
        seq: seqno,
    };
    match kind {
        WriteKind::SetOne {
            subject,
            predicate,
            object,
            valid_from,
        } => Op::SetOne {
            subject: *subject,
            predicate: *predicate,
            object: object.clone(),
            valid_from: *valid_from,
            ok,
        },
        WriteKind::CloseOne {
            subject,
            predicate,
            valid_from,
        } => Op::CloseOne {
            subject: *subject,
            predicate: *predicate,
            valid_from: *valid_from,
            ok,
        },
        WriteKind::AddMany {
            subject,
            predicate,
            object,
        } => Op::AddMany {
            subject: *subject,
            predicate: *predicate,
            object: object.clone(),
            ok,
        },
        WriteKind::RemoveMany {
            subject,
            predicate,
            observed,
        } => Op::RemoveMany {
            subject: *subject,
            predicate: *predicate,
            observed: observed.clone(),
        },
        WriteKind::HardDelete {
            subject,
            predicate,
            cardinality,
        } => Op::HardDelete {
            subject: *subject,
            predicate: *predicate,
            ok,
            cardinality: *cardinality,
        },
    }
}

impl Changelog {
    /// `max_unmaterialized` bounds the in-flight (appended but not-yet-materialized) backlog.
    pub fn new(max_unmaterialized: usize) -> Self {
        Changelog {
            records: Vec::new(),
            materialized: 0,
            max_unmaterialized,
        }
    }

    /// Append a write; returns its assigned `seqno`, or [`Backpressure`] if the backlog is full.
    pub fn append(&mut self, source: FieldId, kind: WriteKind) -> Result<u64, Backpressure> {
        let unmaterialized = self.records.len() - self.materialized as usize;
        if unmaterialized >= self.max_unmaterialized {
            return Err(Backpressure {
                unmaterialized,
                limit: self.max_unmaterialized,
            });
        }
        let seqno = self.records.len() as u64;
        self.records.push(Record { source, kind });
        Ok(seqno)
    }

    /// Next seqno that will be assigned (== current length).
    pub fn head(&self) -> u64 {
        self.records.len() as u64
    }

    /// Records appended but not yet materialized by a derived store.
    pub fn unmaterialized(&self) -> usize {
        self.records.len() - self.materialized as usize
    }

    /// Advance the materialized watermark (a derived store reports its progress, relieving backpressure).
    pub fn mark_materialized(&mut self, up_to: u64) {
        self.materialized = up_to.min(self.records.len() as u64).max(self.materialized);
    }

    /// Deterministically rebuild state by folding the whole log in seqno order.
    pub fn replay(&self) -> Fold {
        let mut f = Fold::default();
        self.replay_into(&mut f);
        f
    }

    /// Fold records `[from, head)` into an existing fold (incremental catch-up).
    pub fn replay_into(&self, fold: &mut Fold) {
        for (i, r) in self.records.iter().enumerate() {
            let op = record_to_op(i as u64, r.source, &r.kind);
            fold.apply(&op);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(s: NodeId, p: FieldId, o: u64) -> WriteKind {
        WriteKind::AddMany {
            subject: s,
            predicate: p,
            object: ObjKey::Node(o),
        }
    }

    #[test]
    fn append_assigns_monotonic_seqno() {
        let mut log = Changelog::new(1024);
        assert_eq!(log.append(0, add(1, 100, 2)), Ok(0));
        assert_eq!(log.append(0, add(1, 100, 3)), Ok(1));
        assert_eq!(log.head(), 2);
    }

    #[test]
    fn replay_is_deterministic_and_matches_fold() {
        let mut log = Changelog::new(1024);
        log.append(0, add(1, 100, 2)).unwrap();
        log.append(1, add(1, 100, 3)).unwrap();
        log.append(
            0,
            WriteKind::SetOne {
                subject: 1,
                predicate: 0,
                object: ObjKey::Node(9),
                valid_from: 0,
            },
        )
        .unwrap();
        let a = log.replay().observe();
        let b = log.replay().observe();
        assert_eq!(a, b);
        // many{(1,100)} = {2,3}; one{(1,0)} = 9
        assert_eq!(
            a.many.get(&(1, 100)).unwrap(),
            &[ObjKey::Node(2), ObjKey::Node(3)].into_iter().collect()
        );
        assert_eq!(a.one.get(&(1, 0)), Some(&Some(ObjKey::Node(9))));
    }

    #[test]
    fn backpressure_triggers_and_clears() {
        let mut log = Changelog::new(2);
        assert!(log.append(0, add(1, 100, 1)).is_ok());
        assert!(log.append(0, add(1, 100, 2)).is_ok());
        // backlog full (2 unmaterialized, limit 2)
        assert_eq!(
            log.append(0, add(1, 100, 3)),
            Err(Backpressure {
                unmaterialized: 2,
                limit: 2
            })
        );
        // a derived store catches up → backpressure relieved
        log.mark_materialized(2);
        assert_eq!(log.unmaterialized(), 0);
        assert!(log.append(0, add(1, 100, 3)).is_ok());
    }
}
