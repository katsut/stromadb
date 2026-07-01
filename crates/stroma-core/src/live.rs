//! Live Query: registered queries whose result *diffs* are pushed as the graph changes (CAP-5).
//!
//! A live query is any function from a [`Snapshot`] to a node set (the monotonic/bounded-diff class:
//! filter / expand / equi-join / windowed aggregate). On each engine change the registry re-evaluates
//! and emits only the delta (added/removed) — the same query operators as one-shot reads (CAP-10,
//! single algebra).
//!
//! This MVP recomputes-and-diffs (correct, simple). The efficient differential-dataflow backend
//! (validated in Phase 0 `poc-rkyv-ivm`: incremental arrangements over rkyv zero-copy facts) slots
//! in behind the same register/diff contract for hot-path efficiency.

use std::collections::BTreeSet;

use crate::fact::NodeId;
use crate::fold::Snapshot;

/// A result delta: node ids that entered / left the result.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Diff {
    pub added: BTreeSet<NodeId>,
    pub removed: BTreeSet<NodeId>,
}

impl Diff {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }

    fn between(old: &BTreeSet<NodeId>, new: &BTreeSet<NodeId>) -> Diff {
        Diff {
            added: new.difference(old).copied().collect(),
            removed: old.difference(new).copied().collect(),
        }
    }
}

pub type QueryId = u64;

/// Returned when the live-query cap is reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AtCapacity;

type Eval = Box<dyn Fn(&Snapshot) -> BTreeSet<NodeId> + Send>;

struct Live {
    id: QueryId,
    eval: Eval,
    last: BTreeSet<NodeId>,
}

/// A bounded registry of live queries (CAP-5: live-query count is capped).
pub struct LiveQueries {
    next: QueryId,
    queries: Vec<Live>,
    max: usize,
}

impl LiveQueries {
    pub fn new(max: usize) -> Self {
        LiveQueries {
            next: 0,
            queries: Vec::new(),
            max,
        }
    }

    pub fn len(&self) -> usize {
        self.queries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }

    /// Register a live query; returns its id and the initial result as an all-added diff.
    /// Errors with [`AtCapacity`] once `max` live queries are registered.
    pub fn register(
        &mut self,
        snapshot: &Snapshot,
        eval: impl Fn(&Snapshot) -> BTreeSet<NodeId> + Send + 'static,
    ) -> Result<(QueryId, Diff), AtCapacity> {
        if self.queries.len() >= self.max {
            return Err(AtCapacity);
        }
        let id = self.next;
        self.next += 1;
        let last = eval(snapshot);
        let initial = Diff {
            added: last.clone(),
            removed: BTreeSet::new(),
        };
        self.queries.push(Live {
            id,
            eval: Box::new(eval),
            last,
        });
        Ok((id, initial))
    }

    /// Deregister a live query.
    pub fn deregister(&mut self, id: QueryId) {
        self.queries.retain(|q| q.id != id);
    }

    /// Re-evaluate all live queries against a new snapshot; return the non-empty diffs to push.
    pub fn on_change(&mut self, snapshot: &Snapshot) -> Vec<(QueryId, Diff)> {
        let mut out = Vec::new();
        for q in &mut self.queries {
            let new = (q.eval)(snapshot);
            let diff = Diff::between(&q.last, &new);
            if !diff.is_empty() {
                q.last = new;
                out.push((q.id, diff));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WriteKind;
    use crate::engine::Engine;
    use crate::fold::ObjKey;
    use crate::query;

    fn add(s: u64, p: u32, o: u64) -> WriteKind {
        WriteKind::AddMany {
            subject: s,
            predicate: p,
            object: ObjKey::Node(o),
        }
    }

    #[test]
    fn pushes_added_and_removed_diffs() {
        let mut e = Engine::new(1024);
        e.write(0, add(1, 100, 20)).unwrap();
        let mut live = LiveQueries::new(8);
        // live query: 1-hop expand of subject 1 over predicate 100
        let (_id, initial) = live
            .register(&e.snapshot(), |s| query::expand(s, 1, 100))
            .unwrap();
        assert_eq!(initial.added, [20].into_iter().collect());

        // add an edge → diff has it added
        e.write(0, add(1, 100, 21)).unwrap();
        let diffs = live.on_change(&e.snapshot());
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].1.added, [21].into_iter().collect());
        assert!(diffs[0].1.removed.is_empty());

        // no change → no diff pushed
        assert!(live.on_change(&e.snapshot()).is_empty());

        // remove (observed-remove the tag for object 20) → diff has it removed
        // tag for (1,100,20) was the first write, seqno 0.
        e.write(
            0,
            WriteKind::RemoveMany {
                subject: 1,
                predicate: 100,
                observed: vec![crate::fold::OrderKey {
                    tx: 0,
                    source: 0,
                    seq: 0,
                }],
            },
        )
        .unwrap();
        let diffs = live.on_change(&e.snapshot());
        assert_eq!(diffs[0].1.removed, [20].into_iter().collect());
    }

    #[test]
    fn capacity_is_bounded() {
        let e = Engine::new(16);
        let mut live = LiveQueries::new(1);
        assert!(
            live.register(&e.snapshot(), |s| query::expand(s, 1, 100))
                .is_ok()
        );
        assert_eq!(
            live.register(&e.snapshot(), |s| query::expand(s, 2, 100))
                .err(),
            Some(AtCapacity)
        );
    }
}
