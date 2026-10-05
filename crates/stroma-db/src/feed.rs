//! The change feed: a bounded journal of what each durable batch touched, read after authz.
//!
//! The write path records, per tail drain, the `(node, predicate)` keys it touched, the nodes whose
//! attributes it wrote and which of those are new, and the access labels of the rows it wrote on
//! each key. Readers ask for everything after a cursor (a durable head) and get it filtered through
//! their label mask: node labels hide a node's changes, and a touched predicate is shown only when a
//! row the batch wrote on it is visible (D35). The feed carries ids, type names and predicate names,
//! never values. A cursor the journal can no longer answer exactly gets `resync` instead of a gap.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

use serde_json::{Value, json};
use stromadb_core::catalog::Catalog;
use stromadb_core::fact::{FieldId, NodeId};
use stromadb_core::fold::Snapshot;
use stromadb_core::mask::FactMask;

/// Node changes retained across all journaled batches; older batches are dropped first.
pub const FEED_CAP: usize = 4096;

/// Node changes one batch may journal. A larger batch is journaled as an overflow marker, and a
/// reader whose window includes it gets `resync`: past this size a full re-read is cheaper than
/// applying the changes one by one anyway.
pub const FEED_BATCH_MAX: usize = 1024;

/// What the write path noted since the last drain: the labels of the rows written per key (`None`
/// = an unlabeled row) and the nodes that joined the node set.
#[derive(Default)]
pub(crate) struct Pending {
    keys: BTreeMap<(NodeId, FieldId), Vec<Option<u8>>>,
    new_nodes: HashSet<NodeId>,
}

impl Pending {
    /// A row with access label `label` was written on `key`.
    pub(crate) fn note(&mut self, key: (NodeId, FieldId), label: Option<u8>) {
        let labels = self.keys.entry(key).or_default();
        if !labels.contains(&label) {
            labels.push(label);
        }
    }

    /// `node` got its first type or label.
    pub(crate) fn note_new(&mut self, node: NodeId) {
        self.new_nodes.insert(node);
    }

    pub(crate) fn clear(&mut self) {
        self.keys.clear();
        self.new_nodes.clear();
    }
}

/// One node's share of a batch.
struct NodeChange {
    node: NodeId,
    new: bool,
    /// The batch wrote the node's type or label.
    attrs: bool,
    /// Touched predicates with the labels of the rows written on them. An empty label list means
    /// the write path noted none, and the read falls back to the key's current labels.
    preds: Vec<(FieldId, Vec<Option<u8>>)>,
}

enum Batch {
    Changes(Vec<NodeChange>),
    /// More than [`FEED_BATCH_MAX`] nodes: not journaled, readers resync.
    Overflow,
}

impl Batch {
    fn weight(&self) -> usize {
        match self {
            Batch::Changes(c) => c.len(),
            Batch::Overflow => 1,
        }
    }
}

/// The bounded journal, oldest batch first.
pub(crate) struct Journal {
    entries: VecDeque<(u64, Batch)>,
    held: usize,
    /// Batches at or below this head may be missing: cursors behind it resync.
    truncated_to: u64,
    cap: usize,
    batch_max: usize,
}

/// A read of the feed: the head it answers up to and the visible changes after the cursor, one
/// list per batch, oldest first. `resync` means the changes after the cursor are not available
/// (the cursor fell behind the retained window, a batch was too large to journal, or the cursor is
/// ahead of the head, as after a reset); re-read the current state and continue from `head`.
#[derive(Debug, Clone, PartialEq)]
pub struct Feed {
    pub head: u64,
    pub batches: Vec<(u64, Vec<Value>)>,
    pub resync: bool,
}

impl Feed {
    /// Every batch's changes folded into one entry per node, in order of first appearance:
    /// predicates are unioned and `new` is set if any batch created the node.
    pub fn merged(&self) -> Vec<Value> {
        let mut order: Vec<u64> = Vec::new();
        let mut by_node: BTreeMap<u64, Value> = BTreeMap::new();
        for (_, changes) in &self.batches {
            for c in changes {
                let Some(id) = c["node"].as_u64() else {
                    continue;
                };
                match by_node.get_mut(&id) {
                    None => {
                        order.push(id);
                        by_node.insert(id, c.clone());
                    }
                    Some(m) => {
                        if c["new"].as_bool() == Some(true) {
                            m["new"] = json!(true);
                        }
                        if !c["type"].is_null() {
                            m["type"] = c["type"].clone();
                        }
                        if let (Some(have), Some(more)) =
                            (m["predicates"].as_array_mut(), c["predicates"].as_array())
                        {
                            for p in more {
                                if !have.contains(p) {
                                    have.push(p.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
        order
            .into_iter()
            .filter_map(|id| by_node.remove(&id))
            .collect()
    }

    /// `{"head", "changes": [..]}` with the batches merged, plus `"resync": true` when set.
    pub fn to_json(&self) -> Value {
        let mut v = json!({ "head": self.head, "changes": self.merged() });
        if self.resync {
            v["resync"] = json!(true);
        }
        v
    }
}

impl Journal {
    /// An empty journal for a database whose durable head is `head`: nothing before it is known.
    pub(crate) fn new(head: u64) -> Journal {
        Journal::with_caps(head, FEED_CAP, FEED_BATCH_MAX)
    }

    pub(crate) fn with_caps(head: u64, cap: usize, batch_max: usize) -> Journal {
        Journal {
            entries: VecDeque::new(),
            held: 0,
            truncated_to: head,
            cap,
            batch_max,
        }
    }

    /// The database was cleared: forget every batch. The head restarts from zero, so any older
    /// cursor is now ahead of the head and resyncs.
    pub(crate) fn reset(&mut self) {
        self.entries.clear();
        self.held = 0;
        self.truncated_to = 0;
    }

    /// Journal one drained batch ending at durable `head`: the keys and node attributes the drain
    /// applied, with the labels and new nodes the write path noted (`pending` is consumed).
    pub(crate) fn record(
        &mut self,
        head: u64,
        keys: &BTreeSet<(NodeId, FieldId)>,
        nodes: &BTreeSet<NodeId>,
        pending: &mut Pending,
    ) {
        let mut by_node: BTreeMap<NodeId, NodeChange> = BTreeMap::new();
        fn change(by_node: &mut BTreeMap<NodeId, NodeChange>, n: NodeId) -> &mut NodeChange {
            by_node.entry(n).or_insert_with(|| NodeChange {
                node: n,
                new: false,
                attrs: false,
                preds: Vec::new(),
            })
        }
        for &(n, p) in keys {
            let labels = pending.keys.remove(&(n, p)).unwrap_or_default();
            change(&mut by_node, n).preds.push((p, labels));
        }
        for &n in nodes {
            let c = change(&mut by_node, n);
            c.attrs = true;
            c.new = pending.new_nodes.contains(&n);
        }
        pending.clear();
        if by_node.is_empty() {
            return;
        }
        let batch = if by_node.len() > self.batch_max {
            Batch::Overflow
        } else {
            Batch::Changes(by_node.into_values().collect())
        };
        self.held += batch.weight();
        self.entries.push_back((head, batch));
        while self.held > self.cap {
            let Some((h, b)) = self.entries.pop_front() else {
                break;
            };
            self.held -= b.weight();
            self.truncated_to = h;
        }
    }

    /// The changes after `since` up to `head` (the reader's pinned view), as a principal with
    /// `allowed` sees them over `snap`/`cat`.
    pub(crate) fn read(
        &self,
        since: u64,
        head: u64,
        snap: &Snapshot,
        cat: &Catalog,
        allowed: u32,
    ) -> Feed {
        let resync = Feed {
            head,
            batches: Vec::new(),
            resync: true,
        };
        if since > head || since < self.truncated_to {
            return resync;
        }
        let mask = FactMask::new(cat, allowed);
        let masking = mask.hides_any(snap);
        let node_visible = |n: NodeId| {
            snap.node_labels
                .get(&n)
                .is_none_or(|&l| (allowed >> l) & 1 == 1)
        };
        let mut batches = Vec::new();
        for (h, batch) in &self.entries {
            if *h <= since || *h > head {
                continue;
            }
            let changes = match batch {
                Batch::Overflow => return resync,
                Batch::Changes(c) => c,
            };
            let visible: Vec<Value> = changes
                .iter()
                .filter(|c| node_visible(c.node))
                .filter_map(|c| {
                    let preds: Vec<&str> = c
                        .preds
                        .iter()
                        .filter(|(p, labels)| {
                            !masking
                                || match labels.is_empty() {
                                    true => !mask.key_affected(snap, &(c.node, *p)),
                                    false => labels.iter().any(|&l| mask.row_visible(*p, l)),
                                }
                        })
                        .filter_map(|(p, _)| cat.name(*p))
                        .collect();
                    (c.attrs || !preds.is_empty()).then(|| {
                        json!({
                            "node": c.node,
                            "type": snap.node_types.get(&c.node).and_then(|&t| cat.name(t)),
                            "predicates": preds,
                            "new": c.new,
                        })
                    })
                })
                .collect();
            if !visible.is_empty() {
                batches.push((*h, visible));
            }
        }
        Feed {
            head,
            batches,
            resync: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(ns: &[u64]) -> BTreeSet<NodeId> {
        ns.iter().copied().collect()
    }

    // The ring keeps at most `cap` node changes; a cursor older than what it dropped resyncs, a
    // cursor inside the window reads exactly the later batches.
    #[test]
    fn bounded_ring_resyncs_behind_the_window() {
        let mut j = Journal::with_caps(0, 3, 10);
        let snap = Snapshot::default();
        let cat = Catalog::default();
        for head in 1..=5u64 {
            j.record(
                head,
                &BTreeSet::new(),
                &nodes(&[head]),
                &mut Pending::default(),
            );
        }
        assert_eq!(j.held, 3);
        assert_eq!(j.truncated_to, 2);
        assert!(j.read(0, 5, &snap, &cat, u32::MAX).resync);
        assert!(j.read(1, 5, &snap, &cat, u32::MAX).resync);
        let f = j.read(2, 5, &snap, &cat, u32::MAX);
        assert!(!f.resync);
        assert_eq!(
            f.batches.iter().map(|(h, _)| *h).collect::<Vec<_>>(),
            [3, 4, 5]
        );
        // a reader pinned at an older view does not see batches past its head
        let f = j.read(2, 4, &snap, &cat, u32::MAX);
        assert_eq!(f.batches.len(), 2);
        // a cursor ahead of the head (a reset happened) resyncs
        assert!(j.read(9, 5, &snap, &cat, u32::MAX).resync);
    }

    #[test]
    fn an_oversized_batch_is_a_resync_marker() {
        let mut j = Journal::with_caps(0, 100, 2);
        let snap = Snapshot::default();
        let cat = Catalog::default();
        j.record(
            1,
            &BTreeSet::new(),
            &nodes(&[1, 2, 3]),
            &mut Pending::default(),
        );
        j.record(2, &BTreeSet::new(), &nodes(&[1]), &mut Pending::default());
        assert!(j.read(0, 2, &snap, &cat, u32::MAX).resync);
        // past the oversized batch the journal answers again
        let f = j.read(1, 2, &snap, &cat, u32::MAX);
        assert!(!f.resync);
        assert_eq!(f.batches.len(), 1);
    }

    #[test]
    fn merged_folds_batches_per_node() {
        let f = Feed {
            head: 3,
            batches: vec![
                (
                    2,
                    vec![
                        json!({"node": 7, "type": null, "predicates": ["a"], "new": true}),
                        json!({"node": 8, "type": "T", "predicates": [], "new": false}),
                    ],
                ),
                (
                    3,
                    vec![json!({"node": 7, "type": "T", "predicates": ["a", "b"], "new": false})],
                ),
            ],
            resync: false,
        };
        let m = f.merged();
        assert_eq!(
            m,
            vec![
                json!({"node": 7, "type": "T", "predicates": ["a", "b"], "new": true}),
                json!({"node": 8, "type": "T", "predicates": [], "new": false}),
            ]
        );
        assert_eq!(f.to_json()["head"], 3);
        assert!(f.to_json().get("resync").is_none());
    }
}
