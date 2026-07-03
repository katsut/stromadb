//! Read primitives over a folded [`Snapshot`]: point lookup and 1–2 hop expand.
//!
//! These are the symbolic-core read operators (CAP-2). They operate on the merged snapshot the
//! engine produces (read-merge, see [`crate::engine`]); physical co-location (CSR adjacency) is a
//! later optimization that does not change these contracts.

use std::collections::BTreeSet;

use crate::fact::{FieldId, NodeId};
use crate::fold::{ObjKey, Snapshot};

/// Current functional value of a cardinality-One `(subject, predicate)` (None if absent or closed).
pub fn point_one(snap: &Snapshot, subject: NodeId, predicate: FieldId) -> Option<ObjKey> {
    snap.one.get(&(subject, predicate)).cloned().flatten()
}

/// Valid-time as-of read of a cardinality-One `(subject, predicate)`: the value in effect at
/// valid-time `at`. Among the retained version rows with `valid_from <= at`, the one with the
/// greatest `valid_from` wins (ties broken by the later write = a retroactive correction). Returns
/// `None` if nothing was valid yet at `at`, or the effective version closed the value. This is
/// *valid-time* as-of (bitemporal, single-valued); transaction-time as-of is the version-vector pin.
pub fn point_one_asof(
    snap: &Snapshot,
    subject: NodeId,
    predicate: FieldId,
    at: i64,
) -> Option<ObjKey> {
    snap.one_history
        .get(&(subject, predicate))?
        .iter()
        .filter(|(_ok, _obj, valid_from)| *valid_from <= at)
        .max_by(|a, b| a.2.cmp(&b.2).then(a.0.cmp(&b.0)))
        .and_then(|(_ok, obj, _vf)| obj.clone())
}

/// Present element set of a cardinality-Many `(subject, predicate)` (empty if absent).
pub fn point_many(snap: &Snapshot, subject: NodeId, predicate: FieldId) -> BTreeSet<ObjKey> {
    snap.many
        .get(&(subject, predicate))
        .cloned()
        .unwrap_or_default()
}

/// 1-hop expand: neighbor node ids reachable from `subject` via `predicate` (both the One current
/// value and the Many present set, restricted to node-valued objects).
pub fn expand(snap: &Snapshot, subject: NodeId, predicate: FieldId) -> BTreeSet<NodeId> {
    let mut out = BTreeSet::new();
    if let Some(ObjKey::Node(n)) = point_one(snap, subject, predicate) {
        out.insert(n);
    }
    for o in snap.many.get(&(subject, predicate)).into_iter().flatten() {
        if let ObjKey::Node(n) = o {
            out.insert(*n);
        }
    }
    out
}

/// All node-valued neighbours of `subject` across *every* predicate (One current value + Many
/// present set) — the predicate-agnostic 1-hop expansion used to grow a distance-bounded view of a
/// heterogeneous (ontology) graph. O(predicates on the subject) via a range scan over the fold.
pub fn neighbors(snap: &Snapshot, subject: NodeId) -> BTreeSet<NodeId> {
    let mut out = BTreeSet::new();
    for (_, v) in snap.one.range((subject, u32::MIN)..=(subject, u32::MAX)) {
        if let Some(ObjKey::Node(n)) = v {
            out.insert(*n);
        }
    }
    for (_, set) in snap.many.range((subject, u32::MIN)..=(subject, u32::MAX)) {
        for o in set {
            if let ObjKey::Node(n) = o {
                out.insert(*n);
            }
        }
    }
    out
}

/// 1-hop expand from a set of subjects (multi-source frontier).
pub fn expand_set(
    snap: &Snapshot,
    subjects: &BTreeSet<NodeId>,
    predicate: FieldId,
) -> BTreeSet<NodeId> {
    let mut out = BTreeSet::new();
    for &s in subjects {
        out.extend(expand(snap, s, predicate));
    }
    out
}

/// 2-hop expand: `subject -p1-> X -p2-> Y`, returning the `Y` frontier.
pub fn two_hop(snap: &Snapshot, subject: NodeId, p1: FieldId, p2: FieldId) -> BTreeSet<NodeId> {
    let hop1 = expand(snap, subject, p1);
    expand_set(snap, &hop1, p2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold::{Op, OrderKey, fold};

    fn ok(seq: u64) -> OrderKey {
        OrderKey {
            tx: seq,
            source: 0,
            seq,
        }
    }

    // Person(1) member-of(pred 0, One) Project(10); has-skill(pred 100, Many) Skill(20,21).
    // Project(10) needs-skill(pred 101, Many) Skill(20,22).
    fn snap() -> Snapshot {
        let ops = vec![
            Op::SetOne {
                subject: 1,
                predicate: 0,
                object: ObjKey::Node(10),
                valid_from: 0,
                ok: ok(0),
            },
            Op::AddMany {
                subject: 1,
                predicate: 100,
                object: ObjKey::Node(20),
                ok: ok(1),
            },
            Op::AddMany {
                subject: 1,
                predicate: 100,
                object: ObjKey::Node(21),
                ok: ok(2),
            },
            Op::AddMany {
                subject: 10,
                predicate: 101,
                object: ObjKey::Node(20),
                ok: ok(3),
            },
            Op::AddMany {
                subject: 10,
                predicate: 101,
                object: ObjKey::Node(22),
                ok: ok(4),
            },
        ];
        fold(&ops).observe()
    }

    #[test]
    fn valid_time_as_of() {
        // One-predicate (1, 5) history incl. a retroactive correction (written last, mid valid_from).
        let ops = vec![
            Op::SetOne {
                subject: 1,
                predicate: 5,
                object: ObjKey::Node(100),
                valid_from: 100,
                ok: ok(0),
            },
            Op::SetOne {
                subject: 1,
                predicate: 5,
                object: ObjKey::Node(200),
                valid_from: 200,
                ok: ok(1),
            },
            Op::SetOne {
                subject: 1,
                predicate: 5,
                object: ObjKey::Node(150),
                valid_from: 150,
                ok: ok(2),
            },
        ];
        let s = fold(&ops).observe();
        assert_eq!(point_one_asof(&s, 1, 5, 50), None); // nothing valid yet
        assert_eq!(point_one_asof(&s, 1, 5, 120), Some(ObjKey::Node(100)));
        assert_eq!(point_one_asof(&s, 1, 5, 160), Some(ObjKey::Node(150))); // retroactive correction wins
        assert_eq!(point_one_asof(&s, 1, 5, 250), Some(ObjKey::Node(200)));
        // "now" (current functional value) = latest-written = the retroactive 150.
        assert_eq!(point_one(&s, 1, 5), Some(ObjKey::Node(150)));
    }

    #[test]
    fn point_one_and_many() {
        let s = snap();
        assert_eq!(point_one(&s, 1, 0), Some(ObjKey::Node(10)));
        assert_eq!(point_one(&s, 1, 999), None);
        assert_eq!(
            point_many(&s, 1, 100),
            [ObjKey::Node(20), ObjKey::Node(21)].into_iter().collect()
        );
    }

    #[test]
    fn one_hop_expand() {
        let s = snap();
        assert_eq!(expand(&s, 1, 100), [20, 21].into_iter().collect());
        assert_eq!(expand(&s, 1, 0), [10].into_iter().collect());
    }

    #[test]
    fn two_hop_expand() {
        // person 1 -member-of-> project 10 -needs-skill-> {20, 22}
        let s = snap();
        assert_eq!(two_hop(&s, 1, 0, 101), [20, 22].into_iter().collect());
    }
}
