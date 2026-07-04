//! Read primitives over a folded [`Snapshot`]: point lookup and 1–2 hop expand.
//!
//! These are the symbolic-core read operators (CAP-2). They operate on the merged snapshot the
//! engine produces (read-merge, see [`crate::engine`]); physical co-location (CSR adjacency) is a
//! later optimization that does not change these contracts.

use std::collections::{BTreeSet, HashMap};

use crate::fact::{FieldId, NodeId};
use crate::fold::{ObjKey, Snapshot};

/// Current functional value of a cardinality-One `(subject, predicate)` (None if absent or closed).
pub fn point_one(snap: &Snapshot, subject: NodeId, predicate: FieldId) -> Option<ObjKey> {
    snap.one.get(&(subject, predicate)).cloned().flatten()
}

/// Valid-time as-of read of a cardinality-One `(subject, predicate)`: the value in effect at
/// valid-time `at`. A version row covers `at` iff `valid_from <= at` and (`valid_to` is open or
/// `at < valid_to`) — a closed interval `[valid_from, valid_to)`. Among the covering rows the one
/// with the greatest `valid_from` wins (ties broken by the later write = a retroactive correction).
/// Returns `None` if nothing covered `at`, or the effective version closed the value. This is
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
        .filter(|(_ok, _obj, valid_from, valid_to)| {
            *valid_from <= at && valid_to.is_none_or(|to| at < to)
        })
        .max_by(|a, b| a.2.cmp(&b.2).then(a.0.cmp(&b.0)))
        .and_then(|(_ok, obj, _vf, _vt)| obj.clone())
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

/// All stored assertions on `subject`, across One (current functional value) and Many (present
/// set), keyed by predicate — the raw material for a node-detail / describe view. O(predicates on
/// the subject) via range scans over the fold.
#[allow(clippy::type_complexity)]
pub fn describe(
    snap: &Snapshot,
    subject: NodeId,
) -> (Vec<(FieldId, ObjKey)>, Vec<(FieldId, Vec<ObjKey>)>) {
    let mut ones = Vec::new();
    for (&(_, p), v) in snap.one.range((subject, u32::MIN)..=(subject, u32::MAX)) {
        if let Some(ok) = v {
            ones.push((p, ok.clone()));
        }
    }
    let mut manys = Vec::new();
    for (&(_, p), set) in snap.many.range((subject, u32::MIN)..=(subject, u32::MAX)) {
        if !set.is_empty() {
            manys.push((p, set.iter().cloned().collect()));
        }
    }
    (ones, manys)
}

/// Build the **undirected** node adjacency of the whole snapshot: every node-valued edge
/// `subject -p-> object` contributes a link in *both* directions, so a BFS over it reaches what a
/// node points to *and* what points at it (an interactive explorer wants connectivity, not edge
/// direction). Optionally restricted to a single predicate `p`. O(node-valued edges) — a full scan;
/// a maintained reverse index is the later optimization. Isolated nodes (no edges) are absent.
pub fn undirected_adjacency(
    snap: &Snapshot,
    predicate: Option<FieldId>,
) -> HashMap<NodeId, BTreeSet<NodeId>> {
    let mut adj: HashMap<NodeId, BTreeSet<NodeId>> = HashMap::new();
    let link = |a: NodeId, b: NodeId, adj: &mut HashMap<NodeId, BTreeSet<NodeId>>| {
        adj.entry(a).or_default().insert(b);
        adj.entry(b).or_default().insert(a);
    };
    for (&(s, p), v) in snap.one.iter() {
        if predicate.is_some_and(|pp| pp != p) {
            continue;
        }
        if let Some(ObjKey::Node(n)) = v {
            link(s, *n, &mut adj);
        }
    }
    for (&(s, p), set) in snap.many.iter() {
        if predicate.is_some_and(|pp| pp != p) {
            continue;
        }
        for o in set {
            if let ObjKey::Node(n) = o {
                link(s, *n, &mut adj);
            }
        }
    }
    adj
}

/// Structural strength of every node-valued edge: for each undirected node pair, the number of
/// *distinct predicates* connecting them (both directions) — a lightweight "relationship strength"
/// derived from the graph, not stored. E.g. a pair linked by both `knows` and `reports-to` scores 2;
/// a single `knows` scores 1. Optionally restricted to one predicate (then every weight is 1).
/// O(node-valued edges) — a full scan.
pub fn edge_strengths(
    snap: &Snapshot,
    predicate: Option<FieldId>,
) -> HashMap<(NodeId, NodeId), u32> {
    let mut preds: HashMap<(NodeId, NodeId), BTreeSet<FieldId>> = HashMap::new();
    let note = |a: NodeId,
                b: NodeId,
                p: FieldId,
                preds: &mut HashMap<(NodeId, NodeId), BTreeSet<FieldId>>| {
        let key = if a < b { (a, b) } else { (b, a) };
        preds.entry(key).or_default().insert(p);
    };
    for (&(s, p), v) in snap.one.iter() {
        if predicate.is_some_and(|pp| pp != p) {
            continue;
        }
        if let Some(ObjKey::Node(n)) = v {
            note(s, *n, p, &mut preds);
        }
    }
    for (&(s, p), set) in snap.many.iter() {
        if predicate.is_some_and(|pp| pp != p) {
            continue;
        }
        for o in set {
            if let ObjKey::Node(n) = o {
                note(s, *n, p, &mut preds);
            }
        }
    }
    preds
        .into_iter()
        .map(|(k, s)| (k, s.len() as u32))
        .collect()
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
                valid_to: None,
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
                valid_to: None,
                ok: ok(0),
            },
            Op::SetOne {
                subject: 1,
                predicate: 5,
                object: ObjKey::Node(200),
                valid_from: 200,
                valid_to: None,
                ok: ok(1),
            },
            Op::SetOne {
                subject: 1,
                predicate: 5,
                object: ObjKey::Node(150),
                valid_from: 150,
                valid_to: None,
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
    fn valid_time_bounded_interval() {
        // A single closed interval [100, 200): valid at 150, not before 100, not at/after 200.
        let ops = vec![Op::SetOne {
            subject: 7,
            predicate: 5,
            object: ObjKey::Node(42),
            valid_from: 100,
            valid_to: Some(200),
            ok: ok(0),
        }];
        let s = fold(&ops).observe();
        assert_eq!(point_one_asof(&s, 7, 5, 50), None); // before the interval
        assert_eq!(point_one_asof(&s, 7, 5, 100), Some(ObjKey::Node(42))); // lower bound inclusive
        assert_eq!(point_one_asof(&s, 7, 5, 150), Some(ObjKey::Node(42))); // inside
        assert_eq!(point_one_asof(&s, 7, 5, 200), None); // upper bound exclusive (ended)
        assert_eq!(point_one_asof(&s, 7, 5, 250), None); // after the interval
        // current functional value ignores wall-clock: the asserted value is still present.
        assert_eq!(point_one(&s, 7, 5), Some(ObjKey::Node(42)));
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
