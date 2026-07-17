//! Read primitives over a folded [`Snapshot`]: point lookup and 1–2 hop expand.
//!
//! These are the symbolic-core read operators (CAP-2). They operate on the merged snapshot the
//! engine produces (read-merge, see [`crate::engine`]); physical co-location (CSR adjacency) is a
//! later optimization that does not change these contracts.

use std::collections::{BTreeSet, HashMap};

use crate::catalog::Catalog;
use crate::fact::{FieldId, NodeId};
use crate::fold::{ObjKey, Snapshot, VersionRow};

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

/// The winning version row of a cardinality-One `(subject, predicate)` — the greatest-`OrderKey`
/// live row, which is the last entry of the ascending `one_history`. `None` when the key has no
/// history. This is the head every current-value read resolves; the ingest boundary also compares
/// an incoming re-assertion against it to suppress no-op writes.
pub fn point_one_head(snap: &Snapshot, subject: NodeId, predicate: FieldId) -> Option<&VersionRow> {
    snap.one_history.get(&(subject, predicate))?.last()
}

/// The interned `source` of the current functional value's winning version — the provenance of the
/// value [`point_one`] returns (the [`point_one_head`] row's `OrderKey.source`). `None` when the
/// key has no history; a `source` of `0` is the "unset"/unknown sentinel (callers decide how to
/// surface it — typically by omitting provenance).
pub fn point_one_source(snap: &Snapshot, subject: NodeId, predicate: FieldId) -> Option<FieldId> {
    point_one_head(snap, subject, predicate).map(|(ok, _obj, _vf, _vt)| ok.source)
}

/// The `valid_from` of the current functional value's winning version — the [`point_one_head`]
/// row. `None` when the key has no history.
pub fn point_one_valid_from(snap: &Snapshot, subject: NodeId, predicate: FieldId) -> Option<i64> {
    point_one_head(snap, subject, predicate).map(|(_ok, _obj, vf, _vt)| *vf)
}

/// The `valid_from` of the winning version *when that version is a close* — the
/// [`point_one_head`] row, but `Some` only when that row carries no object. `None` for a live
/// winner and for a key with no history at all, so a caller can tell "ended by a close at `vf`"
/// apart from "never written".
pub fn point_one_closed_from(snap: &Snapshot, subject: NodeId, predicate: FieldId) -> Option<i64> {
    point_one_head(snap, subject, predicate)
        .and_then(|(_ok, obj, vf, _vt)| obj.is_none().then_some(*vf))
}

/// A coarse, deterministic confidence tier for a `point` answer — see [`confidence_signals`].
/// Deliberately three buckets, not a continuous score: the engine reports only what it can observe
/// from provenance and valid-time, and leaves calibration to a policy layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    Low,
    Medium,
    High,
}

impl Tier {
    /// Stable lowercase wire name (`"low"` | `"medium"` | `"high"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Low => "low",
            Tier::Medium => "medium",
            Tier::High => "high",
        }
    }
}

/// The raw confidence signals the engine can observe for a cardinality-One `(subject, predicate)`
/// answer, plus the coarse [`Tier`] derived from them. The raw counts travel with the tier so a
/// caller/policy layer can apply its own thresholds — the engine's `tier` is a coarse default, not
/// the last word.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConfidenceSignals {
    /// Number of DISTINCT sources (excluding the `0` unset sentinel) that asserted the current
    /// winning value. `>= 2` means multiple independent sources agree on the same value.
    pub corroboration: usize,
    /// Whether the winning value carries a source (its `OrderKey.source != 0`).
    pub has_source: bool,
    /// `now - valid_from` of the winning value when a reference `now` was supplied, else `None`
    /// (freshness is unknown without a reference time).
    pub age: Option<i64>,
    /// The coarse tier derived from the signals above (see [`confidence_signals`]).
    pub tier: Tier,
}

/// Derive coarse, deterministic [`ConfidenceSignals`] for a cardinality-One `(subject, predicate)`
/// answer from the signals the engine actually has — no trust ranking of sources, no continuous
/// score, no calibration. Everything is read from `one_history`; the winning (current functional)
/// version is the greatest-`OrderKey` live row = its last entry.
///
/// - **corroboration** = distinct non-zero `OrderKey.source` among the live rows whose object equals
///   the current winning value.
/// - **has_source** = the winning row's `source != 0`.
/// - **age** = `now - winning valid_from` when `now` is given, else `None`; **stale** = `max_age`
///   given and `age > max_age` (both required, so without `now` nothing is ever stale).
///
/// Tier: **low** when the value is source-less OR stale; **high** when corroboration `>= 2` and not
/// stale; **medium** otherwise (a single sourced value, fresh or freshness-unknown). `low` is checked
/// first, so a source-less or stale value is never `high`.
pub fn confidence_signals(
    snap: &Snapshot,
    subject: NodeId,
    predicate: FieldId,
    now: Option<i64>,
    max_age: Option<i64>,
) -> ConfidenceSignals {
    let history = snap.one_history.get(&(subject, predicate));
    // The winning (current functional) value is the greatest-`OrderKey` live row = the last entry.
    let winner = history.and_then(|h| h.last());
    let has_source = winner.is_some_and(|(ok, ..)| ok.source != 0);
    let corroboration = match winner {
        Some((_, obj, ..)) => history
            .into_iter()
            .flatten()
            .filter(|(_, o, ..)| o == obj)
            .map(|(ok, ..)| ok.source)
            .filter(|&src| src != 0)
            .collect::<BTreeSet<_>>()
            .len(),
        None => 0,
    };
    let age = winner.and_then(|(_, _, valid_from, _)| now.map(|n| n - *valid_from));
    let stale = matches!((age, max_age), (Some(a), Some(m)) if a > m);
    let tier = if !has_source || stale {
        Tier::Low
    } else if corroboration >= 2 {
        Tier::High
    } else {
        Tier::Medium
    };
    ConfidenceSignals {
        corroboration,
        has_source,
        age,
        tier,
    }
}

/// All properties on the edge `(subject, predicate, object)` (empty if none), LWW-resolved.
pub fn edge_props<'a>(
    snap: &'a Snapshot,
    subject: NodeId,
    predicate: FieldId,
    object: &ObjKey,
) -> Option<&'a std::collections::BTreeMap<String, ObjKey>> {
    snap.edge_props.get(&(subject, predicate))?.get(object)
}

/// A single property value on the edge `(subject, predicate, object)`.
pub fn edge_prop(
    snap: &Snapshot,
    subject: NodeId,
    predicate: FieldId,
    object: &ObjKey,
    key: &str,
) -> Option<ObjKey> {
    edge_props(snap, subject, predicate, object)?
        .get(key)
        .cloned()
}

/// Present element set of a cardinality-Many `(subject, predicate)` (empty if absent).
pub fn point_many(snap: &Snapshot, subject: NodeId, predicate: FieldId) -> BTreeSet<ObjKey> {
    snap.many
        .get(&(subject, predicate))
        .cloned()
        .unwrap_or_default()
}

/// Whether one element's version rows put it in effect at valid-time `at` — per element this is
/// exactly the [`point_one_asof`] rule: among the rows whose `[valid_from, valid_to)` interval
/// covers `at`, the greatest `valid_from` wins (ties → the later write), and the element is in
/// effect iff that winner is an add (close rows carry no object).
fn elem_in_effect_at(rows: &[VersionRow], at: i64) -> bool {
    rows.iter()
        .filter(|(_ok, _obj, valid_from, valid_to)| {
            *valid_from <= at && valid_to.is_none_or(|to| at < to)
        })
        .max_by(|a, b| a.2.cmp(&b.2).then(a.0.cmp(&b.0)))
        .is_some_and(|(_ok, obj, _vf, _vt)| obj.is_some())
}

/// Valid-time as-of read of a cardinality-Many `(subject, predicate)`: the elements in effect at
/// valid-time `at`. The Many analogue of [`point_one_asof`], applied per element over
/// `many_history` — an element added at `t0` and closed at `t1` is in the answer for
/// `t0 <= at < t1`, out after, and back in after a re-add. Elements erased by a retract
/// (observed-remove) have no live rows and never answer — a retract destroys history by design;
/// closing is the temporal path.
pub fn point_many_asof(
    snap: &Snapshot,
    subject: NodeId,
    predicate: FieldId,
    at: i64,
) -> BTreeSet<ObjKey> {
    snap.many_history
        .get(&(subject, predicate))
        .into_iter()
        .flatten()
        .filter(|(_, rows)| elem_in_effect_at(rows, at))
        .map(|(obj, _)| obj.clone())
        .collect()
}

/// The close boundary of one cardinality-Many element *when its winning row is a close* — `Some`
/// only when the element's greatest live row carries no object (the Many analogue of
/// [`point_one_closed_from`], per element). `None` for a present element and for one never
/// written, so an ingest guard can suppress a duplicate close without conflating the two.
pub fn many_closed_from(
    snap: &Snapshot,
    subject: NodeId,
    predicate: FieldId,
    object: &ObjKey,
) -> Option<i64> {
    snap.many_history
        .get(&(subject, predicate))?
        .get(object)?
        .last()
        .and_then(|(_ok, obj, vf, _vt)| obj.is_none().then_some(*vf))
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

/// Reverse adjacency for a single predicate: for each node-valued edge `s --predicate--> o`, maps `o`
/// to the set of subjects `s` that point at it (`{s : s --predicate--> o}`). A single scan restricted
/// to `predicate` — the reverse-direction lookup the symmetric / inverse expansions need without
/// storing both directions.
fn reverse_adjacency(snap: &Snapshot, predicate: FieldId) -> HashMap<NodeId, BTreeSet<NodeId>> {
    let mut rev: HashMap<NodeId, BTreeSet<NodeId>> = HashMap::new();
    for (&(s, p), v) in snap.one.iter() {
        if p == predicate
            && let Some(ObjKey::Node(o)) = v
        {
            rev.entry(*o).or_default().insert(s);
        }
    }
    for (&(s, p), set) in snap.many.iter() {
        if p != predicate {
            continue;
        }
        for o in set {
            if let ObjKey::Node(o) = o {
                rev.entry(*o).or_default().insert(s);
            }
        }
    }
    rev
}

/// Property-aware expand: 1-hop or a bounded transitive closure honoring the predicate's declared
/// [`RelProps`](crate::catalog::RelProps) (`symmetric` / `inverse` / `transitive`). A predicate with
/// no declared properties behaves exactly like [`expand`] (direct 1-hop). Deterministic (BFS over
/// sorted sets) and always bounded by `max_depth` — a visited-set terminates cycles — so it never
/// recurses unboundedly.
///
/// - **symmetric**: each hop is undirected — both `subject --P--> b` and `b --P--> subject`.
/// - **inverse = Q**: each hop also yields `{b : b --Q--> subject}`, the reverse of the named
///   predicate's stored edges (so an inverse predicate is answerable without storing both directions).
/// - **transitive**: every node reachable in `1..=max_depth` hops (each hop honoring the above).
pub fn expand_rel(
    snap: &Snapshot,
    catalog: &Catalog,
    subject: NodeId,
    predicate: FieldId,
    max_depth: usize,
) -> BTreeSet<NodeId> {
    expand_rel_at(snap, catalog, subject, predicate, max_depth, None)
}

/// [`expand_rel`] at valid-time `at`: every hop — forward, symmetric-reverse, inverse-reverse —
/// answers from the state in effect at that instant (One values via [`point_one_asof`], Many
/// elements via [`point_many_asof`]), so a transitive closure over history never mixes eras.
pub fn expand_rel_asof(
    snap: &Snapshot,
    catalog: &Catalog,
    subject: NodeId,
    predicate: FieldId,
    max_depth: usize,
    at: i64,
) -> BTreeSet<NodeId> {
    expand_rel_at(snap, catalog, subject, predicate, max_depth, Some(at))
}

/// 1-hop expand at valid-time `at`: node-valued neighbours in effect at that instant.
pub fn expand_asof(
    snap: &Snapshot,
    subject: NodeId,
    predicate: FieldId,
    at: i64,
) -> BTreeSet<NodeId> {
    let mut out = BTreeSet::new();
    if let Some(ObjKey::Node(n)) = point_one_asof(snap, subject, predicate, at) {
        out.insert(n);
    }
    for o in point_many_asof(snap, subject, predicate, at) {
        if let ObjKey::Node(n) = o {
            out.insert(n);
        }
    }
    out
}

/// [`reverse_adjacency`] at valid-time `at` — the same single restricted scan, but each key's
/// contribution is its as-of winner(s) instead of its current state (histories, not present sets).
fn reverse_adjacency_asof(
    snap: &Snapshot,
    predicate: FieldId,
    at: i64,
) -> HashMap<NodeId, BTreeSet<NodeId>> {
    let mut rev: HashMap<NodeId, BTreeSet<NodeId>> = HashMap::new();
    for &(s, p) in snap.one_history.keys() {
        if p == predicate
            && let Some(ObjKey::Node(o)) = point_one_asof(snap, s, p, at)
        {
            rev.entry(o).or_default().insert(s);
        }
    }
    for &(s, p) in snap.many_history.keys() {
        if p != predicate {
            continue;
        }
        for o in point_many_asof(snap, s, p, at) {
            if let ObjKey::Node(o) = o {
                rev.entry(o).or_default().insert(s);
            }
        }
    }
    rev
}

fn expand_rel_at(
    snap: &Snapshot,
    catalog: &Catalog,
    subject: NodeId,
    predicate: FieldId,
    max_depth: usize,
    at: Option<i64>,
) -> BTreeSet<NodeId> {
    // the 1-hop direct expand, current or as-of
    let hop = |node: NodeId, p: FieldId| -> BTreeSet<NodeId> {
        match at {
            None => expand(snap, node, p),
            Some(t) => expand_asof(snap, node, p, t),
        }
    };
    let rev_of = |p: FieldId| -> HashMap<NodeId, BTreeSet<NodeId>> {
        match at {
            None => reverse_adjacency(snap, p),
            Some(t) => reverse_adjacency_asof(snap, p, t),
        }
    };
    let props = catalog
        .predicate(predicate)
        .map(|d| d.props)
        .unwrap_or_default();
    // No declared properties → identical to the plain 1-hop direct expand.
    if !props.symmetric && !props.transitive && props.inverse.is_none() {
        return hop(subject, predicate);
    }
    // Reverse adjacency is only needed for the undirected (symmetric) and inverse cases; build each
    // once (a single restricted scan) and reuse it across every hop.
    let rev_p = props.symmetric.then(|| rev_of(predicate));
    let rev_inv = props.inverse.map(&rev_of);
    // One property-aware hop from `node`: forward P edges, plus (symmetric) reverse P edges, plus
    // (inverse) the reverse of the named predicate's edges.
    let step = |node: NodeId| -> BTreeSet<NodeId> {
        let mut out = hop(node, predicate);
        if let Some(rp) = &rev_p
            && let Some(s) = rp.get(&node)
        {
            out.extend(s.iter().copied());
        }
        if let Some(ri) = &rev_inv
            && let Some(s) = ri.get(&node)
        {
            out.extend(s.iter().copied());
        }
        out
    };

    if !props.transitive {
        return step(subject);
    }

    // Bounded, deterministic BFS transitive closure. `result` doubles as the visited-set so cycles
    // terminate, and `max_depth` caps the number of hops so it is never unbounded.
    let mut result = BTreeSet::new();
    if max_depth == 0 {
        return result;
    }
    let mut current = step(subject);
    let mut hops = 1usize;
    loop {
        let mut newly = Vec::new();
        for n in &current {
            if result.insert(*n) {
                newly.push(*n);
            }
        }
        if hops >= max_depth || newly.is_empty() {
            break;
        }
        let mut next = BTreeSet::new();
        for n in &newly {
            next.extend(step(*n));
        }
        current = next.into_iter().filter(|v| !result.contains(v)).collect();
        hops += 1;
    }
    result
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
                valid_from: 0,
                valid_to: None,
                ok: ok(1),
            },
            Op::AddMany {
                subject: 1,
                predicate: 100,
                object: ObjKey::Node(21),
                valid_from: 0,
                valid_to: None,
                ok: ok(2),
            },
            Op::AddMany {
                subject: 10,
                predicate: 101,
                object: ObjKey::Node(20),
                valid_from: 0,
                valid_to: None,
                ok: ok(3),
            },
            Op::AddMany {
                subject: 10,
                predicate: 101,
                object: ObjKey::Node(22),
                valid_from: 0,
                valid_to: None,
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

    fn add_many_at(s: u64, p: u32, o: u64, vf: i64, vt: Option<i64>, seq: u64) -> Op {
        Op::AddMany {
            subject: s,
            predicate: p,
            object: ObjKey::Node(o),
            valid_from: vf,
            valid_to: vt,
            ok: ok(seq),
        }
    }

    fn close_many_at(s: u64, p: u32, o: u64, vf: i64, seq: u64) -> Op {
        Op::CloseMany {
            subject: s,
            predicate: p,
            object: ObjKey::Node(o),
            valid_from: vf,
            ok: ok(seq),
        }
    }

    #[test]
    fn many_valid_time_as_of() {
        // grant(20) at 100 closed at 200 then re-granted at 300; grant(21) bounded [150, 250)
        let s = fold(&[
            add_many_at(1, 100, 20, 100, None, 0),
            close_many_at(1, 100, 20, 200, 1),
            add_many_at(1, 100, 20, 300, None, 2),
            add_many_at(1, 100, 21, 150, Some(250), 3),
        ])
        .observe();
        let set = |at: i64| point_many_asof(&s, 1, 100, at);
        assert_eq!(set(50), BTreeSet::new()); // nothing in effect yet
        assert_eq!(set(120), [ObjKey::Node(20)].into_iter().collect());
        assert_eq!(
            set(180),
            [ObjKey::Node(20), ObjKey::Node(21)].into_iter().collect()
        );
        assert_eq!(set(220), [ObjKey::Node(21)].into_iter().collect()); // 20 closed at 200
        assert_eq!(set(260), BTreeSet::new()); // 21's bound passed, 20 not yet re-granted
        assert_eq!(set(350), [ObjKey::Node(20)].into_iter().collect()); // re-grant back in effect
        // the CURRENT set follows the winning rows: 20 present (re-add wins), 21 present
        // (valid_to is read-time only, exactly like the One-cardinality current read)
        assert_eq!(
            point_many(&s, 1, 100),
            [ObjKey::Node(20), ObjKey::Node(21)].into_iter().collect()
        );
    }

    #[test]
    fn many_closed_from_distinguishes_ended_from_never_written() {
        let s = fold(&[
            add_many_at(1, 100, 20, 100, None, 0),
            close_many_at(1, 100, 20, 200, 1),
        ])
        .observe();
        assert_eq!(many_closed_from(&s, 1, 100, &ObjKey::Node(20)), Some(200));
        assert_eq!(many_closed_from(&s, 1, 100, &ObjKey::Node(99)), None); // never written
        let s2 = fold(&[add_many_at(1, 100, 20, 100, None, 0)]).observe();
        assert_eq!(many_closed_from(&s2, 1, 100, &ObjKey::Node(20)), None); // live winner
    }

    #[test]
    fn expand_asof_slices_both_cardinalities() {
        // One edge (1 -0-> 10) superseded at 200 by (1 -0-> 11); Many edge (1 -100-> 20) closed at 150
        let s = fold(&[
            Op::SetOne {
                subject: 1,
                predicate: 0,
                object: ObjKey::Node(10),
                valid_from: 100,
                valid_to: None,
                ok: ok(0),
            },
            Op::SetOne {
                subject: 1,
                predicate: 0,
                object: ObjKey::Node(11),
                valid_from: 200,
                valid_to: None,
                ok: ok(1),
            },
            add_many_at(1, 100, 20, 100, None, 2),
            close_many_at(1, 100, 20, 150, 3),
        ])
        .observe();
        assert_eq!(expand_asof(&s, 1, 0, 120), [10].into_iter().collect());
        assert_eq!(expand_asof(&s, 1, 0, 250), [11].into_iter().collect());
        assert_eq!(expand_asof(&s, 1, 100, 120), [20].into_iter().collect());
        assert_eq!(expand_asof(&s, 1, 100, 180), BTreeSet::new());
    }

    #[test]
    fn expand_rel_asof_honours_symmetric_reverse_edges_in_their_era() {
        // symmetric predicate: 2 -sym-> 1 granted at 100, closed at 200 — the REVERSE hop from 1
        // must appear inside the era and vanish after it
        let mut c = Catalog::new();
        let t = c.register_type("Node");
        let sym = c.register_predicate(
            "sym",
            crate::catalog::Cardinality::Many,
            crate::catalog::RelProps {
                symmetric: true,
                ..Default::default()
            },
            t,
            crate::catalog::Range::Type(t),
        );
        let s = fold(&[
            add_many_at(2, sym, 1, 100, None, 0),
            close_many_at(2, sym, 1, 200, 1),
        ])
        .observe();
        assert_eq!(
            expand_rel_asof(&s, &c, 1, sym, 16, 150),
            [2].into_iter().collect()
        );
        assert_eq!(expand_rel_asof(&s, &c, 1, sym, 16, 250), BTreeSet::new());
    }

    #[test]
    fn edge_props_lww_and_read() {
        // Edge (1, has-skill=100, Skill 20): set level=3 then level=5 (later ok wins), plus role.
        let ops = vec![
            Op::AddMany {
                subject: 1,
                predicate: 100,
                object: ObjKey::Node(20),
                valid_from: 0,
                valid_to: None,
                ok: ok(0),
            },
            Op::SetEdgeProp {
                subject: 1,
                predicate: 100,
                object: ObjKey::Node(20),
                key: "level".into(),
                value: ObjKey::Int(3),
                ok: ok(1),
            },
            Op::SetEdgeProp {
                subject: 1,
                predicate: 100,
                object: ObjKey::Node(20),
                key: "level".into(),
                value: ObjKey::Int(5),
                ok: ok(2), // greater order key → wins
            },
            Op::SetEdgeProp {
                subject: 1,
                predicate: 100,
                object: ObjKey::Node(20),
                key: "role".into(),
                value: ObjKey::Text("lead".into()),
                ok: ok(3),
            },
        ];
        let s = fold(&ops).observe();
        assert_eq!(
            edge_prop(&s, 1, 100, &ObjKey::Node(20), "level"),
            Some(ObjKey::Int(5))
        );
        assert_eq!(
            edge_prop(&s, 1, 100, &ObjKey::Node(20), "role"),
            Some(ObjKey::Text("lead".into()))
        );
        assert_eq!(edge_prop(&s, 1, 100, &ObjKey::Node(20), "missing"), None);
        // a different edge object has no properties
        assert_eq!(edge_props(&s, 1, 100, &ObjKey::Node(21)), None);
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
    fn point_one_source_is_the_winner() {
        // Two competing SetOne on (1, 5) from different sources, different valid_from. The current
        // functional value is the greatest-OrderKey write (source 9), so its source is the provenance.
        let ops = vec![
            Op::SetOne {
                subject: 1,
                predicate: 5,
                object: ObjKey::Node(100),
                valid_from: 10,
                valid_to: None,
                ok: OrderKey {
                    tx: 1,
                    source: 7,
                    seq: 0,
                },
            },
            Op::SetOne {
                subject: 1,
                predicate: 5,
                object: ObjKey::Node(200),
                valid_from: 20,
                valid_to: None,
                ok: OrderKey {
                    tx: 2,
                    source: 9,
                    seq: 1,
                },
            },
        ];
        let s = fold(&ops).observe();
        assert_eq!(point_one(&s, 1, 5), Some(ObjKey::Node(200)));
        assert_eq!(point_one_source(&s, 1, 5), Some(9));
    }

    #[test]
    fn point_one_source_unset_and_absent() {
        // A write with no source carries the 0 sentinel; an absent key has no source at all.
        let ops = vec![Op::SetOne {
            subject: 1,
            predicate: 5,
            object: ObjKey::Node(1),
            valid_from: 0,
            valid_to: None,
            ok: OrderKey {
                tx: 1,
                source: 0,
                seq: 0,
            },
        }];
        let s = fold(&ops).observe();
        assert_eq!(point_one_source(&s, 1, 5), Some(0)); // 0 = unset sentinel
        assert_eq!(point_one_source(&s, 1, 999), None); // absent key
    }

    #[test]
    fn point_one_closed_from_reports_a_close_winner_only() {
        // (1, 5): fact then close — the close wins, so its valid_from is reported.
        let ops = vec![
            Op::SetOne {
                subject: 1,
                predicate: 5,
                object: ObjKey::Node(100),
                valid_from: 100,
                valid_to: None,
                ok: ok(0),
            },
            Op::CloseOne {
                subject: 1,
                predicate: 5,
                valid_from: 200,
                ok: ok(1),
            },
        ];
        let s = fold(&ops).observe();
        assert_eq!(point_one(&s, 1, 5), None); // closed → no current value
        assert_eq!(point_one_closed_from(&s, 1, 5), Some(200));
        assert_eq!(point_one_closed_from(&s, 1, 999), None); // never written

        // A live winner (fact written after the close) is not a close.
        let mut ops = ops;
        ops.push(Op::SetOne {
            subject: 1,
            predicate: 5,
            object: ObjKey::Node(300),
            valid_from: 300,
            valid_to: None,
            ok: ok(2),
        });
        let s = fold(&ops).observe();
        assert_eq!(point_one(&s, 1, 5), Some(ObjKey::Node(300)));
        assert_eq!(point_one_closed_from(&s, 1, 5), None);
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

    // ---- property-aware expand (expand_rel) ----

    use crate::catalog::{Cardinality, Catalog, Range, RelProps};

    /// A Many-cardinality graph from `(subject, predicate, object)` edges (all node-valued).
    fn many_edges(edges: &[(NodeId, FieldId, NodeId)]) -> Snapshot {
        let ops: Vec<Op> = edges
            .iter()
            .enumerate()
            .map(|(i, &(subject, predicate, object))| Op::AddMany {
                subject,
                predicate,
                object: ObjKey::Node(object),
                valid_from: 0,
                valid_to: None,
                ok: ok(i as u64),
            })
            .collect();
        fold(&ops).observe()
    }

    #[test]
    fn expand_rel_default_props_equals_plain_expand() {
        let mut c = Catalog::new();
        let t = c.register_type("T");
        let p = c.register_predicate(
            "p",
            Cardinality::Many,
            RelProps::default(),
            t,
            Range::Type(t),
        );
        let s = many_edges(&[(1, p, 2), (1, p, 3)]);
        assert_eq!(expand_rel(&s, &c, 1, p, 16), expand(&s, 1, p));
        assert_eq!(expand_rel(&s, &c, 1, p, 16), [2, 3].into_iter().collect());
    }

    #[test]
    fn expand_rel_symmetric_is_undirected() {
        let mut c = Catalog::new();
        let t = c.register_type("T");
        let knows = c.register_predicate(
            "knows",
            Cardinality::Many,
            RelProps {
                symmetric: true,
                transitive: false,
                inverse: None,
            },
            t,
            Range::Type(t),
        );
        // 1 knows 2 (forward); 3 knows 1 (reverse — reachable from 1 only because knows is symmetric).
        let s = many_edges(&[(1, knows, 2), (3, knows, 1)]);
        assert_eq!(
            expand_rel(&s, &c, 1, knows, 16),
            [2, 3].into_iter().collect()
        );
        // plain (direction-respecting) expand sees only the forward edge
        assert_eq!(expand(&s, 1, knows), [2].into_iter().collect());
    }

    #[test]
    fn expand_rel_inverse_reads_reverse_of_named_predicate() {
        let mut c = Catalog::new();
        let t = c.register_type("T");
        let parent = c.register_predicate(
            "parent-of",
            Cardinality::Many,
            RelProps::default(),
            t,
            Range::Type(t),
        );
        let child = c.register_predicate(
            "child-of",
            Cardinality::Many,
            RelProps {
                symmetric: false,
                transitive: false,
                inverse: Some(parent),
            },
            t,
            Range::Type(t),
        );
        // 1 parent-of {2, 3}; child-of stores no edges of its own.
        let s = many_edges(&[(1, parent, 2), (1, parent, 3)]);
        // expanding the inverse (child-of) on 2 yields 2's parents = {1}
        assert_eq!(expand_rel(&s, &c, 2, child, 16), [1].into_iter().collect());
        assert_eq!(expand_rel(&s, &c, 3, child, 16), [1].into_iter().collect());
        // 1 has no parent → child-of on 1 is empty
        assert!(expand_rel(&s, &c, 1, child, 16).is_empty());
        // parent-of still expands directly (forward)
        assert_eq!(
            expand_rel(&s, &c, 1, parent, 16),
            [2, 3].into_iter().collect()
        );
    }

    #[test]
    fn expand_rel_transitive_closure_and_depth_bound() {
        let mut c = Catalog::new();
        let t = c.register_type("T");
        let reaches = c.register_predicate(
            "reaches",
            Cardinality::Many,
            RelProps {
                symmetric: false,
                transitive: true,
                inverse: None,
            },
            t,
            Range::Type(t),
        );
        // chain 1 → 2 → 3 → 4
        let s = many_edges(&[(1, reaches, 2), (2, reaches, 3), (3, reaches, 4)]);
        assert_eq!(
            expand_rel(&s, &c, 1, reaches, 16),
            [2, 3, 4].into_iter().collect()
        );
        // depth bound respected
        assert_eq!(
            expand_rel(&s, &c, 1, reaches, 2),
            [2, 3].into_iter().collect()
        );
        assert_eq!(expand_rel(&s, &c, 1, reaches, 1), [2].into_iter().collect());
    }

    #[test]
    fn expand_rel_transitive_cycle_terminates() {
        let mut c = Catalog::new();
        let t = c.register_type("T");
        let reaches = c.register_predicate(
            "reaches",
            Cardinality::Many,
            RelProps {
                symmetric: false,
                transitive: true,
                inverse: None,
            },
            t,
            Range::Type(t),
        );
        // cycle 1 → 2 → 3 → 1: the visited-set must terminate the BFS. 1 is re-reached at 3 hops, so
        // the whole cycle is in the 1..=16-hop reachable set.
        let s = many_edges(&[(1, reaches, 2), (2, reaches, 3), (3, reaches, 1)]);
        assert_eq!(
            expand_rel(&s, &c, 1, reaches, 16),
            [1, 2, 3].into_iter().collect()
        );
    }

    // ---- coarse confidence signals (confidence_signals) ----

    fn ok_src(seq: u64, source: FieldId) -> OrderKey {
        OrderKey {
            tx: seq,
            source,
            seq,
        }
    }

    /// A single One value at `(1, 5)` = Node(100), asserted `valid_from` at `vf` by each of `sources`
    /// in order (later writes win). A `source` of `0` is the unset sentinel.
    fn one_value(sources: &[(FieldId, i64)]) -> Snapshot {
        let ops: Vec<Op> = sources
            .iter()
            .enumerate()
            .map(|(i, &(source, vf))| Op::SetOne {
                subject: 1,
                predicate: 5,
                object: ObjKey::Node(100),
                valid_from: vf,
                valid_to: None,
                ok: ok_src(i as u64, source),
            })
            .collect();
        fold(&ops).observe()
    }

    #[test]
    fn confidence_two_distinct_sources_is_high() {
        // Same value from two distinct sources (7, 9) → corroborated, sourced, fresh → high.
        let s = one_value(&[(7, 10), (9, 20)]);
        let c = confidence_signals(&s, 1, 5, None, None);
        assert_eq!(c.corroboration, 2);
        assert!(c.has_source);
        assert_eq!(c.age, None);
        assert_eq!(c.tier, Tier::High);
        assert_eq!(c.tier.as_str(), "high");
    }

    #[test]
    fn confidence_single_source_is_medium() {
        let s = one_value(&[(7, 10)]);
        let c = confidence_signals(&s, 1, 5, None, None);
        assert_eq!(c.corroboration, 1);
        assert!(c.has_source);
        assert_eq!(c.tier, Tier::Medium);
        assert_eq!(c.tier.as_str(), "medium");
    }

    #[test]
    fn confidence_sourceless_is_low() {
        // The winning value carries the 0 unset sentinel → no source → low; 0 is not corroboration.
        let s = one_value(&[(0, 10)]);
        let c = confidence_signals(&s, 1, 5, None, None);
        assert_eq!(c.corroboration, 0);
        assert!(!c.has_source);
        assert_eq!(c.tier, Tier::Low);
        assert_eq!(c.tier.as_str(), "low");
    }

    #[test]
    fn confidence_stale_overrides_corroboration_to_low() {
        // Corroborated + sourced, but old relative to now/max_age → stale → low.
        let s = one_value(&[(7, 100), (9, 100)]);
        let c = confidence_signals(&s, 1, 5, Some(1000), Some(100));
        assert_eq!(c.corroboration, 2);
        assert_eq!(c.age, Some(900)); // 1000 - valid_from(100)
        assert_eq!(c.tier, Tier::Low);
        // within max_age → fresh again → back to high (corroborated).
        let c2 = confidence_signals(&s, 1, 5, Some(150), Some(100));
        assert_eq!(c2.age, Some(50));
        assert_eq!(c2.tier, Tier::High);
    }

    #[test]
    fn confidence_now_without_max_age_is_never_stale() {
        // `now` alone yields an age but no staleness (max_age is required to judge stale).
        let s = one_value(&[(7, 0)]);
        let c = confidence_signals(&s, 1, 5, Some(10_000), None);
        assert_eq!(c.age, Some(10_000));
        assert_eq!(c.tier, Tier::Medium);
    }

    #[test]
    fn confidence_absent_value_is_low() {
        let s = one_value(&[(7, 10)]);
        // an unasserted key: no winner → no source, no corroboration, unknown age → low.
        let c = confidence_signals(&s, 1, 999, Some(100), Some(1));
        assert_eq!(c.corroboration, 0);
        assert!(!c.has_source);
        assert_eq!(c.age, None);
        assert_eq!(c.tier, Tier::Low);
    }

    #[test]
    fn expand_rel_symmetric_transitive_reaches_component() {
        let mut c = Catalog::new();
        let t = c.register_type("T");
        let linked = c.register_predicate(
            "linked",
            Cardinality::Many,
            RelProps {
                symmetric: true,
                transitive: true,
                inverse: None,
            },
            t,
            Range::Type(t),
        );
        // stored one-directionally: 1-2, 2-3, 4-3 → undirected + transitive spans component {1,2,3,4}.
        let s = many_edges(&[(1, linked, 2), (2, linked, 3), (4, linked, 3)]);
        assert_eq!(
            expand_rel(&s, &c, 1, linked, 16),
            [1, 2, 3, 4].into_iter().collect()
        );
    }
}
