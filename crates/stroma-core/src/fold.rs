//! The fold: stream diffs → current graph state, per `(subject, predicate)`.
//!
//! Each key's state is a **join-semilattice** (commutative + associative + idempotent merge), so the
//! fold converges under any arrival order / source partition / redelivery — the basis for
//! deterministic replay and audit (algebra validated in Phase 0 `poc-fold-determinism`).
//!
//! Cardinality (from the [`Catalog`]) drives behaviour: `One` → LWW-Register with history (supersede);
//! `Many` → OR-Set whose per-element rows carry valid-time (accumulate; a `CloseMany` row ends an
//! element's interval, presence = the element's greatest live row is an add). Hard-delete is a
//! max-register floor that purges everything `<= floor` (re-assertion above the floor survives).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::catalog::{Cardinality, Catalog};
use crate::fact::{Fact, FieldId, NodeId, Object, Value};
use crate::hash::FxHashMap;
use crate::wal;

/// Orderable, hashable object identity (the value/edge target an op refers to). `Float` keeps the
/// raw bits so it has a total order (no NaN ambiguity in the fold).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjKey {
    Node(u64),
    Int(i64),
    Float(u64),
    Text(String),
    Bool(bool),
}

impl ObjKey {
    pub fn of(o: &Object) -> Self {
        match o {
            Object::Node(n) => ObjKey::Node(*n),
            Object::Value(Value::Int(i)) => ObjKey::Int(*i),
            Object::Value(Value::Float(f)) => ObjKey::Float(f.to_bits()),
            Object::Value(Value::Text(t)) => ObjKey::Text(t.clone()),
            Object::Value(Value::Bool(b)) => ObjKey::Bool(*b),
        }
    }
}

/// Total order over competing writes: `transaction_time`, ties broken by `(source, write_seq)`.
/// The write engine MUST make `(source, seq)` globally unique so distinct competing writes never
/// share an `OrderKey` (otherwise the LWW winner is ambiguous and the fold is non-deterministic).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct OrderKey {
    pub tx: u64,
    pub source: FieldId,
    pub seq: u64,
}

/// A stream diff. `SetOne`/`CloseOne` target cardinality-`One` keys, `AddMany`/`CloseMany`/
/// `RemoveMany` target cardinality-`Many` keys; `HardDelete` carries the key's cardinality so it
/// can apply before any add.
#[derive(Clone, Debug)]
pub enum Op {
    SetOne {
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
        valid_from: i64,
        valid_to: Option<i64>,
        ok: OrderKey,
    },
    CloseOne {
        subject: NodeId,
        predicate: FieldId,
        valid_from: i64,
        ok: OrderKey,
    },
    AddMany {
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
        valid_from: i64,
        valid_to: Option<i64>,
        ok: OrderKey,
    },
    /// End one element's valid-time interval (the Many analogue of [`Op::CloseOne`]): a per-element
    /// version row with no object. When it wins the element's LWW the element leaves the present
    /// set; as-of reads before `valid_from` still see the earlier add rows.
    CloseMany {
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
        valid_from: i64,
        ok: OrderKey,
    },
    RemoveMany {
        subject: NodeId,
        predicate: FieldId,
        observed: Vec<OrderKey>,
    },
    HardDelete {
        subject: NodeId,
        predicate: FieldId,
        ok: OrderKey,
        cardinality: Cardinality,
    },
    /// Set a property on edge `(subject, predicate, object)`; last-writer-wins per `(edge, key)` by
    /// `ok`. Folds into a store independent of the graph state, so graph determinism is unaffected.
    SetEdgeProp {
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
        key: String,
        value: ObjKey,
        ok: OrderKey,
    },
    /// Set node `node`'s entity type; last-writer-wins by `ok`. Node-scoped, **not**
    /// `(subject, predicate)`-keyed — see [`Op::node_attr_node`]. Folds into an independent
    /// node-attribute store, so graph-state determinism is unaffected.
    SetNodeType {
        node: NodeId,
        type_id: FieldId,
        ok: OrderKey,
    },
    /// Set node `node`'s ABAC sensitivity label; last-writer-wins by `ok`. Node-scoped like
    /// [`Op::SetNodeType`].
    SetNodeLabel {
        node: NodeId,
        label: u8,
        ok: OrderKey,
    },
}

impl Op {
    /// Build the assert op for a fact, routed by the predicate's cardinality. Returns `None` if the
    /// predicate is not registered. `seq` is the unique per-op sequence (assigned by the write engine).
    pub fn assert_from(catalog: &Catalog, fact: &Fact, seq: u64) -> Option<Op> {
        let pred = catalog.predicate(fact.predicate)?;
        let ok = OrderKey {
            tx: fact.transaction_time,
            source: fact.provenance.source,
            seq,
        };
        let (subject, predicate) = (fact.subject, fact.predicate);
        Some(match pred.cardinality {
            Cardinality::One => Op::SetOne {
                subject,
                predicate,
                object: ObjKey::of(&fact.object),
                valid_from: fact.valid_time.from,
                valid_to: fact.valid_time.to,
                ok,
            },
            Cardinality::Many => Op::AddMany {
                subject,
                predicate,
                object: ObjKey::of(&fact.object),
                valid_from: fact.valid_time.from,
                valid_to: fact.valid_time.to,
                ok,
            },
        })
    }

    pub(crate) fn key(&self) -> (NodeId, FieldId) {
        match self {
            Op::SetOne {
                subject, predicate, ..
            }
            | Op::CloseOne {
                subject, predicate, ..
            }
            | Op::AddMany {
                subject, predicate, ..
            }
            | Op::CloseMany {
                subject, predicate, ..
            }
            | Op::RemoveMany {
                subject, predicate, ..
            }
            | Op::HardDelete {
                subject, predicate, ..
            }
            | Op::SetEdgeProp {
                subject, predicate, ..
            } => (*subject, *predicate),
            // Node-attribute ops are not `(subject, predicate)`-keyed; callers must branch on
            // `node_attr_node()` first (they route by node), so this is unreachable in practice.
            // Guarded against misuse; never contributes to the graph touched-set.
            Op::SetNodeType { node, .. } | Op::SetNodeLabel { node, .. } => {
                debug_assert!(false, "Op::key() called on a node-attribute op");
                (*node, FieldId::MAX)
            }
        }
    }

    /// The node a node-attribute op ([`Op::SetNodeType`]/[`Op::SetNodeLabel`]) targets, or `None`
    /// for graph ops. Callers route node-attribute ops by node — never through [`Op::key`], which is
    /// the `(subject, predicate)` graph touched-set accessor.
    pub(crate) fn node_attr_node(&self) -> Option<NodeId> {
        match self {
            Op::SetNodeType { node, .. } | Op::SetNodeLabel { node, .. } => Some(*node),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Version {
    object: Option<ObjKey>, // None = close/delete
    valid_from: i64,
    valid_to: Option<i64>, // end of valid interval (None = open); read-time only
}

#[derive(Clone, Debug, Default)]
struct OneState {
    versions: BTreeMap<OrderKey, Version>,
    hd: Option<OrderKey>,
}

/// Per-element version rows keyed by the element object. Each row reuses [`Version`]: an add row
/// carries `object = Some(element)` and its `[valid_from, valid_to)` interval, a close row carries
/// `object = None` and ends the interval from its `valid_from` — the exact shape [`OneState`] uses,
/// so the element-level LWW/as-of semantics are the One-cardinality semantics per element. Rows are
/// keyed by globally-unique [`OrderKey`], so map union stays a join (idempotent, order-free).
/// `removes` still tombstones observed ADD rows outright (the history-destroying hard retract);
/// presence is "the element's greatest live row is an add" — with adds only, exactly the old
/// add-wins OR-Set.
#[derive(Clone, Debug, Default)]
struct ManyState {
    elems: BTreeMap<ObjKey, BTreeMap<OrderKey, Version>>,
    removes: BTreeSet<OrderKey>,
    hd: Option<OrderKey>,
}

#[derive(Clone, Debug)]
enum KeyState {
    One(OneState),
    Many(ManyState),
}

impl KeyState {
    fn new(c: Cardinality) -> Self {
        match c {
            Cardinality::One => KeyState::One(OneState::default()),
            Cardinality::Many => KeyState::Many(ManyState::default()),
        }
    }
}

fn join_hd(a: Option<OrderKey>, b: Option<OrderKey>) -> Option<OrderKey> {
    a.max(b)
}

/// A last-writer-wins register for one edge property: the value and the order key that set it.
#[derive(Clone, Debug, PartialEq)]
struct PropReg {
    value: ObjKey,
    ok: OrderKey,
}

/// A node's last-writer-wins attribute registers: its entity type and its ABAC label, each paired
/// with the order key that set it (mirrors [`PropReg`]). Set-only registers — there is no
/// hard-delete floor for node attributes (see [`Fold::gc`]).
#[derive(Clone, Debug, Default, PartialEq)]
struct NodeAttrState {
    ty: Option<(OrderKey, FieldId)>,
    label: Option<(OrderKey, u8)>,
}

/// Folded graph state keyed by `(subject, predicate)`. `edge_props` is an independent store: per
/// `(subject, predicate)`, per edge object, an LWW register per property key. `node_attrs` is a
/// second independent store: per node, LWW registers for its entity type and ABAC label. Both are
/// independent of the graph fold, so neither affects graph-state determinism.
#[derive(Clone, Debug, Default)]
pub struct Fold {
    keys: BTreeMap<(NodeId, FieldId), KeyState>,
    edge_props: BTreeMap<(NodeId, FieldId), BTreeMap<ObjKey, BTreeMap<String, PropReg>>>,
    node_attrs: BTreeMap<NodeId, NodeAttrState>,
}

/// One history row above the hard-delete floor: `(order key, object, valid_from, valid_to)`.
/// `valid_to = None` means the interval is open (currently valid).
pub type VersionRow = (OrderKey, Option<ObjKey>, i64, Option<i64>);

/// Canonical, deterministic observation; two folds converge iff their snapshots are equal.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub one: BTreeMap<(NodeId, FieldId), Option<ObjKey>>,
    pub one_history: BTreeMap<(NodeId, FieldId), Vec<VersionRow>>,
    pub many: BTreeMap<(NodeId, FieldId), BTreeSet<ObjKey>>,
    /// Per-element version rows of each Many key, ascending by order key — the Many analogue of
    /// `one_history` (an add row carries `Some(element)` and its interval, a close row `None`).
    /// Valid-time as-of reads slice these; the present set above stays the current read.
    pub many_history: BTreeMap<(NodeId, FieldId), BTreeMap<ObjKey, Vec<VersionRow>>>,
    /// Edge properties: `(subject, predicate)` → edge object → property key → value (LWW-resolved).
    pub edge_props: BTreeMap<(NodeId, FieldId), BTreeMap<ObjKey, BTreeMap<String, ObjKey>>>,
    /// Node → entity type (LWW-resolved). A flat `FxHashMap` behind an `Arc`: cloning a snapshot
    /// (pinning a reader / refreshing on publish) is an O(1) refcount bump, while the read-path authz +
    /// type filter — which probes this once per candidate — gets a single-shot flat lookup. Copied on
    /// write only on the rare node-attribute change (creation-time), not on the hot fact path.
    pub node_types: Arc<FxHashMap<NodeId, FieldId>>,
    /// Node → ABAC sensitivity label (LWW-resolved). Same flat-`Arc` rationale as `node_types`.
    pub node_labels: Arc<FxHashMap<NodeId, u8>>,
}

impl Fold {
    /// Fold one diff. Each op is a monotonic join-update (grow a set / raise a max), so any apply
    /// sequence is order-independent.
    pub fn apply(&mut self, op: &Op) {
        match op {
            Op::SetOne {
                object,
                valid_from,
                valid_to,
                ok,
                ..
            } => {
                let st = self
                    .keys
                    .entry(op.key())
                    .or_insert_with(|| KeyState::new(Cardinality::One));
                if let KeyState::One(s) = st {
                    s.versions.insert(
                        *ok,
                        Version {
                            object: Some(object.clone()),
                            valid_from: *valid_from,
                            valid_to: *valid_to,
                        },
                    );
                } else {
                    panic!("cardinality mismatch (expected One)");
                }
            }
            Op::CloseOne { valid_from, ok, .. } => {
                let st = self
                    .keys
                    .entry(op.key())
                    .or_insert_with(|| KeyState::new(Cardinality::One));
                if let KeyState::One(s) = st {
                    s.versions.insert(
                        *ok,
                        Version {
                            object: None,
                            valid_from: *valid_from,
                            valid_to: None,
                        },
                    );
                } else {
                    panic!("cardinality mismatch (expected One)");
                }
            }
            Op::AddMany {
                object,
                valid_from,
                valid_to,
                ok,
                ..
            } => {
                let st = self
                    .keys
                    .entry(op.key())
                    .or_insert_with(|| KeyState::new(Cardinality::Many));
                if let KeyState::Many(s) = st {
                    s.elems.entry(object.clone()).or_default().insert(
                        *ok,
                        Version {
                            object: Some(object.clone()),
                            valid_from: *valid_from,
                            valid_to: *valid_to,
                        },
                    );
                } else {
                    panic!("cardinality mismatch (expected Many)");
                }
            }
            Op::CloseMany {
                object,
                valid_from,
                ok,
                ..
            } => {
                let st = self
                    .keys
                    .entry(op.key())
                    .or_insert_with(|| KeyState::new(Cardinality::Many));
                if let KeyState::Many(s) = st {
                    s.elems.entry(object.clone()).or_default().insert(
                        *ok,
                        Version {
                            object: None,
                            valid_from: *valid_from,
                            valid_to: None,
                        },
                    );
                } else {
                    panic!("cardinality mismatch (expected Many)");
                }
            }
            Op::RemoveMany { observed, .. } => {
                let st = self
                    .keys
                    .entry(op.key())
                    .or_insert_with(|| KeyState::new(Cardinality::Many));
                if let KeyState::Many(s) = st {
                    for t in observed {
                        s.removes.insert(*t);
                    }
                } else {
                    panic!("cardinality mismatch (expected Many)");
                }
            }
            Op::HardDelete {
                ok, cardinality, ..
            } => {
                let st = self
                    .keys
                    .entry(op.key())
                    .or_insert_with(|| KeyState::new(*cardinality));
                match st {
                    KeyState::One(s) => s.hd = join_hd(s.hd, Some(*ok)),
                    KeyState::Many(s) => s.hd = join_hd(s.hd, Some(*ok)),
                }
            }
            Op::SetEdgeProp {
                object,
                key,
                value,
                ok,
                ..
            } => {
                // LWW per (edge, key): the greatest order key wins, independent of apply order.
                let reg = self
                    .edge_props
                    .entry(op.key())
                    .or_default()
                    .entry(object.clone())
                    .or_default()
                    .entry(key.clone());
                match reg {
                    std::collections::btree_map::Entry::Vacant(e) => {
                        e.insert(PropReg {
                            value: value.clone(),
                            ok: *ok,
                        });
                    }
                    std::collections::btree_map::Entry::Occupied(mut e) => {
                        if *ok > e.get().ok {
                            e.insert(PropReg {
                                value: value.clone(),
                                ok: *ok,
                            });
                        }
                    }
                }
            }
            Op::SetNodeType { node, type_id, ok } => {
                // LWW per node: the greatest order key wins, independent of apply order.
                let st = self.node_attrs.entry(*node).or_default();
                if st.ty.is_none_or(|(cur, _)| *ok > cur) {
                    st.ty = Some((*ok, *type_id));
                }
            }
            Op::SetNodeLabel { node, label, ok } => {
                let st = self.node_attrs.entry(*node).or_default();
                if st.label.is_none_or(|(cur, _)| *ok > cur) {
                    st.label = Some((*ok, *label));
                }
            }
        }
    }

    /// Least-upper-bound merge (commutative, associative, idempotent).
    pub fn merge(&mut self, other: &Fold) {
        for (k, st) in &other.keys {
            match self.keys.get_mut(k) {
                None => {
                    self.keys.insert(*k, st.clone());
                }
                Some(KeyState::One(a)) => {
                    if let KeyState::One(b) = st {
                        for (ok, v) in &b.versions {
                            a.versions.insert(*ok, v.clone());
                        }
                        a.hd = join_hd(a.hd, b.hd);
                    } else {
                        panic!("cardinality mismatch on merge");
                    }
                }
                Some(KeyState::Many(a)) => {
                    if let KeyState::Many(b) = st {
                        for (obj, rows) in &b.elems {
                            let e = a.elems.entry(obj.clone()).or_default();
                            for (ok, v) in rows {
                                e.insert(*ok, v.clone());
                            }
                        }
                        for t in &b.removes {
                            a.removes.insert(*t);
                        }
                        a.hd = join_hd(a.hd, b.hd);
                    } else {
                        panic!("cardinality mismatch on merge");
                    }
                }
            }
        }
        // edge properties: LWW per (edge, key) — keep the register with the greater order key.
        for (k, objs) in &other.edge_props {
            let me = self.edge_props.entry(*k).or_default();
            for (obj, props) in objs {
                let mo = me.entry(obj.clone()).or_default();
                for (key, reg) in props {
                    match mo.get(key) {
                        Some(cur) if cur.ok >= reg.ok => {}
                        _ => {
                            mo.insert(key.clone(), reg.clone());
                        }
                    }
                }
            }
        }
        // node attributes: LWW per node per register — keep the register with the greater order key.
        for (node, st) in &other.node_attrs {
            let me = self.node_attrs.entry(*node).or_default();
            if let Some((ok, ty)) = st.ty
                && me.ty.is_none_or(|(cur, _)| ok > cur)
            {
                me.ty = Some((ok, ty));
            }
            if let Some((ok, label)) = st.label
                && me.label.is_none_or(|(cur, _)| ok > cur)
            {
                me.label = Some((ok, label));
            }
        }
    }

    /// Drop state dominated by the hard-delete floor; provably preserves `observe()`.
    ///
    /// `node_attrs` is intentionally untouched: node type/label are set-only LWW registers with no
    /// hard-delete floor, so there is nothing to collect (nothing is ever dominated-and-removable).
    pub fn gc(&mut self) {
        for st in self.keys.values_mut() {
            match st {
                KeyState::One(s) => {
                    if let Some(h) = s.hd {
                        s.versions.retain(|ok, _| *ok > h);
                    }
                }
                KeyState::Many(s) => {
                    if let Some(h) = s.hd {
                        for rows in s.elems.values_mut() {
                            rows.retain(|ok, _| *ok > h);
                        }
                        s.elems.retain(|_, rows| !rows.is_empty());
                        s.removes.retain(|t| *t > h);
                    }
                }
            }
        }
    }

    /// The live add-tags for a cardinality-Many element `(subject, predicate, object)` — the tags a
    /// retraction must observe-and-remove. Close rows are not tags (a retract erases adds; a close
    /// that then wins the element's LWW keeps it absent). Empty if the element has no live adds.
    /// This is the resolver the DB↔ETL "diff reflection" needs (turn "remove this edge" into an
    /// observed-remove).
    pub fn live_tags(&self, subject: NodeId, predicate: FieldId, object: &ObjKey) -> Vec<OrderKey> {
        match self.keys.get(&(subject, predicate)) {
            Some(KeyState::Many(s)) => s
                .elems
                .get(object)
                .into_iter()
                .flatten()
                .filter(|(ok, v)| {
                    v.object.is_some() && !s.removes.contains(ok) && s.hd.is_none_or(|h| **ok > h)
                })
                .map(|(ok, _)| *ok)
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Ingest no-op probe for a cardinality-Many assertion: the element is currently PRESENT (its
    /// greatest live row is an add) and some live add row matches `(source, valid_from, valid_to)`
    /// exactly. A re-assertion matching this changes nothing; anything else — a different source
    /// (corroboration), a corrected interval, or a re-grant after a close — must append.
    pub fn many_live_asserted(
        &self,
        subject: NodeId,
        predicate: FieldId,
        object: &ObjKey,
        source: FieldId,
        valid_from: i64,
        valid_to: Option<i64>,
    ) -> bool {
        let Some(KeyState::Many(s)) = self.keys.get(&(subject, predicate)) else {
            return false;
        };
        let Some(rows) = s.elems.get(object) else {
            return false;
        };
        let live: Vec<(&OrderKey, &Version)> = rows
            .iter()
            .filter(|(ok, _)| !s.removes.contains(ok) && s.hd.is_none_or(|h| **ok > h))
            .collect();
        let Some((_, top)) = live.last() else {
            return false;
        };
        top.object.is_some()
            && live.iter().any(|(ok, v)| {
                v.object.is_some()
                    && ok.source == source
                    && v.valid_from == valid_from
                    && v.valid_to == valid_to
            })
    }

    /// Canonical observation. Keys with no live state are omitted.
    pub fn observe(&self) -> Snapshot {
        let mut snap = Snapshot::default();
        // union of graph keys and edge-property keys (a property may exist for an edge whose key
        // carries no other live state, and vice versa).
        let keys: BTreeSet<&(NodeId, FieldId)> =
            self.keys.keys().chain(self.edge_props.keys()).collect();
        for k in keys {
            self.observe_key_into(k, &mut snap);
        }
        // node attributes live in a disjoint region of the snapshot (node_types/node_labels), so
        // projecting them independently of the graph keys yields the same canonical observation.
        for node in self.node_attrs.keys() {
            self.observe_node_into(*node, &mut snap);
        }
        snap
    }

    /// Re-observe a single `(subject, predicate)` key into an existing snapshot — the incremental
    /// form of [`Fold::observe`]: after folding tail ops, refreshing just the touched keys keeps a
    /// cached snapshot current in O(touched) instead of O(state). Removes the key's entries when it
    /// has no live state (mirrors observe's omission).
    pub fn observe_key_into(&self, k: &(NodeId, FieldId), snap: &mut Snapshot) {
        snap.one.remove(k);
        snap.one_history.remove(k);
        snap.many.remove(k);
        snap.many_history.remove(k);
        snap.edge_props.remove(k);
        // edge properties for this key: project each edge's LWW-resolved property values.
        if let Some(objs) = self.edge_props.get(k) {
            let projected: BTreeMap<ObjKey, BTreeMap<String, ObjKey>> = objs
                .iter()
                .map(|(obj, props)| {
                    let vals = props
                        .iter()
                        .map(|(key, reg)| (key.clone(), reg.value.clone()))
                        .collect();
                    (obj.clone(), vals)
                })
                .collect();
            if !projected.is_empty() {
                snap.edge_props.insert(*k, projected);
            }
        }
        match self.keys.get(k) {
            Some(KeyState::One(s)) => {
                let live: Vec<(OrderKey, Version)> = s
                    .versions
                    .iter()
                    .filter(|(ok, _)| s.hd.is_none_or(|h| **ok > h))
                    .map(|(ok, v)| (*ok, v.clone()))
                    .collect();
                if let Some((_, top)) = live.last() {
                    snap.one.insert(*k, top.object.clone());
                    snap.one_history.insert(
                        *k,
                        live.iter()
                            .map(|(ok, v)| (*ok, v.object.clone(), v.valid_from, v.valid_to))
                            .collect(),
                    );
                }
            }
            Some(KeyState::Many(s)) => {
                let mut present = BTreeSet::new();
                let mut history: BTreeMap<ObjKey, Vec<VersionRow>> = BTreeMap::new();
                for (obj, rows) in &s.elems {
                    let live: Vec<VersionRow> = rows
                        .iter()
                        .filter(|(ok, _)| !s.removes.contains(ok) && s.hd.is_none_or(|h| **ok > h))
                        .map(|(ok, v)| (*ok, v.object.clone(), v.valid_from, v.valid_to))
                        .collect();
                    if let Some((_, top, _, _)) = live.last() {
                        // present iff the element's greatest live row is an add — with adds only
                        // this is exactly the old add-wins OR-Set observation
                        if top.is_some() {
                            present.insert(obj.clone());
                        }
                        history.insert(obj.clone(), live);
                    }
                }
                if !present.is_empty() {
                    snap.many.insert(*k, present);
                }
                if !history.is_empty() {
                    snap.many_history.insert(*k, history);
                }
            }
            None => {}
        }
    }

    /// Re-observe a single node's attributes (entity type + ABAC label) into an existing snapshot —
    /// the node-attribute analogue of [`Fold::observe_key_into`]. Removes the node's entries then
    /// re-projects from its LWW registers (remove-then-reinsert), so an incremental refresh over the
    /// touched nodes equals a full [`Fold::observe`]. A register with no live value leaves the node
    /// absent (mirrors observe's omission).
    pub fn observe_node_into(&self, node: NodeId, snap: &mut Snapshot) {
        let types = Arc::make_mut(&mut snap.node_types);
        let labels = Arc::make_mut(&mut snap.node_labels);
        types.remove(&node);
        labels.remove(&node);
        if let Some(st) = self.node_attrs.get(&node) {
            if let Some((_, ty)) = st.ty {
                types.insert(node, ty);
            }
            if let Some((_, label)) = st.label {
                labels.insert(node, label);
            }
        }
    }
}

// --- compaction codec --------------------------------------------------------------------------
// The fold serialized VERBATIM — every version row with its ORIGINAL order key, the observed-remove
// tombstones, and the hard-delete floors — so a decoded fold observes identically AND keeps every
// LWW / as-of tie-break. This is what a compaction snapshot persists: superseded rows included
// (as-of reads are part of the read contract); only what `gc()` already provably drops is gone
// (the compaction path runs gc first). Deterministic bytes: BTreeMap iteration order throughout.

fn put_opt_i64(buf: &mut Vec<u8>, v: Option<i64>) {
    match v {
        Some(t) => {
            buf.push(1);
            wal::put_i64(buf, t);
        }
        None => buf.push(0),
    }
}

fn put_opt_ok(buf: &mut Vec<u8>, ok: Option<OrderKey>) {
    match ok {
        Some(k) => {
            buf.push(1);
            wal::put_orderkey(buf, &k);
        }
        None => buf.push(0),
    }
}

fn read_opt_i64(r: &mut wal::Reader) -> Option<Option<i64>> {
    match r.u8()? {
        0 => Some(None),
        1 => Some(Some(r.i64()?)),
        _ => None,
    }
}

fn read_opt_ok(r: &mut wal::Reader) -> Option<Option<OrderKey>> {
    match r.u8()? {
        0 => Some(None),
        1 => Some(Some(r.orderkey()?)),
        _ => None,
    }
}

impl Fold {
    /// Serialize this fold into `buf` — see the codec note above.
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        wal::put_u32(buf, self.keys.len() as u32);
        for ((node, field), st) in &self.keys {
            wal::put_u64(buf, *node);
            wal::put_u32(buf, *field);
            match st {
                KeyState::One(s) => {
                    buf.push(0);
                    put_opt_ok(buf, s.hd);
                    wal::put_u32(buf, s.versions.len() as u32);
                    for (ok, v) in &s.versions {
                        wal::put_orderkey(buf, ok);
                        match &v.object {
                            Some(o) => {
                                buf.push(1);
                                wal::put_objkey(buf, o);
                            }
                            None => buf.push(0),
                        }
                        wal::put_i64(buf, v.valid_from);
                        put_opt_i64(buf, v.valid_to);
                    }
                }
                KeyState::Many(s) => {
                    buf.push(1);
                    put_opt_ok(buf, s.hd);
                    wal::put_u32(buf, s.elems.len() as u32);
                    for (obj, rows) in &s.elems {
                        wal::put_objkey(buf, obj);
                        wal::put_u32(buf, rows.len() as u32);
                        for (ok, v) in rows {
                            wal::put_orderkey(buf, ok);
                            // an add row's object IS the element — one flag reconstructs it exactly
                            buf.push(v.object.is_some() as u8);
                            wal::put_i64(buf, v.valid_from);
                            put_opt_i64(buf, v.valid_to);
                        }
                    }
                    wal::put_u32(buf, s.removes.len() as u32);
                    for t in &s.removes {
                        wal::put_orderkey(buf, t);
                    }
                }
            }
        }
        wal::put_u32(buf, self.edge_props.len() as u32);
        for ((node, field), objs) in &self.edge_props {
            wal::put_u64(buf, *node);
            wal::put_u32(buf, *field);
            wal::put_u32(buf, objs.len() as u32);
            for (obj, props) in objs {
                wal::put_objkey(buf, obj);
                wal::put_u32(buf, props.len() as u32);
                for (key, reg) in props {
                    wal::put_str(buf, key);
                    wal::put_objkey(buf, &reg.value);
                    wal::put_orderkey(buf, &reg.ok);
                }
            }
        }
        wal::put_u32(buf, self.node_attrs.len() as u32);
        for (node, st) in &self.node_attrs {
            wal::put_u64(buf, *node);
            match st.ty {
                Some((ok, ty)) => {
                    buf.push(1);
                    wal::put_orderkey(buf, &ok);
                    wal::put_u32(buf, ty);
                }
                None => buf.push(0),
            }
            match st.label {
                Some((ok, label)) => {
                    buf.push(1);
                    wal::put_orderkey(buf, &ok);
                    buf.push(label);
                }
                None => buf.push(0),
            }
        }
    }

    /// Decode a fold serialized by [`Fold::encode_into`]. `None` on a malformed buffer (a caller
    /// treats that like a torn WAL tail — refuse the snapshot, never guess).
    pub fn decode(bytes: &[u8]) -> Option<Fold> {
        let mut r = wal::Reader::new(bytes);
        let mut f = Fold::default();
        for _ in 0..r.u32()? {
            let node = r.u64()?;
            let field = r.u32()?;
            let st = match r.u8()? {
                0 => {
                    let hd = read_opt_ok(&mut r)?;
                    let mut versions = BTreeMap::new();
                    for _ in 0..r.u32()? {
                        let ok = r.orderkey()?;
                        let object = match r.u8()? {
                            0 => None,
                            1 => Some(r.objkey()?),
                            _ => return None,
                        };
                        let valid_from = r.i64()?;
                        let valid_to = read_opt_i64(&mut r)?;
                        versions.insert(
                            ok,
                            Version {
                                object,
                                valid_from,
                                valid_to,
                            },
                        );
                    }
                    KeyState::One(OneState { versions, hd })
                }
                1 => {
                    let hd = read_opt_ok(&mut r)?;
                    let mut elems = BTreeMap::new();
                    for _ in 0..r.u32()? {
                        let obj = r.objkey()?;
                        let mut rows = BTreeMap::new();
                        for _ in 0..r.u32()? {
                            let ok = r.orderkey()?;
                            let is_add = match r.u8()? {
                                0 => false,
                                1 => true,
                                _ => return None,
                            };
                            let valid_from = r.i64()?;
                            let valid_to = read_opt_i64(&mut r)?;
                            rows.insert(
                                ok,
                                Version {
                                    object: is_add.then(|| obj.clone()),
                                    valid_from,
                                    valid_to,
                                },
                            );
                        }
                        elems.insert(obj, rows);
                    }
                    let mut removes = BTreeSet::new();
                    for _ in 0..r.u32()? {
                        removes.insert(r.orderkey()?);
                    }
                    KeyState::Many(ManyState { elems, removes, hd })
                }
                _ => return None,
            };
            f.keys.insert((node, field), st);
        }
        for _ in 0..r.u32()? {
            let node = r.u64()?;
            let field = r.u32()?;
            let mut objs = BTreeMap::new();
            for _ in 0..r.u32()? {
                let obj = r.objkey()?;
                let mut props = BTreeMap::new();
                for _ in 0..r.u32()? {
                    let key = r.string()?;
                    let value = r.objkey()?;
                    let ok = r.orderkey()?;
                    props.insert(key, PropReg { value, ok });
                }
                objs.insert(obj, props);
            }
            f.edge_props.insert((node, field), objs);
        }
        for _ in 0..r.u32()? {
            let node = r.u64()?;
            let ty = match r.u8()? {
                0 => None,
                1 => Some((r.orderkey()?, r.u32()?)),
                _ => return None,
            };
            let label = match r.u8()? {
                0 => None,
                1 => Some((r.orderkey()?, r.u8()?)),
                _ => return None,
            };
            f.node_attrs.insert(node, NodeAttrState { ty, label });
        }
        r.done().then_some(f)
    }
}

/// Fold a slice of ops in order.
pub fn fold(ops: &[Op]) -> Fold {
    let mut f = Fold::default();
    for op in ops {
        f.apply(op);
    }
    f
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Range, RelProps};
    use crate::fact::{Provenance, ProvenanceKind, ValidTime};

    #[test]
    fn assert_from_routes_by_cardinality() {
        let mut c = Catalog::new();
        let person = c.register_type("Person");
        let skill = c.register_type("Skill");
        let project = c.register_type("Project");
        c.register_predicate(
            "has-skill",
            Cardinality::Many,
            RelProps::default(),
            person,
            Range::Type(skill),
        );
        c.register_predicate(
            "member-of",
            Cardinality::One,
            RelProps::default(),
            person,
            Range::Type(project),
        );

        let mk = |pred: FieldId, obj: u64| Fact {
            subject: 1,
            predicate: pred,
            object: Object::Node(obj),
            valid_time: ValidTime::from(0),
            transaction_time: 1,
            provenance: Provenance {
                kind: ProvenanceKind::Asserted,
                source: 0,
            },
            confidence: 1.0,
        };
        let hs = c.field_id("has-skill").unwrap();
        let mo = c.field_id("member-of").unwrap();
        assert!(matches!(
            Op::assert_from(&c, &mk(hs, 2), 0),
            Some(Op::AddMany { .. })
        ));
        assert!(matches!(
            Op::assert_from(&c, &mk(mo, 3), 1),
            Some(Op::SetOne { .. })
        ));
        assert!(Op::assert_from(&c, &mk(999, 4), 2).is_none());
    }

    fn ok(tx: u64, src: FieldId, seq: u64) -> OrderKey {
        OrderKey {
            tx,
            source: src,
            seq,
        }
    }

    #[test]
    fn lww_tiebreak_is_total_order() {
        let s = 0u64;
        let p = 0u32;
        let a = Op::SetOne {
            subject: s,
            predicate: p,
            object: ObjKey::Node(10),
            valid_from: 0,
            valid_to: None,
            ok: ok(1, 0, 0),
        };
        let b = Op::SetOne {
            subject: s,
            predicate: p,
            object: ObjKey::Node(20),
            valid_from: 0,
            valid_to: None,
            ok: ok(1, 2, 1),
        };
        let s1 = fold(&[a.clone(), b.clone()]).observe();
        let s2 = fold(&[b, a]).observe();
        assert_eq!(s1, s2);
        assert_eq!(s1.one.get(&(s, p)), Some(&Some(ObjKey::Node(20))));
    }

    #[test]
    fn hard_delete_purges_then_allows_reassert() {
        let s = 0u64;
        let p = 100u32;
        let ops = vec![
            Op::AddMany {
                subject: s,
                predicate: p,
                object: ObjKey::Node(1),
                valid_from: 0,
                valid_to: None,
                ok: ok(1, 0, 0),
            },
            Op::HardDelete {
                subject: s,
                predicate: p,
                ok: ok(5, 0, 1),
                cardinality: Cardinality::Many,
            },
            Op::AddMany {
                subject: s,
                predicate: p,
                object: ObjKey::Node(2),
                valid_from: 0,
                valid_to: None,
                ok: ok(9, 0, 2),
            },
        ];
        let snap = fold(&ops).observe();
        let present = snap.many.get(&(s, p)).cloned().unwrap_or_default();
        assert!(!present.contains(&ObjKey::Node(1)));
        assert!(present.contains(&ObjKey::Node(2)));
        let mut rev = ops.clone();
        rev.reverse();
        assert_eq!(snap, fold(&rev).observe());
    }

    #[test]
    fn or_set_concurrent_add_survives_remove() {
        let s = 0u64;
        let p = 100u32;
        let tag_a = ok(1, 0, 0);
        let tag_b = ok(1, 1, 1);
        let ops = vec![
            Op::AddMany {
                subject: s,
                predicate: p,
                object: ObjKey::Node(7),
                valid_from: 0,
                valid_to: None,
                ok: tag_a,
            },
            Op::AddMany {
                subject: s,
                predicate: p,
                object: ObjKey::Node(7),
                valid_from: 0,
                valid_to: None,
                ok: tag_b,
            },
            Op::RemoveMany {
                subject: s,
                predicate: p,
                observed: vec![tag_a],
            },
        ];
        assert!(
            fold(&ops)
                .observe()
                .many
                .get(&(s, p))
                .unwrap()
                .contains(&ObjKey::Node(7))
        );
    }

    fn add_at(s: u64, p: u32, o: u64, vf: i64, okey: OrderKey) -> Op {
        Op::AddMany {
            subject: s,
            predicate: p,
            object: ObjKey::Node(o),
            valid_from: vf,
            valid_to: None,
            ok: okey,
        }
    }

    fn close_at(s: u64, p: u32, o: u64, vf: i64, okey: OrderKey) -> Op {
        Op::CloseMany {
            subject: s,
            predicate: p,
            object: ObjKey::Node(o),
            valid_from: vf,
            ok: okey,
        }
    }

    #[test]
    fn close_many_removes_from_present_set_but_keeps_history() {
        let (s, p) = (0u64, 100u32);
        let snap = fold(&[
            add_at(s, p, 7, 100, ok(1, 0, 0)),
            close_at(s, p, 7, 200, ok(2, 0, 1)),
        ])
        .observe();
        // the winning row is the close → the element leaves the CURRENT set…
        assert!(!snap.many.contains_key(&(s, p)));
        // …but both rows stay sliceable in the element's history
        let rows = snap.many_history[&(s, p)][&ObjKey::Node(7)].clone();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1, Some(ObjKey::Node(7))); // the add
        assert_eq!(rows[1].1, None); // the close
    }

    #[test]
    fn close_many_then_readd_restores_presence_order_free() {
        let (s, p) = (0u64, 100u32);
        let ops = vec![
            add_at(s, p, 7, 100, ok(1, 0, 0)),
            close_at(s, p, 7, 200, ok(2, 0, 1)),
            add_at(s, p, 7, 300, ok(3, 0, 2)),
        ];
        let snap = fold(&ops).observe();
        assert!(snap.many[&(s, p)].contains(&ObjKey::Node(7)));
        // any arrival order converges to the same observation (join-semilattice)
        let mut rev = ops.clone();
        rev.reverse();
        assert_eq!(snap, fold(&rev).observe());
    }

    #[test]
    fn retract_after_close_leaves_element_absent_and_close_unobservable_by_tags() {
        let (s, p) = (0u64, 100u32);
        let f = fold(&[
            add_at(s, p, 7, 100, ok(1, 0, 0)),
            close_at(s, p, 7, 200, ok(2, 0, 1)),
        ]);
        // live_tags exposes ADD rows only — a retraction never observes the close row
        assert_eq!(f.live_tags(s, p, &ObjKey::Node(7)), vec![ok(1, 0, 0)]);
        let mut f2 = f.clone();
        f2.apply(&Op::RemoveMany {
            subject: s,
            predicate: p,
            observed: vec![ok(1, 0, 0)],
        });
        let snap = f2.observe();
        assert!(!snap.many.contains_key(&(s, p)));
        // only the close row survives in history
        assert_eq!(snap.many_history[&(s, p)][&ObjKey::Node(7)].len(), 1);
    }

    #[test]
    fn many_live_asserted_probe_tracks_presence_and_interval() {
        let (s, p) = (0u64, 100u32);
        let o = ObjKey::Node(7);
        let mut f = fold(&[add_at(s, p, 7, 100, ok(1, 3, 0))]);
        assert!(f.many_live_asserted(s, p, &o, 3, 100, None)); // identical re-assertion → suppress
        assert!(!f.many_live_asserted(s, p, &o, 4, 100, None)); // different source → corroborate
        assert!(!f.many_live_asserted(s, p, &o, 3, 150, None)); // corrected interval → append
        f.apply(&close_at(s, p, 7, 200, ok(2, 3, 1)));
        // closed → not present → a re-grant must append even with the original interval
        assert!(!f.many_live_asserted(s, p, &o, 3, 100, None));
    }
}
