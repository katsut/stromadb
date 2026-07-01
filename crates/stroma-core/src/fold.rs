//! The fold: stream diffs → current graph state, per `(subject, predicate)`.
//!
//! Each key's state is a **join-semilattice** (commutative + associative + idempotent merge), so the
//! fold converges under any arrival order / source partition / redelivery — the basis for
//! deterministic replay and audit (algebra validated in Phase 0 `poc-fold-determinism`).
//!
//! Cardinality (from the [`Catalog`]) drives behaviour: `One` → LWW-Register with history (supersede);
//! `Many` → OR-Set (accumulate). Hard-delete is a max-register floor that purges everything `<= floor`
//! (re-assertion above the floor survives).

use std::collections::{BTreeMap, BTreeSet};

use crate::catalog::{Cardinality, Catalog};
use crate::fact::{Fact, FieldId, NodeId, Object, Value};

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

/// A stream diff. `SetOne`/`CloseOne` target cardinality-`One` keys, `AddMany`/`RemoveMany` target
/// cardinality-`Many` keys; `HardDelete` carries the key's cardinality so it can apply before any add.
#[derive(Clone, Debug)]
pub enum Op {
    SetOne {
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
        valid_from: i64,
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
                ok,
            },
            Cardinality::Many => Op::AddMany {
                subject,
                predicate,
                object: ObjKey::of(&fact.object),
                ok,
            },
        })
    }

    fn key(&self) -> (NodeId, FieldId) {
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
            | Op::RemoveMany {
                subject, predicate, ..
            }
            | Op::HardDelete {
                subject, predicate, ..
            } => (*subject, *predicate),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Version {
    object: Option<ObjKey>, // None = close/delete
    valid_from: i64,
}

#[derive(Clone, Debug, Default)]
struct OneState {
    versions: BTreeMap<OrderKey, Version>,
    hd: Option<OrderKey>,
}

#[derive(Clone, Debug, Default)]
struct ManyState {
    adds: BTreeMap<ObjKey, BTreeSet<OrderKey>>,
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

/// Folded graph state keyed by `(subject, predicate)`.
#[derive(Clone, Debug, Default)]
pub struct Fold {
    keys: BTreeMap<(NodeId, FieldId), KeyState>,
}

/// One history row above the hard-delete floor.
pub type VersionRow = (OrderKey, Option<ObjKey>, i64);

/// Canonical, deterministic observation; two folds converge iff their snapshots are equal.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub one: BTreeMap<(NodeId, FieldId), Option<ObjKey>>,
    pub one_history: BTreeMap<(NodeId, FieldId), Vec<VersionRow>>,
    pub many: BTreeMap<(NodeId, FieldId), BTreeSet<ObjKey>>,
}

impl Fold {
    /// Fold one diff. Each op is a monotonic join-update (grow a set / raise a max), so any apply
    /// sequence is order-independent.
    pub fn apply(&mut self, op: &Op) {
        match op {
            Op::SetOne {
                object,
                valid_from,
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
                        },
                    );
                } else {
                    panic!("cardinality mismatch (expected One)");
                }
            }
            Op::AddMany { object, ok, .. } => {
                let st = self
                    .keys
                    .entry(op.key())
                    .or_insert_with(|| KeyState::new(Cardinality::Many));
                if let KeyState::Many(s) = st {
                    s.adds.entry(object.clone()).or_default().insert(*ok);
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
                        for (obj, tags) in &b.adds {
                            let e = a.adds.entry(obj.clone()).or_default();
                            for t in tags {
                                e.insert(*t);
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
    }

    /// Drop state dominated by the hard-delete floor; provably preserves `observe()`.
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
                        for tags in s.adds.values_mut() {
                            tags.retain(|t| *t > h);
                        }
                        s.adds.retain(|_, tags| !tags.is_empty());
                        s.removes.retain(|t| *t > h);
                    }
                }
            }
        }
    }

    /// The live add-tags for a cardinality-Many element `(subject, predicate, object)` — the tags a
    /// retraction must observe-and-remove. Empty if the element isn't currently present. This is the
    /// resolver the DB↔ETL "diff reflection" needs (turn "remove this edge" into an observed-remove).
    pub fn live_tags(&self, subject: NodeId, predicate: FieldId, object: &ObjKey) -> Vec<OrderKey> {
        match self.keys.get(&(subject, predicate)) {
            Some(KeyState::Many(s)) => s
                .adds
                .get(object)
                .into_iter()
                .flatten()
                .filter(|t| !s.removes.contains(t) && s.hd.is_none_or(|h| **t > h))
                .copied()
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Canonical observation. Keys with no live state are omitted.
    pub fn observe(&self) -> Snapshot {
        let mut snap = Snapshot::default();
        for (k, st) in &self.keys {
            match st {
                KeyState::One(s) => {
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
                                .map(|(ok, v)| (*ok, v.object.clone(), v.valid_from))
                                .collect(),
                        );
                    }
                }
                KeyState::Many(s) => {
                    let mut present = BTreeSet::new();
                    for (obj, tags) in &s.adds {
                        let live = tags
                            .iter()
                            .any(|t| !s.removes.contains(t) && s.hd.is_none_or(|h| *t > h));
                        if live {
                            present.insert(obj.clone());
                        }
                    }
                    if !present.is_empty() {
                        snap.many.insert(*k, present);
                    }
                }
            }
        }
        snap
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
            ok: ok(1, 0, 0),
        };
        let b = Op::SetOne {
            subject: s,
            predicate: p,
            object: ObjKey::Node(20),
            valid_from: 0,
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
                ok: tag_a,
            },
            Op::AddMany {
                subject: s,
                predicate: p,
                object: ObjKey::Node(7),
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
}
