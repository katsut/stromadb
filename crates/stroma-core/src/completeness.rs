//! Completeness: report, per node of a type, the schema-required predicates that are *absent*.
//!
//! This generalizes the conformance op's absence case ([`crate::conformance`]) into a schema-driven
//! "expected-but-absent" check. Given a node's type, the caller supplies the predicates that *should*
//! carry a value for a node of that type (the `required` set); for each node of the type the evaluator
//! reports the required predicates that currently have no value. There is no new inference and no
//! reasoner — a required predicate is "present" iff one of the existing deterministic read primitives
//! ([`point_one`], [`expand`]) yields something, so the whole composition is a pure function of the
//! snapshot and yields the same result for the same snapshot (sorted by node id for stable output).
//!
//! The `required` set comes from the request (an explicit list of predicate names), not from new
//! schema state — the catalog does not carry a per-predicate "required" flag. So the op is: "for every
//! node of type T, which of these required predicates has no value?". Names are resolved against the
//! [`Catalog`]; the DB boundary rejects unknown names up front (see [`unresolved_names`]).

use crate::catalog::Catalog;
use crate::fact::{FieldId, NodeId};
use crate::fold::Snapshot;
use crate::query::{expand, point_one};

/// One node with at least one absent required predicate: the node id plus the missing predicate names
/// in the request's order. Nodes with every required predicate present are omitted from the result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Incomplete {
    pub node: NodeId,
    pub missing: Vec<String>,
}

/// The names a completeness request references that the catalog does not know — the DB boundary
/// resolves this to a clear error before evaluating. Empty = every name resolves.
pub fn unresolved_names(type_name: &str, required: &[String], cat: &Catalog) -> Vec<String> {
    let mut names: Vec<&str> = vec![type_name];
    names.extend(required.iter().map(String::as_str));
    names
        .into_iter()
        .filter(|n| cat.field_id(n).is_none())
        .map(str::to_string)
        .collect()
}

/// Evaluate expected-but-absent completeness over `snap`: one [`Incomplete`] per node of `type_name`
/// that has ≥1 absent required predicate, sorted by node id.
///
/// Post-authz: a node whose ABAC label is not permitted by `principal_labels` is skipped (same bit-test
/// as the other read ops). Names are resolved against `cat`; an unknown type yields no nodes, and a
/// required predicate name unknown to the catalog is treated as absent on every node (the DB boundary
/// rejects unknown names up front via [`unresolved_names`], so this is only a defensive fallback).
pub fn evaluate(
    snap: &Snapshot,
    cat: &Catalog,
    type_name: &str,
    required: &[String],
    principal_labels: u32,
) -> Vec<Incomplete> {
    let Some(ty) = cat.field_id(type_name) else {
        return Vec::new();
    };
    // resolve the required predicate names once (in request order); an unknown name stays `None` and
    // is treated as absent on every node below.
    let resolved: Vec<(&str, Option<FieldId>)> = required
        .iter()
        .map(|p| (p.as_str(), cat.field_id(p)))
        .collect();

    let mut nodes: Vec<NodeId> = snap
        .node_types
        .iter()
        .filter(|&(_, &t)| t == ty)
        .map(|(&n, _)| n)
        .filter(|&n| visible(snap, n, principal_labels))
        .collect();
    nodes.sort_unstable();
    nodes
        .into_iter()
        .filter_map(|n| {
            let missing: Vec<String> = resolved
                .iter()
                .filter(|(_, pid)| !present(snap, n, *pid))
                .map(|(name, _)| (*name).to_string())
                .collect();
            (!missing.is_empty()).then_some(Incomplete { node: n, missing })
        })
        .collect()
}

/// Whether required predicate `pid` has any value on `node` — a one-cardinality current value
/// ([`point_one`]) or a non-empty node-valued expansion ([`expand`], covering many-cardinality). An
/// unresolved predicate (`None`) is treated as absent.
fn present(snap: &Snapshot, node: NodeId, pid: Option<FieldId>) -> bool {
    match pid {
        None => false,
        Some(p) => point_one(snap, node, p).is_some() || !expand(snap, node, p).is_empty(),
    }
}

/// Whether `node` is visible to a principal with `allowed_labels` (unlabeled = public).
fn visible(snap: &Snapshot, node: NodeId, allowed_labels: u32) -> bool {
    snap.node_labels
        .get(&node)
        .is_none_or(|&l| (allowed_labels >> l) & 1 == 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Cardinality, Range, RelProps, ValueType};
    use crate::fold::{ObjKey, Op, OrderKey, fold};

    fn ok(seq: u64) -> OrderKey {
        OrderKey {
            tx: seq,
            source: 0,
            seq,
        }
    }

    fn set_one(
        ops: &mut Vec<Op>,
        seq: &mut u64,
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
    ) {
        ops.push(Op::SetOne {
            subject,
            predicate,
            object,
            valid_from: 0,
            valid_to: None,
            ok: ok(*seq),
        });
        *seq += 1;
    }

    fn add_many(
        ops: &mut Vec<Op>,
        seq: &mut u64,
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
    ) {
        ops.push(Op::AddMany {
            subject,
            predicate,
            object,
            ok: ok(*seq),
        });
        *seq += 1;
    }

    fn set_type(ops: &mut Vec<Op>, seq: &mut u64, node: NodeId, type_id: FieldId) {
        ops.push(Op::SetNodeType {
            node,
            type_id,
            ok: ok(*seq),
        });
        *seq += 1;
    }

    fn text(s: &str) -> ObjKey {
        ObjKey::Text(s.to_string())
    }

    struct Fixture {
        snap: Snapshot,
        cat: Catalog,
        required: Vec<String>,
    }

    // Issues with three required predicates:
    //   status      (One, Text)   — a one-cardinality value
    //   assigned-to (One, Person) — a one-cardinality node ref
    //   reviewers   (Many, Person)— a many-cardinality node ref
    // Issue 1: all three present            → complete (omitted)
    // Issue 2: status only                  → missing assigned-to, reviewers
    // Issue 3: assigned-to + reviewers only  → missing status
    // Issue 4: nothing                       → missing all three (in request order)
    fn fixture() -> Fixture {
        let mut cat = Catalog::new();
        let issue = cat.register_type("Issue");
        let person = cat.register_type("Person");
        let d = RelProps::default();
        let status = cat.register_predicate(
            "status",
            Cardinality::One,
            d,
            issue,
            Range::Value(ValueType::Text),
        );
        let assigned_to = cat.register_predicate(
            "assigned-to",
            Cardinality::One,
            d,
            issue,
            Range::Type(person),
        );
        let reviewers = cat.register_predicate(
            "reviewers",
            Cardinality::Many,
            d,
            issue,
            Range::Type(person),
        );

        let mut ops: Vec<Op> = Vec::new();
        let mut seq = 0u64;

        set_type(&mut ops, &mut seq, 10, person);
        set_type(&mut ops, &mut seq, 11, person);
        for id in 1..=4u64 {
            set_type(&mut ops, &mut seq, id, issue);
        }

        // Issue 1 — complete.
        set_one(&mut ops, &mut seq, 1, status, text("open"));
        set_one(&mut ops, &mut seq, 1, assigned_to, ObjKey::Node(10));
        add_many(&mut ops, &mut seq, 1, reviewers, ObjKey::Node(11));

        // Issue 2 — only status.
        set_one(&mut ops, &mut seq, 2, status, text("open"));

        // Issue 3 — assigned-to + reviewers, no status.
        set_one(&mut ops, &mut seq, 3, assigned_to, ObjKey::Node(10));
        add_many(&mut ops, &mut seq, 3, reviewers, ObjKey::Node(10));
        add_many(&mut ops, &mut seq, 3, reviewers, ObjKey::Node(11));

        // Issue 4 — nothing.

        Fixture {
            snap: fold(&ops).observe(),
            cat,
            required: vec!["status".into(), "assigned-to".into(), "reviewers".into()],
        }
    }

    #[test]
    fn reports_absent_required_predicates() {
        let f = fixture();
        let out = evaluate(&f.snap, &f.cat, "Issue", &f.required, u32::MAX);
        // sorted by node id; complete node (1) omitted.
        assert_eq!(
            out,
            vec![
                Incomplete {
                    node: 2,
                    missing: vec!["assigned-to".into(), "reviewers".into()],
                },
                Incomplete {
                    node: 3,
                    missing: vec!["status".into()],
                },
                Incomplete {
                    node: 4,
                    missing: vec!["status".into(), "assigned-to".into(), "reviewers".into()],
                },
            ]
        );
    }

    #[test]
    fn unknown_type_yields_nothing() {
        let f = fixture();
        assert!(evaluate(&f.snap, &f.cat, "Nope", &f.required, u32::MAX).is_empty());
    }

    #[test]
    fn post_authz_skips_hidden_nodes() {
        let mut f = fixture();
        // hide issue 4 behind sensitivity label 1.
        std::sync::Arc::make_mut(&mut f.snap.node_labels).insert(4, 1);
        // a principal allowed only label 0 must not see issue 4.
        let out = evaluate(&f.snap, &f.cat, "Issue", &f.required, 0b1);
        assert!(out.iter().all(|i| i.node != 4));
        // with all labels allowed, it reappears.
        let out_all = evaluate(&f.snap, &f.cat, "Issue", &f.required, u32::MAX);
        assert!(out_all.iter().any(|i| i.node == 4));
    }

    #[test]
    fn unresolved_names_reported() {
        let f = fixture();
        let req = vec!["status".to_string(), "nope".to_string()];
        assert_eq!(
            unresolved_names("Issue", &req, &f.cat),
            vec!["nope".to_string()]
        );
        assert!(unresolved_names("Bad", &req, &f.cat).contains(&"Bad".to_string()));
        assert!(unresolved_names("Issue", &f.required, &f.cat).is_empty());
    }
}
