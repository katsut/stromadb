//! Conformance: evaluate a *declared* rule into deterministic per-subject verdicts.
//!
//! A conformance rule is a declaration over the engine's existing deterministic read primitives
//! ([`point_one`], [`point_one_asof`]) — there is no new inference and no reasoner. For every
//! subject of a given type the evaluator:
//!   1. checks an optional **scope** condition (out-of-scope subjects are `NOT_APPLICABLE`);
//!   2. walks a **required** derived path — a chain of one-cardinality hops, left→right, the last of
//!      which may be read *as-of* a valid-time anchor — to derive the required value;
//!   3. reads the **actual** value and compares.
//!
//! The whole composition is a pure function of the snapshot, so the same snapshot always yields the
//! same verdicts (sorted by subject id for stable output).
//!
//! The rule is a JSON declaration; a first cut supports one derived path plus an actual predicate and
//! equality comparison:
//!
//! ```json
//! {
//!   "subject_type": "Issue",
//!   "scope":     { "predicate": "issue-type", "equals": "release" },
//!   "required":  { "hops": [ {"predicate":"assigned-to"}, {"predicate":"member-of"},
//!                            {"predicate":"manager-of","as_of":"approved-at"} ] },
//!   "actual":    "approved-by",
//!   "absent_when": { "predicate": "status", "equals": "released" }
//! }
//! ```
//!
//! (The JSON is parsed at the DB boundary, where names are resolved against the catalog; this module
//! holds the name-based rule types and the evaluator, and resolves names via the [`Catalog`].)

use crate::catalog::Catalog;
use crate::fact::{FieldId, NodeId};
use crate::fold::{ObjKey, Snapshot};
use crate::query::{point_one, point_one_asof};

/// A value-equality condition on a subject: `point_one(subject, predicate) == equals`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cond {
    pub predicate: String,
    pub equals: ObjKey,
}

/// One hop of a required derived path. A plain hop reads the current one-cardinality value
/// (`point_one`); a hop carrying `as_of` names a predicate on the *original* subject whose integer
/// value is the valid-time instant at which this hop is read (`point_one_asof`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hop {
    pub predicate: String,
    pub as_of: Option<String>,
}

/// A declared conformance rule. Predicate/type references are names, resolved against the
/// [`Catalog`] at evaluation time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    pub subject_type: String,
    pub scope: Option<Cond>,
    pub required: Vec<Hop>,
    pub actual: String,
    pub absent_when: Option<Cond>,
}

/// The verdict outcome for one subject. A `Mismatch` carries a [`MismatchKind`] sub-classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Absent,
    Mismatch,
    NotApplicable,
}

impl Outcome {
    /// The stable wire name of this outcome.
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Ok => "OK",
            Outcome::Absent => "ABSENT",
            Outcome::Mismatch => "MISMATCH",
            Outcome::NotApplicable => "NOT_APPLICABLE",
        }
    }
}

/// Sub-classification of a [`Outcome::Mismatch`] via a valid-time history probe on the final as-of hop:
/// `Stale` = the actual value once satisfied the required derivation at an earlier valid-time (it held
/// the role before, but not as-of the anchor); `Wrong` = it never did. A mismatch on a timeless final
/// hop (no as-of) is always `Wrong`, since there is no earlier time at which it could have held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MismatchKind {
    Stale,
    Wrong,
}

impl MismatchKind {
    /// The stable wire name of this kind.
    pub fn as_str(self) -> &'static str {
        match self {
            MismatchKind::Stale => "stale",
            MismatchKind::Wrong => "wrong",
        }
    }
}

/// A per-subject verdict. `required`/`actual` are the derived and observed values (node-valued in the
/// first cut); `as_of` is the valid-time instant the as-of hop was read at, if the path used one.
/// `mismatch_kind` is `Some` only when `verdict == Mismatch`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verdict {
    pub subject: NodeId,
    pub verdict: Outcome,
    pub mismatch_kind: Option<MismatchKind>,
    pub required: Option<ObjKey>,
    pub actual: Option<ObjKey>,
    pub as_of: Option<i64>,
}

/// The names a rule references that the catalog does not know — the caller resolves this to a clear
/// error before evaluating. Empty = every name resolves.
pub fn unresolved_names(rule: &Rule, cat: &Catalog) -> Vec<String> {
    let mut names: Vec<&str> = vec![rule.subject_type.as_str(), rule.actual.as_str()];
    if let Some(s) = &rule.scope {
        names.push(&s.predicate);
    }
    if let Some(a) = &rule.absent_when {
        names.push(&a.predicate);
    }
    for h in &rule.required {
        names.push(&h.predicate);
        if let Some(anchor) = &h.as_of {
            names.push(anchor);
        }
    }
    names
        .into_iter()
        .filter(|n| cat.field_id(n).is_none())
        .map(str::to_string)
        .collect()
}

/// Evaluate `rule` over `snap`, one verdict per subject of the rule's type, sorted by subject id.
///
/// Post-authz: a subject whose ABAC label is not permitted by `principal_labels` is skipped (same
/// bit-test as the other read ops). Names are resolved against `cat`; an unknown subject-type name
/// yields no subjects, and an unknown predicate name makes its lookup yield nothing (the DB boundary
/// rejects unknown names up front via [`unresolved_names`], so this is only a defensive fallback).
pub fn evaluate(
    snap: &Snapshot,
    cat: &Catalog,
    rule: &Rule,
    principal_labels: u32,
) -> Vec<Verdict> {
    let Some(subject_ty) = cat.field_id(&rule.subject_type) else {
        return Vec::new();
    };
    let mut subjects: Vec<NodeId> = snap
        .node_types
        .iter()
        .filter(|&(_, &ty)| ty == subject_ty)
        .map(|(&n, _)| n)
        .filter(|&n| visible(snap, n, principal_labels))
        .collect();
    subjects.sort_unstable();
    subjects
        .into_iter()
        .map(|s| judge(snap, cat, rule, s))
        .collect()
}

/// Whether `node` is visible to a principal with `allowed_labels` (unlabeled = public).
fn visible(snap: &Snapshot, node: NodeId, allowed_labels: u32) -> bool {
    snap.node_labels
        .get(&node)
        .is_none_or(|&l| (allowed_labels >> l) & 1 == 1)
}

/// The verdict for a single subject `s`.
fn judge(snap: &Snapshot, cat: &Catalog, rule: &Rule, s: NodeId) -> Verdict {
    // scope: out-of-scope subjects are not judged.
    if let Some(scope) = &rule.scope
        && !cond_holds(snap, cat, s, scope)
    {
        return Verdict {
            subject: s,
            verdict: Outcome::NotApplicable,
            mismatch_kind: None,
            required: None,
            actual: None,
            as_of: None,
        };
    }

    // required = the derived path walked left→right from S; a hop with an as-of anchor reads the
    // valid-time value of that hop at the anchor's integer value on the ORIGINAL subject S.
    // `final_asof_hop` remembers the (node, predicate) of the LAST hop when it was an as-of read —
    // the site to history-probe for a stale-vs-wrong mismatch. It is `None` if the final hop was
    // timeless (a mismatch there is always `Wrong`).
    let mut cur = Some(s);
    let mut as_of: Option<i64> = None;
    let mut final_asof_hop: Option<(NodeId, FieldId)> = None;
    for hop in &rule.required {
        let Some(node) = cur else { break };
        let pid = cat.field_id(&hop.predicate);
        final_asof_hop = None;
        cur = match (&hop.as_of, pid) {
            (_, None) => None,
            (None, Some(p)) => node_of(point_one(snap, node, p)),
            (Some(anchor), Some(p)) => {
                let t = cat
                    .field_id(anchor)
                    .and_then(|ap| int_of(point_one(snap, s, ap)));
                as_of = t;
                final_asof_hop = Some((node, p));
                t.and_then(|at| node_of(point_one_asof(snap, node, p, at)))
            }
        };
    }
    let required = cur.map(ObjKey::Node);

    // actual = the observed value on S.
    let actual = cat
        .field_id(&rule.actual)
        .and_then(|p| point_one(snap, s, p));

    let outcome = match &actual {
        // absent: no actual value. If an absence condition is declared and holds, it is a gap;
        // otherwise it is not (yet) expected, so OK.
        None => {
            if rule
                .absent_when
                .as_ref()
                .is_some_and(|c| cond_holds(snap, cat, s, c))
            {
                Outcome::Absent
            } else {
                Outcome::Ok
            }
        }
        // present: match iff it equals the required value (a broken/underived path never matches).
        Some(a) => {
            if required.as_ref() == Some(a) {
                Outcome::Ok
            } else {
                Outcome::Mismatch
            }
        }
    };

    // classify a mismatch: stale if the actual value once satisfied the final as-of hop at an earlier
    // valid-time, otherwise wrong.
    let mismatch_kind = (outcome == Outcome::Mismatch).then(|| match (final_asof_hop, &actual) {
        (Some((node, p)), Some(ObjKey::Node(a))) if ever_held(snap, node, p, *a) => {
            MismatchKind::Stale
        }
        _ => MismatchKind::Wrong,
    });

    Verdict {
        subject: s,
        verdict: outcome,
        mismatch_kind,
        required,
        actual,
        as_of,
    }
}

/// Whether `(node, predicate)` ever held the value `Node(value)` at any valid-time — a scan of the
/// one-cardinality valid-time history. Used to tell a stale mismatch (once valid) from a wrong one.
fn ever_held(snap: &Snapshot, node: NodeId, predicate: FieldId, value: NodeId) -> bool {
    snap.one_history
        .get(&(node, predicate))
        .is_some_and(|rows| {
            rows.iter()
                .any(|(_, obj, _, _)| matches!(obj, Some(ObjKey::Node(n)) if *n == value))
        })
}

fn cond_holds(snap: &Snapshot, cat: &Catalog, s: NodeId, cond: &Cond) -> bool {
    cat.field_id(&cond.predicate)
        .and_then(|p| point_one(snap, s, p))
        .is_some_and(|v| v == cond.equals)
}

fn node_of(o: Option<ObjKey>) -> Option<NodeId> {
    match o {
        Some(ObjKey::Node(n)) => Some(n),
        _ => None,
    }
}

fn int_of(o: Option<ObjKey>) -> Option<i64> {
    match o {
        Some(ObjKey::Int(i)) => Some(i),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Cardinality, Range, RelProps, ValueType};
    use crate::fact::FieldId;
    use crate::fold::{Op, OrderKey, fold};
    use std::collections::BTreeMap;

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
        valid_from: i64,
    ) {
        ops.push(Op::SetOne {
            subject,
            predicate,
            object,
            valid_from,
            valid_to: None,
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

    fn rule() -> Rule {
        Rule {
            subject_type: "Issue".into(),
            scope: Some(Cond {
                predicate: "issue-type".into(),
                equals: text("release"),
            }),
            required: vec![
                Hop {
                    predicate: "assigned-to".into(),
                    as_of: None,
                },
                Hop {
                    predicate: "member-of".into(),
                    as_of: None,
                },
                Hop {
                    predicate: "manager-of".into(),
                    as_of: Some("approved-at".into()),
                },
            ],
            actual: "approved-by".into(),
            absent_when: Some(Cond {
                predicate: "status".into(),
                equals: text("released"),
            }),
        }
    }

    // An approval-conformance graph exercising every verdict + the as-of hop. A Platform department
    // (100) whose manager changes at valid-time 5000: Alice(10) → Carol(12). Non-manager Dave(20).
    // Assignees 201/202 are members of the department.
    struct Fixture {
        snap: Snapshot,
        cat: Catalog,
    }

    fn fixture() -> Fixture {
        let mut cat = Catalog::new();
        let issue = cat.register_type("Issue");
        let person = cat.register_type("Person");
        let dept = cat.register_type("Department");
        let d = RelProps::default();
        let issue_type = cat.register_predicate(
            "issue-type",
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
        let member_of =
            cat.register_predicate("member-of", Cardinality::One, d, person, Range::Type(dept));
        let manager_of =
            cat.register_predicate("manager-of", Cardinality::One, d, dept, Range::Type(person));
        let approved_by = cat.register_predicate(
            "approved-by",
            Cardinality::One,
            d,
            issue,
            Range::Type(person),
        );
        let approved_at = cat.register_predicate(
            "approved-at",
            Cardinality::One,
            d,
            issue,
            Range::Value(ValueType::Int),
        );
        let status = cat.register_predicate(
            "status",
            Cardinality::One,
            d,
            issue,
            Range::Value(ValueType::Text),
        );

        let mut ops: Vec<Op> = Vec::new();
        let mut seq = 0u64;

        set_type(&mut ops, &mut seq, 100, dept);
        set_type(&mut ops, &mut seq, 10, person);
        set_type(&mut ops, &mut seq, 12, person);
        set_type(&mut ops, &mut seq, 20, person);
        set_type(&mut ops, &mut seq, 201, person);
        set_type(&mut ops, &mut seq, 202, person);
        for id in 1..=6u64 {
            set_type(&mut ops, &mut seq, id, issue);
        }

        // dept 100 manager-of: Alice(10) from 1000, Carol(12) from 5000 (a valid-time transfer).
        set_one(&mut ops, &mut seq, 100, manager_of, ObjKey::Node(10), 1000);
        set_one(&mut ops, &mut seq, 100, manager_of, ObjKey::Node(12), 5000);
        set_one(&mut ops, &mut seq, 201, member_of, ObjKey::Node(100), 1000);
        set_one(&mut ops, &mut seq, 202, member_of, ObjKey::Node(100), 1000);

        // Issue 1 — OK: approved by the manager as-of approval time.
        set_one(&mut ops, &mut seq, 1, issue_type, text("release"), 0);
        set_one(&mut ops, &mut seq, 1, assigned_to, ObjKey::Node(201), 0);
        set_one(&mut ops, &mut seq, 1, approved_at, ObjKey::Int(1200), 0);
        set_one(&mut ops, &mut seq, 1, approved_by, ObjKey::Node(10), 0);
        set_one(&mut ops, &mut seq, 1, status, text("released"), 0);

        // Issue 2 — ABSENT: released with no approval.
        set_one(&mut ops, &mut seq, 2, issue_type, text("release"), 0);
        set_one(&mut ops, &mut seq, 2, assigned_to, ObjKey::Node(201), 0);
        set_one(&mut ops, &mut seq, 2, status, text("released"), 0);

        // Issue 3 — MISMATCH: approved by a non-manager (Dave).
        set_one(&mut ops, &mut seq, 3, issue_type, text("release"), 0);
        set_one(&mut ops, &mut seq, 3, assigned_to, ObjKey::Node(201), 0);
        set_one(&mut ops, &mut seq, 3, approved_at, ObjKey::Int(1200), 0);
        set_one(&mut ops, &mut seq, 3, approved_by, ObjKey::Node(20), 0);
        set_one(&mut ops, &mut seq, 3, status, text("released"), 0);

        // Issue 4 — NOT_APPLICABLE: not a release.
        set_one(&mut ops, &mut seq, 4, issue_type, text("task"), 0);
        set_one(&mut ops, &mut seq, 4, assigned_to, ObjKey::Node(201), 0);
        set_one(&mut ops, &mut seq, 4, status, text("released"), 0);

        // Issue 5 — OK (as-of before the transfer): Alice approved at 1200, still manager then.
        set_one(&mut ops, &mut seq, 5, issue_type, text("release"), 0);
        set_one(&mut ops, &mut seq, 5, assigned_to, ObjKey::Node(202), 0);
        set_one(&mut ops, &mut seq, 5, approved_at, ObjKey::Int(1200), 0);
        set_one(&mut ops, &mut seq, 5, approved_by, ObjKey::Node(10), 0);
        set_one(&mut ops, &mut seq, 5, status, text("released"), 0);

        // Issue 6 — MISMATCH (as-of after the transfer): Alice approved at 6000, but as-of 6000 the
        // manager is Carol(12), so Alice is stale authority.
        set_one(&mut ops, &mut seq, 6, issue_type, text("release"), 0);
        set_one(&mut ops, &mut seq, 6, assigned_to, ObjKey::Node(202), 0);
        set_one(&mut ops, &mut seq, 6, approved_at, ObjKey::Int(6000), 0);
        set_one(&mut ops, &mut seq, 6, approved_by, ObjKey::Node(10), 0);
        set_one(&mut ops, &mut seq, 6, status, text("released"), 0);

        Fixture {
            snap: fold(&ops).observe(),
            cat,
        }
    }

    fn verdicts_by_subject(vs: &[Verdict]) -> BTreeMap<NodeId, &Verdict> {
        vs.iter().map(|v| (v.subject, v)).collect()
    }

    #[test]
    fn every_verdict_and_asof_hop() {
        let f = fixture();
        let vs = evaluate(&f.snap, &f.cat, &rule(), u32::MAX);
        // deterministic, one verdict per issue, sorted by subject id
        assert_eq!(
            vs.iter().map(|v| v.subject).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6]
        );
        let by = verdicts_by_subject(&vs);
        assert_eq!(by[&1].verdict, Outcome::Ok);
        assert_eq!(by[&2].verdict, Outcome::Absent);
        assert_eq!(by[&3].verdict, Outcome::Mismatch);
        assert_eq!(by[&4].verdict, Outcome::NotApplicable);
        assert_eq!(by[&5].verdict, Outcome::Ok);
        assert_eq!(by[&6].verdict, Outcome::Mismatch);

        // Issue 1: derived required = manager-of(dept)@1200 = Alice(10); actual = Alice(10).
        assert_eq!(by[&1].required, Some(ObjKey::Node(10)));
        assert_eq!(by[&1].actual, Some(ObjKey::Node(10)));
        assert_eq!(by[&1].as_of, Some(1200));

        // Issue 2: no actual; absence condition (status == released) holds.
        assert_eq!(by[&2].actual, None);

        // Issue 4: out of scope — nothing derived.
        assert_eq!(by[&4].required, None);
        assert_eq!(by[&4].actual, None);
        assert_eq!(by[&4].as_of, None);

        // Issue 5 vs 6 = the as-of hop: same approver (Alice), different approval time flips the
        // required manager across the 5000 transfer.
        assert_eq!(by[&5].required, Some(ObjKey::Node(10)));
        assert_eq!(by[&5].as_of, Some(1200));
        assert_eq!(by[&6].required, Some(ObjKey::Node(12)));
        assert_eq!(by[&6].actual, Some(ObjKey::Node(10)));
        assert_eq!(by[&6].as_of, Some(6000));

        // mismatch sub-classification via valid-time history probe:
        //   Issue 3 = Dave(20), never a manager of the dept → wrong.
        //   Issue 6 = Alice(10), was the dept manager before the 5000 transfer but not at 6000 → stale.
        assert_eq!(by[&3].mismatch_kind, Some(MismatchKind::Wrong));
        assert_eq!(by[&6].mismatch_kind, Some(MismatchKind::Stale));
        // non-mismatch verdicts carry no kind.
        assert_eq!(by[&1].mismatch_kind, None);
        assert_eq!(by[&2].mismatch_kind, None);
        assert_eq!(by[&4].mismatch_kind, None);
    }

    #[test]
    fn post_authz_skips_hidden_subjects() {
        let mut f = fixture();
        // hide issue 3 behind sensitivity label 1.
        std::sync::Arc::make_mut(&mut f.snap.node_labels).insert(3, 1);
        // a principal allowed only label 0 must not see issue 3's verdict.
        let vs = evaluate(&f.snap, &f.cat, &rule(), 0b1);
        assert!(
            vs.iter().all(|v| v.subject != 3),
            "label-1 subject must be hidden from a label-0 principal"
        );
        // with all labels allowed, it reappears.
        let vs_all = evaluate(&f.snap, &f.cat, &rule(), u32::MAX);
        assert!(vs_all.iter().any(|v| v.subject == 3));
    }

    #[test]
    fn unknown_names_are_reported() {
        let f = fixture();
        let mut r = rule();
        r.actual = "does-not-exist".into();
        assert_eq!(
            unresolved_names(&r, &f.cat),
            vec!["does-not-exist".to_string()]
        );
        assert!(unresolved_names(&rule(), &f.cat).is_empty());
    }
}
