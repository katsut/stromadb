//! Conformance: evaluate a *declared* rule into deterministic per-subject verdicts.
//!
//! A conformance rule is a declaration over the engine's existing deterministic read primitives
//! ([`point_one`], [`point_one_asof`]) — there is no new inference and no reasoner. For every
//! subject of a given type the evaluator:
//!   1. checks an optional **scope** condition (out-of-scope subjects are `NOT_APPLICABLE`);
//!   2. walks the declared derived paths — chains of one-cardinality hops, left→right, whose last
//!      hop may be read *as-of* a valid-time anchor: **required** derives the value the actual must
//!      equal, **distinct_from** the value it must NOT equal (a rule declares either or both);
//!   3. reads the **actual** value and compares.
//!
//! Every hop but the last reaches a node; the last hop may read a literal (a name, an amount), so
//! a path derives either a node or a literal. The actual is compared with it exactly, as the two
//! stored values are keyed at ingest: equal text equals, and an int never equals a float (the same
//! as an `equals` condition). A path hop on a many-cardinality predicate, or a literal-valued hop before the last, can
//! never resolve, so [`path_errors`] reports it and the DB boundary rejects the rule.
//!
//! Every `NOT_APPLICABLE` verdict carries a [`NotApplicableReason`]: `out_of_scope` (the scope
//! failed), `no_matching_case` (a banded rule matched no case), or `required_unresolved` (the actual
//! is present but the required path resolved to no value, so equality cannot be judged). Missing
//! data on the required path is therefore never reported as a violation. Precedence: a missing
//! actual is judged `ABSENT` / `OK` as usual whatever the required path resolved to, and a resolved
//! `distinct_from` collision is still `MISMATCH` when the required path is unresolved.
//!
//! Judgment is unfiltered by fact labels. A principal's label mask is applied to the finished
//! verdict instead ([`mask_verdict`]): a verdict whose support set touches a fact the mask hides is
//! `NOT_APPLICABLE` with `hidden_by_label` and carries no values, so a verdict never rests on, or
//! leaks, a value the caller cannot read.
//!
//! The whole composition is a pure function of the snapshot, so the same snapshot always yields the
//! same verdicts (sorted by subject id for stable output).
//!
//! The rule is a JSON declaration — one equality path and/or one must-differ path plus an actual
//! predicate; condition values take the ingest object forms (`{"node": N}`, `{"int": …}`, or a bare
//! string for text):
//!
//! ```json
//! {
//!   "subject_type": "Issue",
//!   "scope":     { "predicate": "issue-type", "equals": "release" },
//!   "required":  { "hops": [ {"predicate":"assigned-to"}, {"predicate":"member-of"},
//!                            {"predicate":"manager-of","as_of":"approved-at"} ] },
//!   "distinct_from": { "hops": [ {"predicate":"assigned-to"} ] },
//!   "actual":    "approved-by",
//!   "absent_when": { "predicate": "status", "equals": "released" }
//! }
//! ```
//!
//! ## Numeric conditions and banded rules
//!
//! A condition ([`Cond`]) tests the subject's value of a one-cardinality predicate either for
//! equality or against a numeric range ([`NumRange`]): any of `gt` / `gte` / `lt` / `lte`, or
//! `between: [lo, hi]` (both ends inclusive). Combining one lower and one upper bound gives a
//! half-open band such as `{"predicate":"amount","gt":500000,"lte":2000000}`. Int and float values
//! compare numerically (two ints compare exactly; otherwise both sides compare as `f64`). A
//! condition may carry its own `as_of` anchor, in which case the value is read at the valid-time
//! instant held by that anchor predicate on the subject, exactly like an as-of hop.
//!
//! A missing value, a non-numeric value under a range test, or an unresolvable as-of anchor make
//! the condition **not satisfied** — the same treatment as an equality condition whose value is
//! missing. So a range `scope` that cannot be read yields `NOT_APPLICABLE`, and a range
//! `absent_when` that cannot be read does not turn a missing actual into `ABSENT`.
//!
//! A rule may replace `required` with ordered `cases` ([`Case`]), evaluated first-match: the first
//! case whose `when` holds (a case without `when` always holds) supplies the required path, and
//! the verdict reports the index of the matched case. When no case matches, the subject is
//! `NOT_APPLICABLE`. One stored rule thus covers a whole amount-banded table:
//!
//! ```json
//! {
//!   "subject_type": "Request",
//!   "cases": [
//!     { "when": {"predicate":"amount","lte":500000,"as_of":"approved-at"},
//!       "required": {"hops":[{"predicate":"requester"},{"predicate":"member-of"},{"predicate":"manager-of"}]} },
//!     { "when": {"predicate":"amount","gt":500000,"lte":2000000,"as_of":"approved-at"},
//!       "required": {"hops":[{"predicate":"requester"},{"predicate":"member-of"},{"predicate":"parent"},{"predicate":"manager-of"}]} },
//!     { "required": {"hops":[{"predicate":"requester"},{"predicate":"member-of"},{"predicate":"parent"},
//!                            {"predicate":"parent"},{"predicate":"manager-of"}]} }
//!   ],
//!   "actual": "approved-by"
//! }
//! ```
//!
//! Every read a condition or case selection performs is part of the verdict's support set, so a
//! revision of the amount fact re-judges exactly the subjects that read it under incremental
//! maintenance ([`crate::incremental::MaintainedConformance`]).
//!
//! (The JSON is parsed at the DB boundary, where names are resolved against the catalog; this module
//! holds the name-based rule types and the evaluator, and resolves names via the [`Catalog`].)

use std::cmp::Ordering;

use crate::catalog::{Cardinality, Catalog, Range};
use crate::fact::{FieldId, NodeId};
use crate::fold::{ObjKey, Snapshot};
use crate::mask::FactMask;
use crate::query::{point_one, point_one_asof};

/// A condition on a subject's one-cardinality value of `predicate`. With `as_of`, the value is read
/// at the valid-time instant held by the integer predicate `as_of` on the subject
/// (`point_one_asof`); otherwise the current value is read (`point_one`). A missing value never
/// satisfies the condition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cond {
    pub predicate: String,
    pub test: Test,
    pub as_of: Option<String>,
}

impl Cond {
    /// An equality condition on the current value: `point_one(subject, predicate) == value`.
    pub fn equals(predicate: impl Into<String>, value: ObjKey) -> Self {
        Cond {
            predicate: predicate.into(),
            test: Test::Equals(value),
            as_of: None,
        }
    }

    /// A numeric range condition on the current value.
    pub fn range(predicate: impl Into<String>, range: NumRange) -> Self {
        Cond {
            predicate: predicate.into(),
            test: Test::Range(range),
            as_of: None,
        }
    }

    /// This condition, read as-of the instant held by the `anchor` predicate on the subject.
    pub fn as_of(mut self, anchor: impl Into<String>) -> Self {
        self.as_of = Some(anchor.into());
        self
    }
}

/// What a [`Cond`] checks the read value against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Test {
    /// The value equals this key exactly.
    Equals(ObjKey),
    /// The value is numeric and lies within the range.
    Range(NumRange),
}

impl Test {
    fn matches(&self, v: &ObjKey) -> bool {
        match self {
            Test::Equals(e) => v == e,
            Test::Range(r) => r.contains(v),
        }
    }
}

/// A numeric range: an optional lower and an optional upper [`Bound`] (at least one is set by the
/// parser). Bound values are [`ObjKey::Int`] or [`ObjKey::Float`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NumRange {
    pub lower: Option<Bound>,
    pub upper: Option<Bound>,
}

/// One end of a [`NumRange`]; `inclusive` = the end value itself is in the range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bound {
    pub value: ObjKey,
    pub inclusive: bool,
}

impl NumRange {
    /// Whether `v` is numeric and within both bounds. A non-numeric value (text, bool, node) or an
    /// incomparable float (NaN) is outside every range.
    pub fn contains(&self, v: &ObjKey) -> bool {
        let lower_ok = self.lower.as_ref().is_none_or(|b| {
            matches!(
                (num_cmp(v, &b.value), b.inclusive),
                (Some(Ordering::Greater), _) | (Some(Ordering::Equal), true)
            )
        });
        let upper_ok = self.upper.as_ref().is_none_or(|b| {
            matches!(
                (num_cmp(v, &b.value), b.inclusive),
                (Some(Ordering::Less), _) | (Some(Ordering::Equal), true)
            )
        });
        lower_ok && upper_ok && num_of(v).is_some()
    }
}

/// Numeric comparison of two literal keys: two ints compare exactly, any other int/float pair
/// compares as `f64`. `None` if either side is non-numeric or the floats are incomparable.
fn num_cmp(a: &ObjKey, b: &ObjKey) -> Option<Ordering> {
    match (a, b) {
        (ObjKey::Int(x), ObjKey::Int(y)) => Some(x.cmp(y)),
        _ => num_of(a)?.partial_cmp(&num_of(b)?),
    }
}

fn num_of(o: &ObjKey) -> Option<f64> {
    match o {
        ObjKey::Int(i) => Some(*i as f64),
        ObjKey::Float(bits) => Some(f64::from_bits(*bits)),
        _ => None,
    }
}

/// One case of a banded rule: when `when` holds (or is absent), `required` is the derived path the
/// actual must equal. An empty `required` declares no equality expectation for that case.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Case {
    pub when: Option<Cond>,
    pub required: Vec<Hop>,
}

/// One hop of a required derived path. A plain hop reads the current one-cardinality value
/// (`point_one`); a hop carrying `as_of` names a predicate on the *original* subject whose integer
/// value is the valid-time instant at which this hop is read (`point_one_asof`). Only the last hop
/// of a path may name a literal-valued predicate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hop {
    pub predicate: String,
    pub as_of: Option<String>,
}

/// A declared conformance rule. Predicate/type references are names, resolved against the
/// [`Catalog`] at evaluation time. `required` derives the value the actual must equal;
/// `distinct_from` derives the value it must NOT equal (a self-approval ban is
/// `distinct_from: [assigned-to]`). Either may be empty, not both — an empty path declares no
/// expectation on its side. A non-empty `cases` list replaces `required` (the parser rejects a rule
/// declaring both): the first matching [`Case`] supplies the required path, and no match means
/// `NOT_APPLICABLE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    pub subject_type: String,
    pub scope: Option<Cond>,
    pub required: Vec<Hop>,
    pub cases: Vec<Case>,
    pub distinct_from: Vec<Hop>,
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

/// Sub-classification of a [`Outcome::Mismatch`] via a valid-time history probe on the final as-of hop.
/// A mismatch is only ever judged against a required value that was in effect at the anchor: `Stale`
/// = that value differs from the actual, and the actual value appears in the hop's valid-time history
/// at some other valid-time, earlier or later (it held the role before a transfer, or took it over
/// afterwards); `Wrong` = the actual value never appears in that history. When no value was in effect
/// at the anchor at all (the history starts later, or is closed over the instant) there is nothing to
/// compare against, so the verdict is `NOT_APPLICABLE` with
/// [`NotApplicableReason::RequiredUnresolved`], never `Stale`, whether or not the actual held earlier.
/// A mismatch on a timeless final hop (no as-of) is always `Wrong`, since there is no other time at
/// which it could have held.
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

/// Why a subject is `NOT_APPLICABLE`. Every `NOT_APPLICABLE` verdict carries exactly one reason, and
/// no other verdict carries one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotApplicableReason {
    /// The rule's `scope` condition does not hold for the subject.
    OutOfScope,
    /// The rule is banded and no case's `when` holds.
    NoMatchingCase,
    /// The actual is present but the required path resolves to no value (a missing hop value, a
    /// missing as-of anchor, or no value in effect at the anchor because the hop's history starts
    /// later or is closed over it), so equality cannot be judged. The row keeps its derived and
    /// observed values. A resolved `distinct_from` collision still wins as `MISMATCH`, and a missing
    /// actual is still judged `ABSENT` / `OK` as usual.
    RequiredUnresolved,
    /// Only from [`evaluate_subjects`]: the id names a visible node whose type is not the rule's
    /// `subject_type`.
    NotSubjectType,
    /// Only from [`evaluate_subjects`]: the id names no typed node, or one hidden from the principal
    /// by its label. The two are deliberately indistinguishable, so the answer does not reveal that a
    /// hidden node exists.
    UnknownSubject,
    /// The judgment read at least one fact the principal's label mask hides (a hidden row, or a
    /// predicate whose label floor is hidden), so the row is withheld: no values, no verdict that
    /// could rest on what the caller cannot see. Decided at read time over the verdict's support
    /// set; it takes precedence over every verdict and every other reason except
    /// `unknown_subject` and `not_subject_type`, which are decided from node attributes first.
    HiddenByLabel,
}

impl NotApplicableReason {
    /// Every reason, in wire order.
    pub const ALL: [NotApplicableReason; 6] = [
        NotApplicableReason::OutOfScope,
        NotApplicableReason::NoMatchingCase,
        NotApplicableReason::RequiredUnresolved,
        NotApplicableReason::NotSubjectType,
        NotApplicableReason::UnknownSubject,
        NotApplicableReason::HiddenByLabel,
    ];

    /// The stable wire name of this reason.
    pub fn as_str(self) -> &'static str {
        match self {
            NotApplicableReason::OutOfScope => "out_of_scope",
            NotApplicableReason::NoMatchingCase => "no_matching_case",
            NotApplicableReason::RequiredUnresolved => "required_unresolved",
            NotApplicableReason::NotSubjectType => "not_subject_type",
            NotApplicableReason::UnknownSubject => "unknown_subject",
            NotApplicableReason::HiddenByLabel => "hidden_by_label",
        }
    }
}

/// A per-subject verdict. `required`/`distinct`/`actual` are the derived and observed values (a
/// node, or a literal when the path's last hop reads one); `as_of` is the valid-time instant an as-of hop was read at, if a
/// path used one (the required path's anchor wins when both do).
/// `mismatch_kind` is `Some` only when `verdict == Mismatch`, and `reason` only when
/// `verdict == NotApplicable`. `case` is the index of the matched [`Case`] for a banded rule (`None`
/// for a rule without cases, or when no case matched).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verdict {
    pub subject: NodeId,
    pub verdict: Outcome,
    pub mismatch_kind: Option<MismatchKind>,
    pub reason: Option<NotApplicableReason>,
    pub required: Option<ObjKey>,
    pub distinct: Option<ObjKey>,
    pub actual: Option<ObjKey>,
    pub as_of: Option<i64>,
    pub case: Option<usize>,
}

impl Verdict {
    /// A `NOT_APPLICABLE` verdict for `subject` with `reason`, carrying no derived or observed values.
    pub fn not_applicable(subject: NodeId, reason: NotApplicableReason) -> Self {
        Verdict {
            subject,
            verdict: Outcome::NotApplicable,
            mismatch_kind: None,
            reason: Some(reason),
            required: None,
            distinct: None,
            actual: None,
            as_of: None,
            case: None,
        }
    }
}

/// The names a rule references that the catalog does not know — the caller resolves this to a clear
/// error before evaluating. Empty = every name resolves.
pub fn unresolved_names(rule: &Rule, cat: &Catalog) -> Vec<String> {
    let mut names: Vec<&str> = vec![rule.subject_type.as_str(), rule.actual.as_str()];
    let conds = rule
        .scope
        .iter()
        .chain(rule.cases.iter().filter_map(|c| c.when.as_ref()))
        .chain(&rule.absent_when);
    for c in conds {
        names.push(&c.predicate);
        if let Some(anchor) = &c.as_of {
            names.push(anchor);
        }
    }
    let case_hops = rule.cases.iter().flat_map(|c| &c.required);
    for h in rule
        .required
        .iter()
        .chain(case_hops)
        .chain(&rule.distinct_from)
    {
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

/// The derived-path hops of `rule` that can never resolve, as human-readable errors — the caller
/// rejects the rule with them before evaluating. Empty = every path is walkable. A hop must name a
/// one-cardinality predicate (a many-cardinality value is never a single expected value), and
/// every hop but the last must be node-valued (a literal has no outgoing predicates to walk on);
/// the last hop may be literal-valued. Hops whose predicate is not declared yet are left to
/// [`unresolved_names`].
pub fn path_errors(rule: &Rule, cat: &Catalog) -> Vec<String> {
    let mut paths: Vec<(String, &[Hop])> = vec![("required".into(), &rule.required)];
    for (i, c) in rule.cases.iter().enumerate() {
        paths.push((format!("cases[{i}].required"), &c.required));
    }
    paths.push(("distinct_from".into(), &rule.distinct_from));
    let mut errors = Vec::new();
    for (path, hops) in paths {
        for (i, hop) in hops.iter().enumerate() {
            let Some(def) = cat.field_id(&hop.predicate).and_then(|p| cat.predicate(p)) else {
                continue;
            };
            let name = &hop.predicate;
            if def.cardinality == Cardinality::Many {
                errors.push(format!(
                    "{path}.hops[{i}] '{name}' is many-cardinality; a derived path walks one-cardinality predicates only"
                ));
            } else if i + 1 < hops.len() && matches!(def.range, Range::Value(_)) {
                errors.push(format!(
                    "{path}.hops[{i}] '{name}' is literal-valued; only the last hop of a path may read a literal"
                ));
            }
        }
    }
    errors
}

/// Evaluate `rule` over `snap`, one verdict per subject of the rule's type, sorted by subject id.
///
/// Post-authz: a subject whose ABAC label is not permitted by `principal_labels` is skipped (same
/// bit-test as the other read ops), and a subject whose judgment read a fact hidden by the
/// principal's fact labels answers `NOT_APPLICABLE` with [`NotApplicableReason::HiddenByLabel`]
/// (see [`mask_verdict`]). Names are resolved against `cat`; an unknown subject-type name yields no
/// subjects, and an unknown predicate name makes its lookup yield nothing (the DB boundary rejects
/// unknown names up front via [`unresolved_names`], so this is only a defensive fallback).
pub fn evaluate(
    snap: &Snapshot,
    cat: &Catalog,
    rule: &Rule,
    principal_labels: u32,
) -> Vec<Verdict> {
    let Some(subject_ty) = cat.field_id(&rule.subject_type) else {
        return Vec::new();
    };
    let mask = FactMask::new(cat, principal_labels);
    let masking = mask.hides_any(snap);
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
        .map(|s| judge_masked(snap, cat, rule, s, masking.then_some(&mask)))
        .collect()
}

/// Judge `s`; under a mask that hides something, trace the judgment and withhold the row when its
/// support set touches a hidden fact.
fn judge_masked(
    snap: &Snapshot,
    cat: &Catalog,
    rule: &Rule,
    s: NodeId,
    mask: Option<&FactMask>,
) -> Verdict {
    let Some(mask) = mask else {
        return judge(snap, cat, rule, s);
    };
    let mut keys: Vec<(NodeId, FieldId)> = Vec::new();
    let v = judge_traced(snap, cat, rule, s, &mut |n, p| keys.push((n, p)));
    mask_verdict(v, &Support::of(keys, snap), mask)
}

/// What a judgment read, as far as label masking needs it: every `(node, predicate)` key, and
/// the union (one bit per label) of the access labels stored on those keys' live rows when the
/// judgment ran. Keeping the labels with the verdict lets a maintained verdict, and the old side
/// of a journaled change, be masked as of its own judgment rather than as of a later state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Support {
    pub keys: Vec<(NodeId, FieldId)>,
    pub row_labels: u32,
}

impl Support {
    /// The support of a judgment that read `keys` over `snap`.
    pub fn of(keys: Vec<(NodeId, FieldId)>, snap: &Snapshot) -> Self {
        let row_labels = keys
            .iter()
            .filter_map(|k| snap.fact_labels.get(k))
            .flat_map(|rows| rows.values())
            .filter(|&&l| l <= crate::mask::MAX_LABEL)
            .fold(0u32, |bits, &l| bits | (1 << l));
        Support { keys, row_labels }
    }
}

/// Apply a principal's fact-label mask to a verdict judged unfiltered: when its support touches a
/// fact the mask hides — a stored row label the mask does not allow, or a read predicate whose
/// label floor it does not allow — the verdict becomes `NOT_APPLICABLE` with
/// [`NotApplicableReason::HiddenByLabel`] and carries no values. Otherwise it is returned
/// unchanged. The test is conservative: a hidden row anywhere in a read key's history withholds
/// the row even if that row did not decide the read, so a verdict never rests on a fact the caller
/// cannot see. Rows already answering `unknown_subject` / `not_subject_type` (decided from node
/// attributes) are left as they are.
pub fn mask_verdict(v: Verdict, support: &Support, mask: &FactMask) -> Verdict {
    if matches!(
        v.reason,
        Some(NotApplicableReason::UnknownSubject | NotApplicableReason::NotSubjectType)
    ) {
        return v;
    }
    let hidden = support.row_labels & !mask.allowed() != 0
        || support.keys.iter().any(|k| mask.predicate_hidden(k.1));
    if hidden {
        Verdict::not_applicable(v.subject, NotApplicableReason::HiddenByLabel)
    } else {
        v
    }
}

/// [`evaluate`] restricted to the given `subjects`: exactly one row per distinct listed id, sorted
/// by id. A visible subject of the rule's type is judged exactly as [`evaluate`] judges it. Any
/// other id answers `NOT_APPLICABLE` with no values and a reason only this function produces:
/// [`NotApplicableReason::NotSubjectType`] for a visible node of another type,
/// [`NotApplicableReason::UnknownSubject`] for an id with no typed node or a node hidden by
/// `principal_labels` (a hidden node is reported as unknown whatever its type, so its existence is
/// not revealed). Cost is O(listed), not O(subjects).
pub fn evaluate_subjects(
    snap: &Snapshot,
    cat: &Catalog,
    rule: &Rule,
    principal_labels: u32,
    subjects: &[NodeId],
) -> Vec<Verdict> {
    let subject_ty = cat.field_id(&rule.subject_type);
    let mask = FactMask::new(cat, principal_labels);
    let mask = mask.hides_any(snap).then_some(mask);
    let mut picked: Vec<NodeId> = subjects.to_vec();
    picked.sort_unstable();
    picked.dedup();
    picked
        .into_iter()
        .map(|s| match snap.node_types.get(&s) {
            Some(_) if !visible(snap, s, principal_labels) => {
                Verdict::not_applicable(s, NotApplicableReason::UnknownSubject)
            }
            Some(&ty) if Some(ty) == subject_ty => judge_masked(snap, cat, rule, s, mask.as_ref()),
            Some(_) => Verdict::not_applicable(s, NotApplicableReason::NotSubjectType),
            None => Verdict::not_applicable(s, NotApplicableReason::UnknownSubject),
        })
        .collect()
}

/// Whether `node` is visible to a principal with `allowed_labels` (unlabeled = public).
fn visible(snap: &Snapshot, node: NodeId, allowed_labels: u32) -> bool {
    snap.node_labels
        .get(&node)
        .is_none_or(|&l| (allowed_labels >> l) & 1 == 1)
}

/// What a derived-path walk produced: the terminal value (if the path resolved), the anchor instant
/// an as-of hop read at, and the (node, predicate) of the LAST hop when it was an as-of read — the
/// site to history-probe for a stale-vs-wrong mismatch (`None` if the final hop was timeless).
struct Walk {
    end: Option<ObjKey>,
    as_of: Option<i64>,
    final_asof_hop: Option<(NodeId, FieldId)>,
}

/// Walk a derived path of one-cardinality hops left→right from `s`, reporting every
/// `(node, predicate)` read to `rec` — the read's support set, for incremental maintenance. A hop
/// with an as-of anchor reads the valid-time value of that hop at the anchor's integer value on
/// the ORIGINAL subject `s`. Every hop but the last must reach a node to continue from; the last
/// hop's value is the path's value as read, a node or a literal. An empty path derives nothing (no
/// expectation), same as a broken one.
fn walk(
    snap: &Snapshot,
    cat: &Catalog,
    s: NodeId,
    hops: &[Hop],
    rec: &mut impl FnMut(NodeId, FieldId),
) -> Walk {
    let mut cur = Some(s);
    let mut end: Option<ObjKey> = None;
    let mut as_of: Option<i64> = None;
    let mut final_asof_hop: Option<(NodeId, FieldId)> = None;
    for (i, hop) in hops.iter().enumerate() {
        let Some(node) = cur else { break };
        let pid = cat.field_id(&hop.predicate);
        final_asof_hop = None;
        let value = match (&hop.as_of, pid) {
            (_, None) => None,
            (None, Some(p)) => {
                rec(node, p);
                point_one(snap, node, p)
            }
            (Some(anchor), Some(p)) => {
                let t = cat.field_id(anchor).and_then(|ap| {
                    rec(s, ap);
                    int_of(point_one(snap, s, ap))
                });
                as_of = t;
                final_asof_hop = Some((node, p));
                // recorded whether or not the anchor resolved: once it does, this is the read that
                // answers (and the key ever_held probes for the stale sub-classification)
                rec(node, p);
                t.and_then(|at| point_one_asof(snap, node, p, at))
            }
        };
        if i + 1 == hops.len() {
            end = value;
        } else {
            cur = node_of(value);
        }
    }
    Walk {
        end,
        as_of,
        final_asof_hop,
    }
}

/// The verdict for a single subject `s`.
fn judge(snap: &Snapshot, cat: &Catalog, rule: &Rule, s: NodeId) -> Verdict {
    judge_traced(snap, cat, rule, s, &mut |_, _| {})
}

/// [`judge`], additionally reporting every `(node, predicate)` the judgment read to `rec` — the
/// verdict's **support set**. Any write that could change this subject's verdict must change at
/// least one reported key (reads are recorded exactly as performed: a branch the judgment did not
/// consult cannot have influenced it, and the write that makes it consultable touches a recorded
/// key first). This is what keyed-incremental maintenance re-judges on.
pub(crate) fn judge_traced(
    snap: &Snapshot,
    cat: &Catalog,
    rule: &Rule,
    s: NodeId,
    rec: &mut impl FnMut(NodeId, FieldId),
) -> Verdict {
    // scope: out-of-scope subjects are not judged.
    if let Some(scope) = &rule.scope
        && !cond_holds_traced(snap, cat, s, scope, rec)
    {
        return Verdict::not_applicable(s, NotApplicableReason::OutOfScope);
    }

    // A banded rule selects its required path by the first matching case; only the conditions
    // actually consulted (up to and including the match) are read, and so recorded.
    let (required_hops, case): (&[Hop], Option<usize>) = if rule.cases.is_empty() {
        (&rule.required, None)
    } else {
        let mut matched = None;
        for (i, c) in rule.cases.iter().enumerate() {
            if c.when
                .as_ref()
                .is_none_or(|w| cond_holds_traced(snap, cat, s, w, rec))
            {
                matched = Some(i);
                break;
            }
        }
        match matched {
            Some(i) => (&rule.cases[i].required, Some(i)),
            None => return Verdict::not_applicable(s, NotApplicableReason::NoMatchingCase),
        }
    };

    // The two derived paths: `required` = the value the actual must equal, `distinct_from` = the
    // value it must NOT equal. When both carry an as-of anchor, the required path's instant is the
    // one reported.
    let req = walk(snap, cat, s, required_hops, rec);
    let dis = walk(snap, cat, s, &rule.distinct_from, rec);
    let required = req.end;
    let distinct = dis.end;
    let as_of = req.as_of.or(dis.as_of);

    // actual = the observed value on S.
    let actual = cat.field_id(&rule.actual).and_then(|p| {
        rec(s, p);
        point_one(snap, s, p)
    });

    // Precedence (D31): a missing actual is judged ABSENT / OK whatever the required path resolved
    // to, since absence does not depend on the expected value. A present actual is first checked
    // for an equality mismatch against a RESOLVED required value; when the required path resolved
    // to nothing, equality cannot be judged, so a resolved must-differ collision is still a
    // MISMATCH and anything else is NOT_APPLICABLE with `required_unresolved`.
    let required_unresolved = !required_hops.is_empty() && required.is_none();
    let (outcome, mismatch_kind, reason) = match &actual {
        // absent: no actual value. If an absence condition is declared and holds, it is a gap;
        // otherwise it is not (yet) expected, so OK.
        None => {
            if rule
                .absent_when
                .as_ref()
                .is_some_and(|c| cond_holds_traced(snap, cat, s, c, rec))
            {
                (Outcome::Absent, None, None)
            } else {
                (Outcome::Ok, None, None)
            }
        }
        // present but the expected value is unknown: only a must-differ collision is decidable
        // (an underived must-differ path can never collide).
        Some(a) if required_unresolved => {
            if distinct.as_ref() == Some(a) {
                (Outcome::Mismatch, Some(MismatchKind::Wrong), None)
            } else {
                (
                    Outcome::NotApplicable,
                    None,
                    Some(NotApplicableReason::RequiredUnresolved),
                )
            }
        }
        // present: a declared equality expectation must match, and a must-differ expectation must
        // not collide (an underived must-differ path can never collide).
        Some(a) => {
            if !required_hops.is_empty() && required.as_ref() != Some(a) {
                // classify: stale if the actual value once satisfied the final as-of hop at an
                // earlier valid-time, otherwise wrong.
                let kind = match req.final_asof_hop {
                    Some((node, p)) if ever_held(snap, node, p, a) => MismatchKind::Stale,
                    _ => MismatchKind::Wrong,
                };
                (Outcome::Mismatch, Some(kind), None)
            } else if distinct.as_ref() == Some(a) {
                // a must-differ collision holds *now* — there is nothing stale about it.
                (Outcome::Mismatch, Some(MismatchKind::Wrong), None)
            } else {
                (Outcome::Ok, None, None)
            }
        }
    };

    Verdict {
        subject: s,
        verdict: outcome,
        mismatch_kind,
        reason,
        required,
        distinct,
        actual,
        as_of,
        case,
    }
}

/// Whether `(node, predicate)` ever held `value` (a node or a literal, compared exactly) at any
/// valid-time — a scan of the one-cardinality valid-time history. Used to tell a stale mismatch
/// (once valid) from a wrong one.
fn ever_held(snap: &Snapshot, node: NodeId, predicate: FieldId, value: &ObjKey) -> bool {
    snap.one_history
        .get(&(node, predicate))
        .is_some_and(|rows| {
            rows.iter()
                .any(|(_, obj, _, _)| obj.as_ref() == Some(value))
        })
}

/// Whether `cond` holds on subject `s`, reporting its reads to `rec`. An as-of condition reads the
/// anchor on `s` first, then the predicate's value in effect at that instant; the predicate key is
/// recorded even when the anchor is missing, so the write that supplies the anchor and any later
/// revision of the value both re-judge the subject.
fn cond_holds_traced(
    snap: &Snapshot,
    cat: &Catalog,
    s: NodeId,
    cond: &Cond,
    rec: &mut impl FnMut(NodeId, FieldId),
) -> bool {
    let Some(p) = cat.field_id(&cond.predicate) else {
        return false;
    };
    let value = match &cond.as_of {
        None => {
            rec(s, p);
            point_one(snap, s, p)
        }
        Some(anchor) => {
            let t = cat.field_id(anchor).and_then(|ap| {
                rec(s, ap);
                int_of(point_one(snap, s, ap))
            });
            rec(s, p);
            t.and_then(|at| point_one_asof(snap, s, p, at))
        }
    };
    value.is_some_and(|v| cond.test.matches(&v))
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
            scope: Some(Cond::equals("issue-type", text("release"))),
            cases: Vec::new(),
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
            distinct_from: Vec::new(),
            actual: "approved-by".into(),
            absent_when: Some(Cond::equals("status", text("released"))),
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
    fn evaluate_subjects_matches_the_full_evaluation_for_those_subjects() {
        let mut f = fixture();
        let full = evaluate(&f.snap, &f.cat, &rule(), u32::MAX);
        // unsorted, duplicated, a non-subject (person 10) and an unknown id (999)
        let some = evaluate_subjects(&f.snap, &f.cat, &rule(), u32::MAX, &[6, 3, 6, 10, 999]);
        // one row per distinct id, sorted; judged rows equal the full evaluation's
        assert_eq!(
            some.iter().map(|r| r.subject).collect::<Vec<_>>(),
            vec![3, 6, 10, 999]
        );
        let expect: Vec<Verdict> = full
            .iter()
            .filter(|v| v.subject == 3 || v.subject == 6)
            .cloned()
            .collect();
        assert_eq!(some[..2], expect[..]);
        // a node of another type and an unknown id answer NOT_APPLICABLE with their reason
        use NotApplicableReason::{NotSubjectType, OutOfScope, UnknownSubject};
        assert_eq!(some[2], Verdict::not_applicable(10, NotSubjectType));
        assert_eq!(some[3], Verdict::not_applicable(999, UnknownSubject));
        // an out-of-scope subject is judged, with its own reason
        let scoped = evaluate_subjects(&f.snap, &f.cat, &rule(), u32::MAX, &[4]);
        assert_eq!(scoped, vec![Verdict::not_applicable(4, OutOfScope)]);
        // a hidden node reads as unknown, whether or not it is of the subject type
        std::sync::Arc::make_mut(&mut f.snap.node_labels).insert(3, 1);
        std::sync::Arc::make_mut(&mut f.snap.node_labels).insert(10, 1);
        let masked = evaluate_subjects(&f.snap, &f.cat, &rule(), 0b1, &[3, 6, 10]);
        let reasons: Vec<_> = masked.iter().map(|r| (r.subject, r.reason)).collect();
        assert_eq!(
            reasons,
            vec![
                (3, Some(UnknownSubject)),
                (6, None),
                (10, Some(UnknownSubject))
            ]
        );
        assert_eq!(masked[0], Verdict::not_applicable(3, UnknownSubject));
    }

    #[test]
    fn reason_is_set_exactly_on_not_applicable_verdicts() {
        let f = fixture();
        let vs = evaluate(&f.snap, &f.cat, &rule(), u32::MAX);
        for v in &vs {
            assert_eq!(
                v.reason.is_some(),
                v.verdict == Outcome::NotApplicable,
                "{v:?}"
            );
        }
        assert_eq!(
            verdicts_by_subject(&vs)[&4].reason,
            Some(NotApplicableReason::OutOfScope)
        );
        let b = banded_fixture();
        let vs = evaluate(
            &b.snap,
            &b.cat,
            &banded_rule(Some("approved-at"), false),
            u32::MAX,
        );
        assert_eq!(
            verdicts_by_subject(&vs)[&7].reason,
            Some(NotApplicableReason::NoMatchingCase)
        );
    }

    // The fixture graph plus issues whose required path cannot be resolved: 7 is assigned to a
    // person with no department (Eve, 30), 8 has no approval instant to anchor the as-of hop, 9 is
    // self-approved by that department-less assignee, 10 is released with no approval, 11 is open
    // with no approval, and 12 approved by Alice whose stale authority is only visible through the
    // department Eve does not have.
    fn unresolved_fixture() -> Fixture {
        let cat = fixture().cat;
        let id = |c: &Catalog, n: &str| c.field_id(n).unwrap();
        let (issue, person) = (id(&cat, "Issue"), id(&cat, "Person"));
        let assigned_to = id(&cat, "assigned-to");
        let approved_by = id(&cat, "approved-by");
        let approved_at = id(&cat, "approved-at");
        let issue_type = id(&cat, "issue-type");
        let status = id(&cat, "status");
        let member_of = id(&cat, "member-of");
        let manager_of = id(&cat, "manager-of");
        let mut ops: Vec<Op> = Vec::new();
        let mut seq = 0u64;
        set_type(&mut ops, &mut seq, 100, id(&cat, "Department"));
        for p in [10, 12, 30, 201] {
            set_type(&mut ops, &mut seq, p, person);
        }
        set_one(&mut ops, &mut seq, 100, manager_of, ObjKey::Node(10), 1000);
        set_one(&mut ops, &mut seq, 100, manager_of, ObjKey::Node(12), 5000);
        set_one(&mut ops, &mut seq, 201, member_of, ObjKey::Node(100), 1000);
        // (issue, assignee, approver, approved-at, status)
        type Row<'a> = (NodeId, NodeId, Option<NodeId>, Option<i64>, &'a str);
        let rows: [Row; 6] = [
            (7, 30, Some(10), Some(1200), "released"),
            (8, 201, Some(10), None, "released"),
            (9, 30, Some(30), Some(1200), "released"),
            (10, 30, None, Some(1200), "released"),
            (11, 30, None, None, "open"),
            (12, 30, Some(10), Some(6000), "released"),
        ];
        for (i, who, approver, at, st) in rows {
            set_type(&mut ops, &mut seq, i, issue);
            set_one(&mut ops, &mut seq, i, issue_type, text("release"), 0);
            set_one(&mut ops, &mut seq, i, assigned_to, ObjKey::Node(who), 0);
            set_one(&mut ops, &mut seq, i, status, text(st), 0);
            if let Some(a) = approver {
                set_one(&mut ops, &mut seq, i, approved_by, ObjKey::Node(a), 0);
            }
            if let Some(t) = at {
                set_one(&mut ops, &mut seq, i, approved_at, ObjKey::Int(t), 0);
            }
        }
        Fixture {
            snap: fold(&ops).observe(),
            cat,
        }
    }

    #[test]
    fn an_unresolved_required_path_is_not_a_mismatch() {
        let f = unresolved_fixture();
        let mut r = rule();
        r.distinct_from = hops(&["assigned-to"]);
        let vs = evaluate(&f.snap, &f.cat, &r, u32::MAX);
        let by = verdicts_by_subject(&vs);
        let unresolved = Some(NotApplicableReason::RequiredUnresolved);
        // actual present, the department hop is missing: not judged, values kept for diagnosis
        assert_eq!(by[&7].verdict, Outcome::NotApplicable);
        assert_eq!(by[&7].reason, unresolved);
        assert_eq!(by[&7].mismatch_kind, None);
        assert_eq!(by[&7].required, None);
        assert_eq!(by[&7].actual, Some(ObjKey::Node(10)));
        assert_eq!(by[&7].distinct, Some(ObjKey::Node(30)));
        assert_eq!(by[&7].as_of, None);
        // actual present, the as-of anchor is missing: same outcome
        assert_eq!(
            (by[&8].verdict, by[&8].reason),
            (Outcome::NotApplicable, unresolved)
        );
        assert_eq!(by[&8].actual, Some(ObjKey::Node(10)));
        // a resolved must-differ collision is decidable without the required value
        assert_eq!(by[&9].verdict, Outcome::Mismatch);
        assert_eq!(by[&9].mismatch_kind, Some(MismatchKind::Wrong));
        assert_eq!(by[&9].reason, None);
        // a missing actual is judged as before: ABSENT when absent_when holds, OK otherwise
        assert_eq!((by[&10].verdict, by[&10].reason), (Outcome::Absent, None));
        assert_eq!((by[&11].verdict, by[&11].reason), (Outcome::Ok, None));
        // no stale/wrong classification without a resolved path
        assert_eq!(
            (by[&12].verdict, by[&12].reason),
            (Outcome::NotApplicable, unresolved)
        );
        // without distinct_from, the self-approval is unresolved like any other present actual
        let vs = evaluate(&f.snap, &f.cat, &rule(), u32::MAX);
        assert_eq!(verdicts_by_subject(&vs)[&9].reason, unresolved);
    }

    // Dept 100's `manager-of` timeline: nothing before 1000, Alice(10) over [1000, 3000), closed over
    // [3000, 5000), Carol(12) from 5000. Assignee 201 is a member. Five approvals by Alice, Carol or
    // the non-manager Dave(20) at instants chosen to fall in each segment.
    fn coverage_fixture() -> Fixture {
        let cat = fixture().cat;
        let id = |c: &Catalog, n: &str| c.field_id(n).unwrap();
        let (issue, person) = (id(&cat, "Issue"), id(&cat, "Person"));
        let assigned_to = id(&cat, "assigned-to");
        let approved_by = id(&cat, "approved-by");
        let approved_at = id(&cat, "approved-at");
        let issue_type = id(&cat, "issue-type");
        let status = id(&cat, "status");
        let member_of = id(&cat, "member-of");
        let manager_of = id(&cat, "manager-of");
        let mut ops: Vec<Op> = Vec::new();
        let mut seq = 0u64;
        set_type(&mut ops, &mut seq, 100, id(&cat, "Department"));
        for p in [10, 12, 20, 201] {
            set_type(&mut ops, &mut seq, p, person);
        }
        set_one(&mut ops, &mut seq, 100, manager_of, ObjKey::Node(10), 1000);
        ops.push(Op::CloseOne {
            subject: 100,
            predicate: manager_of,
            valid_from: 3000,
            ok: ok(seq),
        });
        seq += 1;
        set_one(&mut ops, &mut seq, 100, manager_of, ObjKey::Node(12), 5000);
        set_one(&mut ops, &mut seq, 201, member_of, ObjKey::Node(100), 0);
        // (issue, approver, approved-at)
        let rows: [(NodeId, NodeId, i64); 5] = [
            (21, 10, 500),  // before the history starts
            (22, 10, 4000), // inside the closed gap: Alice held earlier, nobody holds now
            (23, 10, 6000), // Carol holds; Alice held earlier
            (24, 20, 6000), // Carol holds; Dave never held
            (25, 12, 1200), // Alice holds; Carol holds later
        ];
        for (i, approver, at) in rows {
            set_type(&mut ops, &mut seq, i, issue);
            set_one(&mut ops, &mut seq, i, issue_type, text("release"), 0);
            set_one(&mut ops, &mut seq, i, assigned_to, ObjKey::Node(201), 0);
            set_one(&mut ops, &mut seq, i, status, text("released"), 0);
            set_one(
                &mut ops,
                &mut seq,
                i,
                approved_by,
                ObjKey::Node(approver),
                0,
            );
            set_one(&mut ops, &mut seq, i, approved_at, ObjKey::Int(at), 0);
        }
        Fixture {
            snap: fold(&ops).observe(),
            cat,
        }
    }

    #[test]
    fn no_value_at_as_of_is_unresolved_not_stale() {
        let f = coverage_fixture();
        let vs = evaluate(&f.snap, &f.cat, &rule(), u32::MAX);
        let by = verdicts_by_subject(&vs);
        let unresolved = Some(NotApplicableReason::RequiredUnresolved);
        // the history starts after as_of: no value was in effect, so nothing to compare against.
        // The row keeps the anchor instant and the actual; it carries no mismatch kind.
        assert_eq!(by[&21].verdict, Outcome::NotApplicable);
        assert_eq!(by[&21].reason, unresolved);
        assert_eq!(by[&21].mismatch_kind, None);
        assert_eq!(by[&21].required, None);
        assert_eq!(by[&21].actual, Some(ObjKey::Node(10)));
        assert_eq!(by[&21].as_of, Some(500));
        // the value is closed over as_of: the actual did hold earlier, but with no value in effect
        // at the instant this is still unresolved, not stale
        assert_eq!(by[&22].verdict, Outcome::NotApplicable);
        assert_eq!(by[&22].reason, unresolved);
        assert_eq!(by[&22].mismatch_kind, None);
        assert_eq!(by[&22].as_of, Some(4000));
        // a different value is in effect at as_of: stale when the actual held at some other
        // valid-time (earlier or later), wrong when it never did
        assert_eq!(by[&23].verdict, Outcome::Mismatch);
        assert_eq!(by[&23].mismatch_kind, Some(MismatchKind::Stale));
        assert_eq!(by[&23].required, Some(ObjKey::Node(12)));
        assert_eq!(by[&24].verdict, Outcome::Mismatch);
        assert_eq!(by[&24].mismatch_kind, Some(MismatchKind::Wrong));
        assert_eq!(by[&25].verdict, Outcome::Mismatch);
        assert_eq!(by[&25].mismatch_kind, Some(MismatchKind::Stale));
        assert_eq!(by[&25].required, Some(ObjKey::Node(10)));
    }

    #[test]
    fn an_unresolved_band_path_is_not_a_mismatch() {
        let f = banded_fixture();
        let mut r = banded_rule(Some("approved-at"), true);
        // the top band walks one `parent` further than the org chart goes
        r.cases[2].required = hops(&[
            "requester",
            "member-of",
            "parent",
            "parent",
            "parent",
            "manager-of",
        ]);
        let vs = evaluate(&f.snap, &f.cat, &r, u32::MAX);
        let by = verdicts_by_subject(&vs);
        assert_eq!(by[&4].verdict, Outcome::NotApplicable);
        assert_eq!(by[&4].reason, Some(NotApplicableReason::RequiredUnresolved));
        assert_eq!(by[&4].case, Some(2));
        assert_eq!(by[&1].verdict, Outcome::Ok);
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
        // distinct_from names are checked too.
        let mut r = rule();
        r.distinct_from = vec![Hop {
            predicate: "also-missing".into(),
            as_of: None,
        }];
        assert_eq!(
            unresolved_names(&r, &f.cat),
            vec!["also-missing".to_string()]
        );
    }

    // A flat workspace with no org chart: person 11 assigned to issues 1/2 (1 self-approved,
    // 2 peer-approved by 12), issue 3 done with no approval at all.
    fn flat_fixture() -> Fixture {
        let mut cat = Catalog::new();
        let issue = cat.register_type("Issue");
        let person = cat.register_type("Person");
        let d = RelProps::default();
        let assigned_to = cat.register_predicate(
            "assigned-to",
            Cardinality::One,
            d,
            issue,
            Range::Type(person),
        );
        let approved_by = cat.register_predicate(
            "approved-by",
            Cardinality::One,
            d,
            issue,
            Range::Type(person),
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
        set_type(&mut ops, &mut seq, 11, person);
        set_type(&mut ops, &mut seq, 12, person);
        for id in 1..=3u64 {
            set_type(&mut ops, &mut seq, id, issue);
        }
        set_one(&mut ops, &mut seq, 1, assigned_to, ObjKey::Node(11), 0);
        set_one(&mut ops, &mut seq, 1, approved_by, ObjKey::Node(11), 0);
        set_one(&mut ops, &mut seq, 1, status, text("done"), 0);
        set_one(&mut ops, &mut seq, 2, assigned_to, ObjKey::Node(11), 0);
        set_one(&mut ops, &mut seq, 2, approved_by, ObjKey::Node(12), 0);
        set_one(&mut ops, &mut seq, 2, status, text("done"), 0);
        set_one(&mut ops, &mut seq, 3, assigned_to, ObjKey::Node(12), 0);
        set_one(&mut ops, &mut seq, 3, status, text("done"), 0);
        Fixture {
            snap: fold(&ops).observe(),
            cat,
        }
    }

    #[test]
    fn distinct_from_bans_self_approval() {
        let f = flat_fixture();
        // no equality expectation at all — the only declaration is "approved by someone OTHER
        // than the assignee".
        let r = Rule {
            subject_type: "Issue".into(),
            scope: None,
            required: Vec::new(),
            cases: Vec::new(),
            distinct_from: vec![Hop {
                predicate: "assigned-to".into(),
                as_of: None,
            }],
            actual: "approved-by".into(),
            absent_when: Some(Cond::equals("status", text("done"))),
        };
        let vs = evaluate(&f.snap, &f.cat, &r, u32::MAX);
        let by = verdicts_by_subject(&vs);
        // 1 = self-approved → the must-differ collision; nothing stale about a value that holds now.
        assert_eq!(by[&1].verdict, Outcome::Mismatch);
        assert_eq!(by[&1].mismatch_kind, Some(MismatchKind::Wrong));
        assert_eq!(by[&1].distinct, Some(ObjKey::Node(11)));
        assert_eq!(by[&1].required, None); // no equality expectation was declared
        // 2 = peer-approved → fine; 3 = done with no approval → the absence gap still fires.
        assert_eq!(by[&2].verdict, Outcome::Ok);
        assert_eq!(by[&3].verdict, Outcome::Absent);
    }

    #[test]
    fn required_and_distinct_compose() {
        let f = flat_fixture();
        // a deliberately contradictory rule — actual must equal AND differ from the assignee — to
        // pin the precedence: the equality side is judged first, then the must-differ side.
        let r = Rule {
            subject_type: "Issue".into(),
            scope: None,
            required: vec![Hop {
                predicate: "assigned-to".into(),
                as_of: None,
            }],
            cases: Vec::new(),
            distinct_from: vec![Hop {
                predicate: "assigned-to".into(),
                as_of: None,
            }],
            actual: "approved-by".into(),
            absent_when: None,
        };
        let vs = evaluate(&f.snap, &f.cat, &r, u32::MAX);
        let by = verdicts_by_subject(&vs);
        // 1: equality holds (self-approved), so the must-differ collision is the violation.
        assert_eq!(by[&1].verdict, Outcome::Mismatch);
        assert_eq!(by[&1].mismatch_kind, Some(MismatchKind::Wrong));
        assert_eq!(by[&1].required, Some(ObjKey::Node(11)));
        assert_eq!(by[&1].distinct, Some(ObjKey::Node(11)));
        // 2: the equality side fails first (approver 12 ≠ assignee 11).
        assert_eq!(by[&2].verdict, Outcome::Mismatch);
        assert_eq!(by[&2].mismatch_kind, Some(MismatchKind::Wrong));
    }

    #[test]
    fn node_valued_scope_conditions() {
        let f = fixture();
        // scope on a node-valued predicate: only issues assigned to 201 are judged.
        let mut r = rule();
        r.scope = Some(Cond::equals("assigned-to", ObjKey::Node(201)));
        let vs = evaluate(&f.snap, &f.cat, &r, u32::MAX);
        let by = verdicts_by_subject(&vs);
        assert_eq!(by[&1].verdict, Outcome::Ok);
        assert_eq!(by[&5].verdict, Outcome::NotApplicable);
        assert_eq!(by[&6].verdict, Outcome::NotApplicable);
    }

    fn bound(value: ObjKey, inclusive: bool) -> Option<Bound> {
        Some(Bound { value, inclusive })
    }

    fn float(f: f64) -> ObjKey {
        ObjKey::Float(f.to_bits())
    }

    #[test]
    fn numeric_ranges_compare_ints_and_floats() {
        // (500000, 2000000]
        let band = NumRange {
            lower: bound(ObjKey::Int(500_000), false),
            upper: bound(ObjKey::Int(2_000_000), true),
        };
        assert!(!band.contains(&ObjKey::Int(500_000)));
        assert!(band.contains(&ObjKey::Int(500_001)));
        assert!(band.contains(&ObjKey::Int(2_000_000)));
        assert!(!band.contains(&ObjKey::Int(2_000_001)));
        // floats compare against int bounds numerically, and vice versa
        assert!(band.contains(&float(500_000.5)));
        assert!(!band.contains(&float(500_000.0)));
        let half_open = NumRange {
            lower: bound(float(0.5), true),
            upper: None,
        };
        assert!(half_open.contains(&ObjKey::Int(1)));
        assert!(half_open.contains(&float(0.5)));
        assert!(!half_open.contains(&ObjKey::Int(0)));
        // two ints compare exactly, beyond f64 integer precision
        let big = NumRange {
            lower: bound(ObjKey::Int(i64::MAX - 1), false),
            upper: None,
        };
        assert!(big.contains(&ObjKey::Int(i64::MAX)));
        assert!(!big.contains(&ObjKey::Int(i64::MAX - 1)));
        // non-numeric and incomparable values are outside every range
        assert!(!half_open.contains(&text("1")));
        assert!(!half_open.contains(&ObjKey::Bool(true)));
        assert!(!half_open.contains(&ObjKey::Node(7)));
        assert!(!half_open.contains(&float(f64::NAN)));
        let unbounded = NumRange {
            lower: None,
            upper: None,
        };
        assert!(unbounded.contains(&ObjKey::Int(3)));
        assert!(!unbounded.contains(&text("3")));
    }

    fn hops(names: &[&str]) -> Vec<Hop> {
        names
            .iter()
            .map(|n| Hop {
                predicate: (*n).into(),
                as_of: None,
            })
            .collect()
    }

    // An amount-banded approval table. Team 100 (manager 10) sits in division 101 (manager 11),
    // which sits in company 102 (manager 12); requester 20 is a member of team 100. Up to 500,000
    // the team manager approves, up to 2,000,000 the division manager, above that the company's.
    fn banded_fixture() -> Fixture {
        let mut cat = Catalog::new();
        let request = cat.register_type("Request");
        let person = cat.register_type("Person");
        let dept = cat.register_type("Department");
        let d = RelProps::default();
        let requester = cat.register_predicate(
            "requester",
            Cardinality::One,
            d,
            request,
            Range::Type(person),
        );
        let member_of =
            cat.register_predicate("member-of", Cardinality::One, d, person, Range::Type(dept));
        let parent = cat.register_predicate("parent", Cardinality::One, d, dept, Range::Type(dept));
        let manager_of =
            cat.register_predicate("manager-of", Cardinality::One, d, dept, Range::Type(person));
        let approved_by = cat.register_predicate(
            "approved-by",
            Cardinality::One,
            d,
            request,
            Range::Type(person),
        );
        let approved_at = cat.register_predicate(
            "approved-at",
            Cardinality::One,
            d,
            request,
            Range::Value(ValueType::Int),
        );
        let amount = cat.register_predicate(
            "amount",
            Cardinality::One,
            d,
            request,
            Range::Value(ValueType::Int),
        );
        let mut ops: Vec<Op> = Vec::new();
        let mut seq = 0u64;
        for id in [100, 101, 102] {
            set_type(&mut ops, &mut seq, id, dept);
        }
        for id in [10, 11, 12, 20] {
            set_type(&mut ops, &mut seq, id, person);
        }
        for id in 1..=7u64 {
            set_type(&mut ops, &mut seq, id, request);
        }
        set_one(&mut ops, &mut seq, 100, parent, ObjKey::Node(101), 0);
        set_one(&mut ops, &mut seq, 101, parent, ObjKey::Node(102), 0);
        set_one(&mut ops, &mut seq, 100, manager_of, ObjKey::Node(10), 0);
        set_one(&mut ops, &mut seq, 101, manager_of, ObjKey::Node(11), 0);
        set_one(&mut ops, &mut seq, 102, manager_of, ObjKey::Node(12), 0);
        set_one(&mut ops, &mut seq, 20, member_of, ObjKey::Node(100), 0);
        // (request, amount, approver); every request approved at valid-time 1200
        let rows: [(NodeId, Option<i64>, NodeId); 7] = [
            (1, Some(300_000), 10),   // band 0, team manager → OK
            (2, Some(1_500_000), 10), // band 1, team manager → MISMATCH
            (3, Some(1_500_000), 11), // band 1, division manager → OK
            (4, Some(5_000_000), 12), // band 2, company manager → OK
            (5, Some(5_000_000), 11), // band 2, division manager → MISMATCH
            (6, Some(400_000), 10),   // band 0 at approval; revised into band 1 below
            (7, None, 10),            // no amount at all
        ];
        for (r, amt, approver) in rows {
            set_one(&mut ops, &mut seq, r, requester, ObjKey::Node(20), 0);
            set_one(&mut ops, &mut seq, r, approved_at, ObjKey::Int(1200), 0);
            set_one(
                &mut ops,
                &mut seq,
                r,
                approved_by,
                ObjKey::Node(approver),
                0,
            );
            if let Some(a) = amt {
                set_one(&mut ops, &mut seq, r, amount, ObjKey::Int(a), 0);
            }
        }
        // request 6's amount is raised above the first band AFTER the approval instant
        set_one(&mut ops, &mut seq, 6, amount, ObjKey::Int(1_500_000), 2000);
        Fixture {
            snap: fold(&ops).observe(),
            cat,
        }
    }

    /// The three-band table; `anchor` = read the amount as-of that predicate's instant.
    fn banded_rule(anchor: Option<&str>, catch_all: bool) -> Rule {
        let at = |c: Cond| match anchor {
            Some(a) => c.as_of(a),
            None => c,
        };
        let mut cases = vec![
            Case {
                when: Some(at(Cond::range(
                    "amount",
                    NumRange {
                        lower: None,
                        upper: bound(ObjKey::Int(500_000), true),
                    },
                ))),
                required: hops(&["requester", "member-of", "manager-of"]),
            },
            Case {
                when: Some(at(Cond::range(
                    "amount",
                    NumRange {
                        lower: bound(ObjKey::Int(500_000), false),
                        upper: bound(ObjKey::Int(2_000_000), true),
                    },
                ))),
                required: hops(&["requester", "member-of", "parent", "manager-of"]),
            },
        ];
        let top = hops(&["requester", "member-of", "parent", "parent", "manager-of"]);
        cases.push(Case {
            when: if catch_all {
                None
            } else {
                Some(at(Cond::range(
                    "amount",
                    NumRange {
                        lower: bound(ObjKey::Int(2_000_000), false),
                        upper: None,
                    },
                )))
            },
            required: top,
        });
        Rule {
            subject_type: "Request".into(),
            scope: None,
            required: Vec::new(),
            cases,
            distinct_from: Vec::new(),
            actual: "approved-by".into(),
            absent_when: None,
        }
    }

    #[test]
    fn banded_rule_selects_the_required_path_per_amount() {
        let f = banded_fixture();
        let vs = evaluate(
            &f.snap,
            &f.cat,
            &banded_rule(Some("approved-at"), false),
            u32::MAX,
        );
        let by = verdicts_by_subject(&vs);
        let got = |s: NodeId| (by[&s].verdict, by[&s].case, by[&s].required.clone());
        assert_eq!(got(1), (Outcome::Ok, Some(0), Some(ObjKey::Node(10))));
        assert_eq!(got(2), (Outcome::Mismatch, Some(1), Some(ObjKey::Node(11))));
        assert_eq!(got(3), (Outcome::Ok, Some(1), Some(ObjKey::Node(11))));
        assert_eq!(got(4), (Outcome::Ok, Some(2), Some(ObjKey::Node(12))));
        assert_eq!(got(5), (Outcome::Mismatch, Some(2), Some(ObjKey::Node(12))));
        // the amount in effect at the approval instant (1200) is 400,000: band 0, approved correctly
        assert_eq!(got(6), (Outcome::Ok, Some(0), Some(ObjKey::Node(10))));
        // no amount: no band holds, so the subject is out of every case
        assert_eq!(got(7), (Outcome::NotApplicable, None, None));
        assert_eq!(by[&7].actual, None);
    }

    #[test]
    fn banded_rule_reads_the_current_amount_without_an_anchor() {
        let f = banded_fixture();
        let vs = evaluate(&f.snap, &f.cat, &banded_rule(None, false), u32::MAX);
        let by = verdicts_by_subject(&vs);
        // request 6's current amount (1,500,000) is band 1, so the team manager's approval is short
        assert_eq!(by[&6].verdict, Outcome::Mismatch);
        assert_eq!(by[&6].case, Some(1));
        assert_eq!(by[&6].required, Some(ObjKey::Node(11)));
    }

    #[test]
    fn a_case_without_when_catches_the_rest() {
        let f = banded_fixture();
        let vs = evaluate(
            &f.snap,
            &f.cat,
            &banded_rule(Some("approved-at"), true),
            u32::MAX,
        );
        let by = verdicts_by_subject(&vs);
        // the missing amount satisfies neither band, so the catch-all applies
        assert_eq!(by[&7].verdict, Outcome::Mismatch);
        assert_eq!(by[&7].case, Some(2));
        assert_eq!(by[&4].case, Some(2));
        assert_eq!(by[&1].case, Some(0));
    }

    #[test]
    fn range_conditions_in_scope_and_absent_when() {
        let f = banded_fixture();
        // scope: only requests above 1,000,000 (current value) are judged
        let mut r = banded_rule(None, true);
        r.cases.clear();
        r.required = hops(&["requester", "member-of", "manager-of"]);
        r.scope = Some(Cond::range(
            "amount",
            NumRange {
                lower: bound(ObjKey::Int(1_000_000), false),
                upper: None,
            },
        ));
        let vs = evaluate(&f.snap, &f.cat, &r, u32::MAX);
        let by = verdicts_by_subject(&vs);
        assert_eq!(by[&1].verdict, Outcome::NotApplicable);
        assert_eq!(by[&2].verdict, Outcome::Ok);
        assert_eq!(by[&6].verdict, Outcome::Ok); // current amount 1,500,000 is in scope
        assert_eq!(by[&7].verdict, Outcome::NotApplicable); // missing value: not satisfied
        // the same scope read as-of the approval instant drops request 6 (400,000 then)
        r.scope = r.scope.map(|c| c.as_of("approved-at"));
        let vs = evaluate(&f.snap, &f.cat, &r, u32::MAX);
        assert_eq!(verdicts_by_subject(&vs)[&6].verdict, Outcome::NotApplicable);

        // absent_when on a range: judged against an actual no request carries (`parent`), a
        // missing value is a gap only for amounts of at least 1,000,000
        r.scope = None;
        r.actual = "parent".into();
        r.absent_when = Some(Cond::range(
            "amount",
            NumRange {
                lower: bound(ObjKey::Int(1_000_000), true),
                upper: None,
            },
        ));
        let vs = evaluate(&f.snap, &f.cat, &r, u32::MAX);
        let by = verdicts_by_subject(&vs);
        assert_eq!(by[&1].verdict, Outcome::Ok);
        assert_eq!(by[&2].verdict, Outcome::Absent);
        assert_eq!(by[&7].verdict, Outcome::Ok); // a missing amount never triggers absence
    }

    #[test]
    fn range_condition_names_are_resolved() {
        let f = banded_fixture();
        let mut r = banded_rule(Some("no-such-anchor"), false);
        r.cases[1].required.push(Hop {
            predicate: "no-such-hop".into(),
            as_of: None,
        });
        let missing = unresolved_names(&r, &f.cat);
        assert!(missing.contains(&"no-such-anchor".to_string()));
        assert!(missing.contains(&"no-such-hop".to_string()));
        assert!(unresolved_names(&banded_rule(Some("approved-at"), false), &f.cat).is_empty());
    }

    // A literal-terminal graph: each Person subject reports to a manager whose `name` is the
    // expected value of the subject's `manager-name`. Manager 50 is "Grace" throughout; manager 51
    // is renamed "Ada" → "Lin" at valid-time 5000; manager 52 has no name. Managers carry an int
    // `level` (50 = 3) for the exact-keying check against the subject's float `manager-level`.
    fn literal_fixture() -> Fixture {
        let mut cat = Catalog::new();
        let person = cat.register_type("Person");
        let d = RelProps::default();
        let one = Cardinality::One;
        let txt = Range::Value(ValueType::Text);
        let reports_to = cat.register_predicate("reports-to", one, d, person, Range::Type(person));
        let name = cat.register_predicate("name", one, d, person, txt);
        let manager_name = cat.register_predicate("manager-name", one, d, person, txt);
        let review_time =
            cat.register_predicate("review-time", one, d, person, Range::Value(ValueType::Int));
        let status = cat.register_predicate("employment-status", one, d, person, txt);
        let level = cat.register_predicate("level", one, d, person, Range::Value(ValueType::Int));
        let manager_level = cat.register_predicate(
            "manager-level",
            one,
            d,
            person,
            Range::Value(ValueType::Float),
        );
        let mut ops: Vec<Op> = Vec::new();
        let mut seq = 0u64;
        for p in [50, 51, 52] {
            set_type(&mut ops, &mut seq, p, person);
        }
        set_one(&mut ops, &mut seq, 50, name, text("Grace"), 0);
        set_one(&mut ops, &mut seq, 50, level, ObjKey::Int(3), 0);
        set_one(&mut ops, &mut seq, 51, name, text("Ada"), 0);
        set_one(&mut ops, &mut seq, 51, name, text("Lin"), 5000);
        // (subject, manager, review-time, manager-name, employment-status)
        type Row<'a> = (NodeId, NodeId, Option<i64>, Option<&'a str>, &'a str);
        let rows: [Row; 7] = [
            (1, 50, Some(1000), Some("Grace"), "active"), // OK
            (2, 51, Some(6000), Some("Ada"), "active"),   // stale: Ada before the rename
            (3, 51, Some(1000), Some("Ada"), "active"),   // OK as-of before the rename
            (4, 51, Some(6000), Some("Bob"), "active"),   // wrong: never the name
            (5, 52, Some(1000), Some("Ivy"), "active"),   // the manager has no name
            (6, 50, None, Some("Grace"), "active"),       // no review instant
            (7, 50, Some(1000), None, "active"),          // ABSENT
        ];
        for (s, mgr, at, mname, st) in rows {
            set_type(&mut ops, &mut seq, s, person);
            set_one(&mut ops, &mut seq, s, reports_to, ObjKey::Node(mgr), 0);
            set_one(&mut ops, &mut seq, s, status, text(st), 0);
            if let Some(t) = at {
                set_one(&mut ops, &mut seq, s, review_time, ObjKey::Int(t), 0);
            }
            if let Some(m) = mname {
                set_one(&mut ops, &mut seq, s, manager_name, text(m), 0);
            }
        }
        set_one(&mut ops, &mut seq, 1, manager_level, float(3.0), 0);
        Fixture {
            snap: fold(&ops).observe(),
            cat,
        }
    }

    fn literal_rule(as_of: Option<&str>) -> Rule {
        Rule {
            subject_type: "Person".into(),
            scope: None,
            required: vec![
                Hop {
                    predicate: "reports-to".into(),
                    as_of: None,
                },
                Hop {
                    predicate: "name".into(),
                    as_of: as_of.map(str::to_string),
                },
            ],
            cases: Vec::new(),
            distinct_from: Vec::new(),
            actual: "manager-name".into(),
            absent_when: Some(Cond::equals("employment-status", text("active"))),
        }
    }

    #[test]
    fn a_literal_terminal_is_compared_by_value_as_of_the_anchor() {
        let f = literal_fixture();
        let vs = evaluate(
            &f.snap,
            &f.cat,
            &literal_rule(Some("review-time")),
            u32::MAX,
        );
        let by = verdicts_by_subject(&vs);
        let row = |s: NodeId| {
            let v = by[&s];
            (v.verdict, v.mismatch_kind, v.reason, v.required.clone())
        };
        let unresolved = Some(NotApplicableReason::RequiredUnresolved);
        assert_eq!(row(1), (Outcome::Ok, None, None, Some(text("Grace"))));
        assert_eq!(by[&1].actual, Some(text("Grace")));
        assert_eq!(by[&1].as_of, Some(1000));
        // the manager was renamed before the review: "Ada" held once, so the mismatch is stale
        assert_eq!(
            row(2),
            (
                Outcome::Mismatch,
                Some(MismatchKind::Stale),
                None,
                Some(text("Lin"))
            )
        );
        assert_eq!(by[&2].as_of, Some(6000));
        assert_eq!(row(3), (Outcome::Ok, None, None, Some(text("Ada"))));
        assert_eq!(
            row(4),
            (
                Outcome::Mismatch,
                Some(MismatchKind::Wrong),
                None,
                Some(text("Lin"))
            )
        );
        // a missing literal or a missing anchor leaves the expected value unknown
        assert_eq!(row(5), (Outcome::NotApplicable, None, unresolved, None));
        assert_eq!(by[&5].actual, Some(text("Ivy")));
        assert_eq!(row(6), (Outcome::NotApplicable, None, unresolved, None));
        assert_eq!(row(7), (Outcome::Absent, None, None, Some(text("Grace"))));
        // managers have no reports-to, no actual and no status: not expected, so OK
        assert_eq!(row(50), (Outcome::Ok, None, None, None));
    }

    #[test]
    fn a_timeless_literal_terminal_reads_the_current_value() {
        let f = literal_fixture();
        let vs = evaluate(&f.snap, &f.cat, &literal_rule(None), u32::MAX);
        let by = verdicts_by_subject(&vs);
        // the current name of 51 is "Lin"; a timeless final hop mismatches as wrong, never stale
        for s in [2, 3] {
            assert_eq!(by[&s].verdict, Outcome::Mismatch);
            assert_eq!(by[&s].mismatch_kind, Some(MismatchKind::Wrong));
            assert_eq!(by[&s].required, Some(text("Lin")));
            assert_eq!(by[&s].as_of, None);
        }
        // no anchor is needed, so subject 6 is judged
        assert_eq!(by[&6].verdict, Outcome::Ok);
    }

    #[test]
    fn a_literal_terminal_uses_the_ingest_keying() {
        let f = literal_fixture();
        let mut r = literal_rule(None);
        r.required[1].predicate = "level".into();
        r.actual = "manager-level".into();
        r.absent_when = None;
        let vs = evaluate_subjects(&f.snap, &f.cat, &r, u32::MAX, &[1]);
        // an int 3 and a float 3.0 are different stored values, like an `equals` condition
        assert_eq!(vs[0].verdict, Outcome::Mismatch);
        assert_eq!(vs[0].required, Some(ObjKey::Int(3)));
        assert_eq!(vs[0].actual, Some(float(3.0)));
        // and a literal distinct_from terminal collides by value
        let mut r = literal_rule(None);
        r.required.clear();
        r.distinct_from = hops(&["reports-to", "name"]);
        let vs = evaluate_subjects(&f.snap, &f.cat, &r, u32::MAX, &[1, 2]);
        assert_eq!(vs[0].verdict, Outcome::Mismatch);
        assert_eq!(vs[0].distinct, Some(text("Grace")));
        assert_eq!(vs[1].verdict, Outcome::Ok);
    }

    #[test]
    fn unwalkable_paths_are_reported() {
        let mut f = literal_fixture();
        assert!(path_errors(&literal_rule(Some("review-time")), &f.cat).is_empty());
        let person = f.cat.field_id("Person").unwrap();
        f.cat.register_predicate(
            "tags",
            Cardinality::Many,
            RelProps::default(),
            person,
            Range::Value(ValueType::Text),
        );
        // a literal before the last hop
        let mut r = literal_rule(None);
        r.required = hops(&["reports-to", "name", "reports-to"]);
        assert_eq!(
            path_errors(&r, &f.cat),
            vec![
                "required.hops[1] 'name' is literal-valued; only the last hop of a path may read a literal"
                    .to_string()
            ]
        );
        // a many-cardinality hop, in a case and in distinct_from
        let mut r = literal_rule(None);
        r.required.clear();
        r.cases = vec![Case {
            when: None,
            required: hops(&["reports-to", "tags"]),
        }];
        r.distinct_from = hops(&["tags"]);
        assert_eq!(
            path_errors(&r, &f.cat),
            vec![
                "cases[0].required.hops[1] 'tags' is many-cardinality; a derived path walks one-cardinality predicates only"
                    .to_string(),
                "distinct_from.hops[0] 'tags' is many-cardinality; a derived path walks one-cardinality predicates only"
                    .to_string(),
            ]
        );
        // an undeclared hop is left to unresolved_names
        let mut r = literal_rule(None);
        r.required = hops(&["no-such", "name"]);
        assert!(path_errors(&r, &f.cat).is_empty());
    }
}
