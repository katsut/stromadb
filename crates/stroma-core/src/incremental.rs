//! Keyed-incremental Live Query maintenance — the efficient realization of the Live Query contract
//! for the completeness/rule class: instead of recomputing a standing rule over the whole
//! graph on every change (recompute-and-diff, [`crate::live`]), re-check only the subjects whose
//! inputs actually changed.
//!
//! A rule declares two things: `holds` (per-subject membership — is this subject a gap?) and
//! `candidates` (given a changed `(subject, predicate)`, which subjects might flip). The engine
//! supplies the changed keys from [`crate::engine::Engine::materialize_tracked`]; [`Maintained`]
//! re-checks only the candidates and emits the [`Diff`]. Cost is O(touched), not O(N) — and the
//! result is identical to a full recompute (property-tested below), provided `candidates` is a
//! superset of the truly-affected subjects.
//!
//! This is the incremental *class* (per-subject predicate with a declared key→candidate mapping),
//! not general differential dataflow (joins/arrangements). It slots behind the same register/diff
//! shape as [`crate::live::LiveQueries`].

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::catalog::Catalog;
use crate::conformance::{self, Rule, Verdict};
use crate::fact::{FieldId, NodeId};
use crate::fold::Snapshot;
use crate::live::Diff;

/// A standing completeness/rule query that can be maintained incrementally.
pub trait CompletenessRule {
    /// Full evaluation from scratch: every subject that currently violates (the gap set). Seeds the
    /// maintained state and is the correctness oracle.
    fn seed(&self, snap: &Snapshot) -> BTreeSet<NodeId>;

    /// Subjects whose membership might flip when `changed = (subject, predicate)` changed. MUST be a
    /// superset of the truly-affected subjects — otherwise maintenance drifts from recompute.
    fn candidates(&self, snap: &Snapshot, changed: (NodeId, FieldId)) -> Vec<NodeId>;

    /// Does `subject` currently violate (is it a gap)?
    fn holds(&self, snap: &Snapshot, subject: NodeId) -> bool;
}

/// A rule maintained against a live-updating snapshot: holds the current gap set and updates it in
/// O(touched) from the keys a materialize reported.
pub struct Maintained<R: CompletenessRule> {
    rule: R,
    gaps: BTreeSet<NodeId>,
}

impl<R: CompletenessRule> Maintained<R> {
    /// Seed from a full evaluation of the current snapshot.
    pub fn new(rule: R, snap: &Snapshot) -> Self {
        let gaps = rule.seed(snap);
        Maintained { rule, gaps }
    }

    /// The current gap set (implicit events: subjects that should be complete but are not).
    pub fn gaps(&self) -> &BTreeSet<NodeId> {
        &self.gaps
    }

    /// Re-check only the subjects the changed keys map to; update the gap set; return the delta.
    pub fn apply(&mut self, snap: &Snapshot, touched: &BTreeSet<(NodeId, FieldId)>) -> Diff {
        let mut candidates = BTreeSet::new();
        for &key in touched {
            candidates.extend(self.rule.candidates(snap, key));
        }
        let mut diff = Diff::default();
        for c in candidates {
            let now = self.rule.holds(snap, c);
            let was = self.gaps.contains(&c);
            if now && !was {
                self.gaps.insert(c);
                diff.added.insert(c);
            } else if !now && was {
                self.gaps.remove(&c);
                diff.removed.insert(c);
            }
        }
        diff
    }
}

/// One subject's verdict change: `old = None` means the subject just entered the maintained set
/// (a node newly of the rule's subject type), `new = None` that it left it. Both `Some` = the
/// verdict flipped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerdictDiff {
    pub subject: NodeId,
    pub old: Option<Verdict>,
    pub new: Option<Verdict>,
}

/// A declared conformance rule maintained incrementally against a live-updating snapshot: the full
/// per-subject verdict map stays current in O(touched) per materialize instead of O(subjects) per
/// poll, and every change comes out as a [`VerdictDiff`].
///
/// The mechanism is **support-set tracking**: judging a subject records every `(node, predicate)`
/// the judgment read ([`conformance::judge_traced`]) — including reads on *intermediate* nodes of
/// the derived paths, which is what lets one upstream write (a manager transfer) re-judge exactly
/// the subjects whose paths run through it. A write to a key re-judges precisely the subjects whose
/// last judgment read that key; a node-attribute touch re-judges the node itself (it may have
/// entered or left the subject type). Judged **unfiltered** (post-authz is the read side's job:
/// consumers filter diffs/verdicts by their own label mask, as the one-shot op does).
///
/// Correctness invariant (property-tested): after every `apply`, the maintained map equals a full
/// [`conformance::evaluate`] over the same snapshot.
pub struct MaintainedConformance {
    rule: Rule,
    verdicts: BTreeMap<NodeId, Verdict>,
    /// read key → subjects whose last judgment read it (the inverted support set)
    dependents: HashMap<(NodeId, FieldId), BTreeSet<NodeId>>,
    /// subject → the keys its last judgment read (for cleanup on re-judge / removal)
    supports: HashMap<NodeId, Vec<(NodeId, FieldId)>>,
}

impl MaintainedConformance {
    /// Seed from a full traced evaluation of the current snapshot.
    pub fn new(rule: Rule, snap: &Snapshot, cat: &Catalog) -> Self {
        let mut m = MaintainedConformance {
            rule,
            verdicts: BTreeMap::new(),
            dependents: HashMap::new(),
            supports: HashMap::new(),
        };
        let Some(ty) = cat.field_id(&m.rule.subject_type) else {
            return m;
        };
        let subjects: Vec<NodeId> = snap
            .node_types
            .iter()
            .filter(|&(_, &t)| t == ty)
            .map(|(&n, _)| n)
            .collect();
        for s in subjects {
            m.judge_into(snap, cat, s);
        }
        m
    }

    /// The maintained per-subject verdicts (unfiltered — apply the consumer's label mask on read).
    pub fn verdicts(&self) -> &BTreeMap<NodeId, Verdict> {
        &self.verdicts
    }

    /// Re-judge only what the materialized tail touched (`keys` + attribute-touched `nodes` from
    /// [`crate::engine::Engine::materialize_tracked_with_nodes`]); return the verdict changes.
    pub fn apply(
        &mut self,
        snap: &Snapshot,
        cat: &Catalog,
        keys: &BTreeSet<(NodeId, FieldId)>,
        nodes: &BTreeSet<NodeId>,
    ) -> Vec<VerdictDiff> {
        let Some(ty) = cat.field_id(&self.rule.subject_type) else {
            return Vec::new();
        };
        let mut candidates: BTreeSet<NodeId> = BTreeSet::new();
        for key in keys {
            if let Some(deps) = self.dependents.get(key) {
                candidates.extend(deps.iter().copied());
            }
            // catch-up: a subject-typed node with no verdict yet. Normally impossible (the type
            // touch below covers new subjects), but reachable when earlier applies could not
            // resolve the subject type name at all (rule declared before the type existed) — the
            // node's later fact writes are then the only signal left.
            if snap.node_types.get(&key.0) == Some(&ty) && !self.verdicts.contains_key(&key.0) {
                candidates.insert(key.0);
            }
        }
        for &n in nodes {
            // entered, left, or changed within the subject type — re-judge either way
            if snap.node_types.get(&n) == Some(&ty) || self.verdicts.contains_key(&n) {
                candidates.insert(n);
            }
        }
        let mut diffs = Vec::new();
        for s in candidates {
            let old = self.verdicts.get(&s).cloned();
            let new = if snap.node_types.get(&s) == Some(&ty) {
                Some(self.judge_into(snap, cat, s))
            } else {
                self.remove(s);
                None
            };
            if old != new {
                diffs.push(VerdictDiff {
                    subject: s,
                    old,
                    new,
                });
            }
        }
        diffs
    }

    /// Judge `s` traced, replace its support set, store and return the verdict.
    fn judge_into(&mut self, snap: &Snapshot, cat: &Catalog, s: NodeId) -> Verdict {
        self.clear_supports(s);
        let mut deps: Vec<(NodeId, FieldId)> = Vec::new();
        let v = conformance::judge_traced(snap, cat, &self.rule, s, &mut |n, p| deps.push((n, p)));
        for &key in &deps {
            self.dependents.entry(key).or_default().insert(s);
        }
        self.supports.insert(s, deps);
        self.verdicts.insert(s, v.clone());
        v
    }

    fn remove(&mut self, s: NodeId) {
        self.clear_supports(s);
        self.verdicts.remove(&s);
    }

    fn clear_supports(&mut self, s: NodeId) {
        for key in self.supports.remove(&s).unwrap_or_default() {
            if let Some(set) = self.dependents.get_mut(&key) {
                set.remove(&s);
                if set.is_empty() {
                    self.dependents.remove(&key);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::WriteKind;
    use crate::engine::Engine;
    use crate::fold::ObjKey;
    use crate::query;

    // Toy rule over subjects 1..=20: gap iff amount > 10 and no `approved` edge. A change to a
    // subject's amount or approved edge flips only that subject.
    struct Toy {
        amount: FieldId,
        approved: FieldId,
        subjects: Vec<NodeId>,
    }

    impl CompletenessRule for Toy {
        fn seed(&self, snap: &Snapshot) -> BTreeSet<NodeId> {
            self.subjects
                .iter()
                .copied()
                .filter(|&s| self.holds(snap, s))
                .collect()
        }
        fn candidates(&self, _snap: &Snapshot, (s, _p): (NodeId, FieldId)) -> Vec<NodeId> {
            if self.subjects.contains(&s) {
                vec![s]
            } else {
                vec![]
            }
        }
        fn holds(&self, snap: &Snapshot, s: NodeId) -> bool {
            let amt = match query::point_one(snap, s, self.amount) {
                Some(ObjKey::Int(n)) => n,
                _ => 0,
            };
            amt > 10 && query::expand(snap, s, self.approved).is_empty()
        }
    }

    fn splitmix(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    #[test]
    fn incremental_equals_full_recompute_over_random_stream() {
        let amount = 1u32;
        let approved = 2u32;
        let subjects: Vec<NodeId> = (1..=20).collect();

        let mut eng = Engine::new(1 << 20);
        eng.materialize();
        let mut maintained = Maintained::new(
            Toy {
                amount,
                approved,
                subjects: subjects.clone(),
            },
            &eng.snapshot(),
        );
        let oracle = Toy {
            amount,
            approved,
            subjects: subjects.clone(),
        };

        let mut rng = 0x1234_5678_9abc_def0u64;
        for _ in 0..3000 {
            let s = 1 + (splitmix(&mut rng) % 20);
            match splitmix(&mut rng) % 3 {
                0 => {
                    let v = (splitmix(&mut rng) % 20) as i64;
                    eng.write(
                        0,
                        WriteKind::SetOne {
                            subject: s,
                            predicate: amount,
                            object: ObjKey::Int(v),
                            valid_from: 0,
                            valid_to: None,
                        },
                    )
                    .unwrap();
                }
                1 => {
                    eng.write(
                        0,
                        WriteKind::AddMany {
                            subject: s,
                            predicate: approved,
                            object: ObjKey::Node(100),
                            valid_from: 0,
                            valid_to: None,
                        },
                    )
                    .unwrap();
                }
                _ => {
                    // the engine resolves observed OR-Set tags itself (diff-reflection resolver)
                    let _ = eng.retract_edge(0, s, approved, ObjKey::Node(100)).unwrap();
                }
            }
            let touched = eng.materialize_tracked();
            let snap = eng.snapshot();
            maintained.apply(&snap, &touched);
            // the invariant: keyed-incremental maintenance == full recompute, after every event
            assert_eq!(maintained.gaps(), &oracle.seed(&snap));
        }
    }

    #[test]
    fn conformance_maintenance_equals_full_eval_over_random_stream() {
        use crate::catalog::{Cardinality, Range, RelProps, ValueType};

        // The full approval-shaped rule (scope + 3-hop as-of required path + distinct_from +
        // absent_when) over a churning graph: assignments and approvals flip, departments and
        // managers transfer with valid-time (late-arriving corrections included), approvals get
        // closed, statuses change, and fresh nodes enter the subject type mid-stream.
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
        let text = |s: &str| ObjKey::Text(s.to_string());
        let rule = Rule {
            subject_type: "Issue".into(),
            scope: Some(conformance::Cond {
                predicate: "issue-type".into(),
                equals: text("release"),
            }),
            required: vec![
                conformance::Hop {
                    predicate: "assigned-to".into(),
                    as_of: None,
                },
                conformance::Hop {
                    predicate: "member-of".into(),
                    as_of: None,
                },
                conformance::Hop {
                    predicate: "manager-of".into(),
                    as_of: Some("approved-at".into()),
                },
            ],
            distinct_from: vec![conformance::Hop {
                predicate: "assigned-to".into(),
                as_of: None,
            }],
            actual: "approved-by".into(),
            absent_when: Some(conformance::Cond {
                predicate: "status".into(),
                equals: text("released"),
            }),
        };

        let persons: Vec<NodeId> = (1..=6).collect();
        let depts = [100u64, 101];
        let issues: Vec<NodeId> = (1000..1012).collect();

        let mut eng = Engine::new(1 << 20);
        for &p in &persons {
            eng.write(
                0,
                WriteKind::SetNodeType {
                    node: p,
                    type_id: person,
                },
            )
            .unwrap();
        }
        for &dp in &depts {
            eng.write(
                0,
                WriteKind::SetNodeType {
                    node: dp,
                    type_id: dept,
                },
            )
            .unwrap();
        }
        // half the issues exist up front; the rest enter the subject type mid-stream
        for &i in &issues[..6] {
            eng.write(
                0,
                WriteKind::SetNodeType {
                    node: i,
                    type_id: issue,
                },
            )
            .unwrap();
        }
        eng.materialize();
        let mut maintained = MaintainedConformance::new(rule.clone(), &eng.snapshot(), &cat);

        let full = |snap: &Snapshot| -> BTreeMap<NodeId, Verdict> {
            conformance::evaluate(snap, &cat, &rule, u32::MAX)
                .into_iter()
                .map(|v| (v.subject, v))
                .collect()
        };

        let set_one = |s: NodeId, p: FieldId, o: ObjKey, vf: i64| WriteKind::SetOne {
            subject: s,
            predicate: p,
            object: o,
            valid_from: vf,
            valid_to: None,
        };
        let mut rng = 0xfeed_beef_dead_c0deu64;
        for step in 0..2500u64 {
            let pick = |r: u64, xs: &[u64]| xs[(r % xs.len() as u64) as usize];
            let i = pick(splitmix(&mut rng), &issues);
            let p = pick(splitmix(&mut rng), &persons);
            let dp = pick(splitmix(&mut rng), &depts);
            let vf = (splitmix(&mut rng) % 10_000) as i64; // late arrivals: earlier vf, later write
            let kind = match splitmix(&mut rng) % 9 {
                0 => set_one(
                    i,
                    issue_type,
                    text(if splitmix(&mut rng).is_multiple_of(2) {
                        "release"
                    } else {
                        "task"
                    }),
                    0,
                ),
                1 => set_one(i, assigned_to, ObjKey::Node(p), 0),
                2 => set_one(i, approved_by, ObjKey::Node(p), vf),
                3 => set_one(i, approved_at, ObjKey::Int(vf), 0),
                4 => set_one(
                    i,
                    status,
                    text(if splitmix(&mut rng).is_multiple_of(2) {
                        "released"
                    } else {
                        "open"
                    }),
                    0,
                ),
                5 => set_one(p, member_of, ObjKey::Node(dp), vf),
                6 => set_one(dp, manager_of, ObjKey::Node(p), vf),
                7 => WriteKind::CloseOne {
                    subject: i,
                    predicate: approved_by,
                    valid_from: vf,
                },
                _ => WriteKind::SetNodeType {
                    node: pick(splitmix(&mut rng), &issues[6..]),
                    type_id: issue,
                },
            };
            eng.write(0, kind).unwrap();
            let (keys, nodes) = eng.materialize_tracked_with_nodes();
            let snap = eng.snapshot();
            let diffs = maintained.apply(&snap, &cat, &keys, &nodes);
            // the invariant: keyed-incremental maintenance == full evaluation, after every event
            assert_eq!(
                maintained.verdicts(),
                &full(&snap),
                "diverged at step {step} (diffs that round: {diffs:?})"
            );
        }
    }
}
