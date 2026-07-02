//! Keyed-incremental Live Query maintenance — the efficient realization of the Live Query contract
//! (CAP-5) for the completeness/rule class: instead of recomputing a standing rule over the whole
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

use std::collections::BTreeSet;

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
}
