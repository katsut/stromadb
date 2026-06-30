//! Fold determinism: out-of-order / multi-source / redelivered diffs converge to one snapshot.
//! Ports the Phase 0 `poc-fold-determinism` properties onto the engine's `fold` types.

use proptest::prelude::*;
use stroma_core::{Cardinality, ObjKey, Op, OrderKey, fold};

const SUBJECTS: u64 = 3;
const ONE_PREDS: [u32; 2] = [0, 1];
const MANY_PREDS: [u32; 2] = [100, 101];
const OBJECTS: u64 = 5;
const TX: u64 = 3; // small range so transaction-time ties are common (exercises tie-break)
const SRC: u32 = 3;

#[derive(Clone, Debug)]
enum Tmpl {
    SetOne {
        subj: u64,
        pred: u32,
        object: u64,
        valid_from: i64,
        tx: u64,
        src: u32,
    },
    CloseOne {
        subj: u64,
        pred: u32,
        valid_from: i64,
        tx: u64,
        src: u32,
    },
    AddMany {
        subj: u64,
        pred: u32,
        object: u64,
        tx: u64,
        src: u32,
    },
    RemoveMany {
        subj: u64,
        pred: u32,
        targets: Vec<u64>,
        tx: u64,
        src: u32,
    },
    HardDelete {
        subj: u64,
        pred: u32,
        many: bool,
        tx: u64,
        src: u32,
    },
}

fn txsrc(t: &Tmpl) -> (u64, u32) {
    match t {
        Tmpl::SetOne { tx, src, .. }
        | Tmpl::CloseOne { tx, src, .. }
        | Tmpl::AddMany { tx, src, .. }
        | Tmpl::RemoveMany { tx, src, .. }
        | Tmpl::HardDelete { tx, src, .. } => (*tx, *src),
    }
}

/// Unique seq = index → no OrderKey collisions; resolve RemoveMany targets to earlier ops' keys.
fn materialize(tmpls: &[Tmpl]) -> Vec<Op> {
    let oks: Vec<OrderKey> = tmpls
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let (tx, src) = txsrc(t);
            OrderKey {
                tx,
                source: src,
                seq: i as u64,
            }
        })
        .collect();

    tmpls
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let ok = oks[i];
            match t {
                Tmpl::SetOne {
                    subj,
                    pred,
                    object,
                    valid_from,
                    ..
                } => Op::SetOne {
                    subject: *subj,
                    predicate: *pred,
                    object: ObjKey::Node(*object),
                    valid_from: *valid_from,
                    ok,
                },
                Tmpl::CloseOne {
                    subj,
                    pred,
                    valid_from,
                    ..
                } => Op::CloseOne {
                    subject: *subj,
                    predicate: *pred,
                    valid_from: *valid_from,
                    ok,
                },
                Tmpl::AddMany {
                    subj, pred, object, ..
                } => Op::AddMany {
                    subject: *subj,
                    predicate: *pred,
                    object: ObjKey::Node(*object),
                    ok,
                },
                Tmpl::RemoveMany {
                    subj,
                    pred,
                    targets,
                    ..
                } => {
                    let observed = targets
                        .iter()
                        .filter(|&&t| (t as usize) < i)
                        .map(|&t| oks[t as usize])
                        .collect();
                    Op::RemoveMany {
                        subject: *subj,
                        predicate: *pred,
                        observed,
                    }
                }
                Tmpl::HardDelete {
                    subj, pred, many, ..
                } => Op::HardDelete {
                    subject: *subj,
                    predicate: *pred,
                    ok,
                    cardinality: if *many {
                        Cardinality::Many
                    } else {
                        Cardinality::One
                    },
                },
            }
        })
        .collect()
}

fn tmpl_strategy() -> impl Strategy<Value = Tmpl> {
    prop_oneof![
        (
            0..SUBJECTS,
            prop::sample::select(ONE_PREDS.to_vec()),
            0..OBJECTS,
            0..TX,
            0..TX,
            0..SRC
        )
            .prop_map(|(subj, pred, object, valid_from, tx, src)| Tmpl::SetOne {
                subj,
                pred,
                object,
                valid_from: valid_from as i64,
                tx,
                src
            }),
        (
            0..SUBJECTS,
            prop::sample::select(ONE_PREDS.to_vec()),
            0..TX,
            0..TX,
            0..SRC
        )
            .prop_map(|(subj, pred, valid_from, tx, src)| Tmpl::CloseOne {
                subj,
                pred,
                valid_from: valid_from as i64,
                tx,
                src
            }),
        (
            0..SUBJECTS,
            prop::sample::select(MANY_PREDS.to_vec()),
            0..OBJECTS,
            0..TX,
            0..SRC
        )
            .prop_map(|(subj, pred, object, tx, src)| Tmpl::AddMany {
                subj,
                pred,
                object,
                tx,
                src
            }),
        (
            0..SUBJECTS,
            prop::sample::select(MANY_PREDS.to_vec()),
            prop::collection::vec(0..32u64, 0..4),
            0..TX,
            0..SRC
        )
            .prop_map(|(subj, pred, targets, tx, src)| Tmpl::RemoveMany {
                subj,
                pred,
                targets,
                tx,
                src
            }),
        (0..SUBJECTS, any::<bool>(), 0..TX, 0..SRC).prop_map(|(subj, many, tx, src)| {
            let pred = if many { MANY_PREDS[0] } else { ONE_PREDS[0] };
            Tmpl::HardDelete {
                subj,
                pred,
                many,
                tx,
                src,
            }
        }),
    ]
}

fn workload() -> impl Strategy<Value = Vec<Tmpl>> {
    prop::collection::vec(tmpl_strategy(), 0..40)
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn shuffled(ops: &[Op], seed: u64) -> Vec<Op> {
    let mut idx: Vec<usize> = (0..ops.len()).collect();
    idx.sort_by_key(|&i| splitmix(seed.wrapping_add((i as u64).wrapping_mul(0x0100_0000_01B3))));
    idx.into_iter().map(|i| ops[i].clone()).collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// P1 — permutation invariance.
    #[test]
    fn p1_permutation_invariant(tmpls in workload()) {
        let ops = materialize(&tmpls);
        let base = fold(&ops).observe();
        let mut rev = ops.clone();
        rev.reverse();
        prop_assert_eq!(&base, &fold(&rev).observe());
        for seed in [1u64, 7, 42, 1000, 999_999] {
            prop_assert_eq!(&base, &fold(&shuffled(&ops, seed)).observe());
        }
    }

    /// P2 — multi-source invariance: fold per source, then merge.
    #[test]
    fn p2_multisource_invariant(tmpls in workload()) {
        let ops = materialize(&tmpls);
        let base = fold(&ops).observe();
        for k in [2usize, 3, 4] {
            let mut sources: Vec<stroma_core::Fold> = (0..k).map(|_| stroma_core::Fold::default()).collect();
            for (i, op) in ops.iter().enumerate() {
                sources[(splitmix(i as u64) as usize) % k].apply(op);
            }
            let mut merged = stroma_core::Fold::default();
            for s in &sources {
                merged.merge(s);
            }
            prop_assert_eq!(&base, &merged.observe());
        }
    }

    /// P3 — idempotence (at-least-once redelivery).
    #[test]
    fn p3_idempotent(tmpls in workload()) {
        let ops = materialize(&tmpls);
        let base = fold(&ops).observe();
        let mut twice = ops.clone();
        twice.extend(ops.iter().cloned());
        prop_assert_eq!(&base, &fold(&twice).observe());
    }

    /// P4 — GC preserves observation and stays convergent after further merges.
    #[test]
    fn p4_gc_preserves(tmpls in workload()) {
        let ops = materialize(&tmpls);
        let mut f = fold(&ops);
        let before = f.observe();
        f.gc();
        prop_assert_eq!(&before, &f.observe());
        f.merge(&fold(&ops));
        prop_assert_eq!(&before, &f.observe());
    }
}
