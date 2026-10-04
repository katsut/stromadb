//! The reverse value index behind `lookup`: `query::lookup_one` (index candidates confirmed by a
//! point read) must answer exactly what a full scan of the One keys answers — current and as-of,
//! under any label mask and predicate floor, through incremental re-observation, and after a
//! compaction round trip (gc + codec).

use proptest::prelude::*;
use stromadb_core::fold::{Fold, value_hash};
use stromadb_core::mask::Facts;
use stromadb_core::query::{self, point_one_asof};
use stromadb_core::{
    Cardinality, Catalog, FieldId, Masked, NodeId, ObjKey, Op, OrderKey, Range, RelProps, Snapshot,
    ValueType,
};

const SUBJECTS: u64 = 6;
const VALUES: i64 = 4;
const LABELS: u8 = 4;
const T_MAX: i64 = 40;

/// The pre-index `lookup`: a walk of every One key of the snapshot through the caller's view.
fn scan(
    facts: &impl Facts,
    pid: FieldId,
    want: &ObjKey,
    at: Option<i64>,
    keep: impl Fn(NodeId) -> bool,
) -> Vec<NodeId> {
    let mut ids = Vec::new();
    match at {
        None => facts.for_each_one(.., |&(n, p), v| {
            if p == pid && v.as_ref() == Some(want) && keep(n) {
                ids.push(n);
            }
        }),
        Some(at) => facts.for_each_one_key(|&(n, p)| {
            if p == pid && keep(n) && point_one_asof(facts, n, pid, at).as_ref() == Some(want) {
                ids.push(n);
            }
        }),
    }
    ids
}

fn catalog() -> (Catalog, [FieldId; 2], FieldId) {
    let mut c = Catalog::new();
    let t = c.register_type("Item");
    let one = |c: &mut Catalog, name: &str| {
        c.register_predicate(
            name,
            Cardinality::One,
            RelProps::default(),
            t,
            Range::Value(ValueType::Text),
        )
    };
    let a = one(&mut c, "key");
    let b = one(&mut c, "alias");
    let many = c.register_predicate(
        "tag",
        Cardinality::Many,
        RelProps::default(),
        t,
        Range::Value(ValueType::Text),
    );
    (c, [a, b], many)
}

fn value(v: i64) -> ObjKey {
    // half text, half int: both literal shapes go through the index
    if v % 2 == 0 {
        ObjKey::Text(format!("K-{v}"))
    } else {
        ObjKey::Int(v)
    }
}

#[derive(Clone, Debug)]
enum Tmpl {
    Set {
        subj: u64,
        pred: usize,
        v: i64,
        from: i64,
        to: Option<i64>,
        tx: u64,
    },
    Close {
        subj: u64,
        pred: usize,
        from: i64,
        tx: u64,
    },
    Delete {
        subj: u64,
        pred: usize,
        tx: u64,
    },
    AddMany {
        subj: u64,
        v: i64,
        tx: u64,
    },
}

fn tmpl() -> impl Strategy<Value = (Tmpl, Option<u8>)> {
    let label = prop_oneof![3 => Just(None), 2 => (0..LABELS).prop_map(Some)];
    let op = prop_oneof![
        8 => (1..=SUBJECTS, 0..2usize, 0..VALUES, 0..T_MAX, prop::option::of(1..T_MAX), 0..30u64)
            .prop_map(|(subj, pred, v, from, to, tx)| Tmpl::Set {
                subj,
                pred,
                v,
                from,
                to: to.map(|d| from + d),
                tx
            }),
        2 => (1..=SUBJECTS, 0..2usize, 0..T_MAX, 0..30u64)
            .prop_map(|(subj, pred, from, tx)| Tmpl::Close { subj, pred, from, tx }),
        1 => (1..=SUBJECTS, 0..2usize, 0..30u64)
            .prop_map(|(subj, pred, tx)| Tmpl::Delete { subj, pred, tx }),
        1 => (1..=SUBJECTS, 0..VALUES, 0..30u64)
            .prop_map(|(subj, v, tx)| Tmpl::AddMany { subj, v, tx }),
    ];
    (op, label)
}

fn ops(tmpls: &[(Tmpl, Option<u8>)], preds: [FieldId; 2], many: FieldId) -> Vec<(Op, Option<u8>)> {
    tmpls
        .iter()
        .enumerate()
        .map(|(i, (t, l))| {
            let ok = |tx: u64| OrderKey {
                tx,
                source: 1,
                seq: i as u64,
            };
            let op = match *t {
                Tmpl::Set {
                    subj,
                    pred,
                    v,
                    from,
                    to,
                    tx,
                } => Op::SetOne {
                    subject: subj,
                    predicate: preds[pred],
                    object: value(v),
                    valid_from: from,
                    valid_to: to,
                    ok: ok(tx),
                },
                Tmpl::Close {
                    subj,
                    pred,
                    from,
                    tx,
                } => Op::CloseOne {
                    subject: subj,
                    predicate: preds[pred],
                    valid_from: from,
                    ok: ok(tx),
                },
                Tmpl::Delete { subj, pred, tx } => Op::HardDelete {
                    subject: subj,
                    predicate: preds[pred],
                    ok: ok(tx),
                    cardinality: Cardinality::One,
                },
                Tmpl::AddMany { subj, v, tx } => Op::AddMany {
                    subject: subj,
                    predicate: many,
                    object: value(v),
                    valid_from: 0,
                    valid_to: None,
                    ok: ok(tx),
                },
            };
            (op, *l)
        })
        .collect()
}

fn key_of(op: &Op) -> (NodeId, FieldId) {
    match op {
        Op::SetOne {
            subject, predicate, ..
        }
        | Op::CloseOne {
            subject, predicate, ..
        }
        | Op::HardDelete {
            subject, predicate, ..
        }
        | Op::AddMany {
            subject, predicate, ..
        } => (*subject, *predicate),
        _ => unreachable!("the workload only writes graph keys"),
    }
}

/// Every lookup the scan answers, the index answers identically: each predicate, each value, the
/// current read and every instant, under the mask and a subject filter.
fn assert_equivalent(
    snap: &Snapshot,
    cat: &Catalog,
    preds: [FieldId; 2],
    mask: u32,
    drop: u64,
) -> Result<(), TestCaseError> {
    let facts = Masked::new(snap, cat, mask);
    let keep = |n: NodeId| n % 7 != drop;
    for p in preds {
        for v in 0..VALUES {
            let want = value(v);
            let instants = std::iter::once(None).chain((-1..=T_MAX + 1).map(Some));
            for at in instants {
                let fast = query::lookup_one(&facts, p, &want, at, keep);
                let slow = scan(&facts, p, &want, at, keep);
                prop_assert_eq!(fast, slow, "pred {} value {:?} at {:?}", p, want, at);
            }
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn index_lookup_equals_the_scan(
        tmpls in prop::collection::vec(tmpl(), 0..40),
        floor in prop::option::of(0..LABELS),
        mask in 0u32..(1 << LABELS),
        drop in 0u64..8,
    ) {
        let (mut cat, preds, many) = catalog();
        cat.set_label_floor(preds[0], floor);
        let ops = ops(&tmpls, preds, many);

        // the index is maintained incrementally: re-observe the touched key after every op
        let mut fold = Fold::default();
        let mut incr = Snapshot::default();
        for (op, l) in &ops {
            fold.apply_labeled(op, *l);
            fold.observe_key_into(&key_of(op), &mut incr);
        }
        let full = fold.observe();
        prop_assert_eq!(&incr, &full);
        assert_equivalent(&full, &cat, preds, mask, drop)?;
        // the unmasked view too (every mask bit allowed)
        assert_equivalent(&full, &cat, preds, u32::MAX, drop)?;

        // compaction: gc + codec round trip rebuilds the same index from the decoded fold
        let mut compacted = fold.clone();
        compacted.gc();
        let mut bytes = Vec::new();
        compacted.encode_into(&mut bytes);
        let decoded = Fold::decode(&bytes).expect("decode").observe();
        prop_assert_eq!(&decoded.one_values, &full.one_values);
        assert_equivalent(&decoded, &cat, preds, mask, drop)?;
    }
}

#[test]
fn index_holds_superseded_values_and_drops_purged_ones() {
    let (cat, [key, _], _) = catalog();
    let ok = |seq| OrderKey {
        tx: seq,
        source: 1,
        seq,
    };
    let set = |v: &str, from, seq| Op::SetOne {
        subject: 1,
        predicate: key,
        object: ObjKey::Text(v.into()),
        valid_from: from,
        valid_to: None,
        ok: ok(seq),
    };
    let mut fold = Fold::default();
    let mut snap = Snapshot::default();
    for op in [set("A-1", 10, 1), set("A-2", 20, 2)] {
        fold.apply(&op);
        fold.observe_key_into(&(1, key), &mut snap);
    }
    let a1 = ObjKey::Text("A-1".into());
    assert_eq!(
        snap.one_value_subjects(key, &a1).collect::<Vec<_>>(),
        vec![1]
    );
    let facts = Masked::new(&snap, &cat, u32::MAX);
    assert_eq!(
        query::lookup_one(&facts, key, &a1, Some(15), |_| true),
        vec![1]
    );
    assert!(query::lookup_one(&facts, key, &a1, None, |_| true).is_empty());

    // a hard delete purges both rows: the subject leaves the index
    fold.apply(&Op::HardDelete {
        subject: 1,
        predicate: key,
        ok: ok(3),
        cardinality: Cardinality::One,
    });
    fold.observe_key_into(&(1, key), &mut snap);
    assert!(snap.one_values.is_empty());
}

#[test]
fn a_hash_collision_only_adds_a_candidate_that_the_point_read_rejects() {
    let (cat, [key, _], _) = catalog();
    let mut fold = Fold::default();
    fold.apply(&Op::SetOne {
        subject: 1,
        predicate: key,
        object: ObjKey::Text("A-1".into()),
        valid_from: 0,
        valid_to: None,
        ok: OrderKey {
            tx: 1,
            source: 1,
            seq: 1,
        },
    });
    let mut snap = fold.observe();
    // forge a colliding entry: subject 1 now looks like a candidate for "B-2"
    let b2 = ObjKey::Text("B-2".into());
    snap.one_values.insert((key, value_hash(&b2), 1));
    let facts = Masked::new(&snap, &cat, u32::MAX);
    assert!(query::lookup_one(&facts, key, &b2, None, |_| true).is_empty());
    assert!(query::lookup_one(&facts, key, &b2, Some(5), |_| true).is_empty());
}
