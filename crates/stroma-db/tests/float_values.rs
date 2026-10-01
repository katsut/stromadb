//! Float literals keep `f64` precision end to end: a value ingested as JSON reads back bit-exact
//! through the fold key, a WAL replay, and a compaction snapshot, and conformance range bounds
//! compare against it at the same precision. `16777217` (2^24 + 1) and `1234567.89` are the two
//! shapes a single-precision key would round away; a property test covers random finite `f64`.

use proptest::prelude::*;
use serde_json::json;
use stromadb_store::Db;

const SCHEMA: &str = r#"
{"type_def":{"name":"Request"}}
{"type_def":{"name":"Person"}}
{"pred_def":{"name":"amount","cardinality":"one","domain":"Request","range_value":"float"}}
{"pred_def":{"name":"requester","cardinality":"one","domain":"Request","range":"Person"}}
{"pred_def":{"name":"approved-by","cardinality":"one","domain":"Request","range":"Person"}}
{"pred_def":{"name":"delegates-to","cardinality":"many","domain":"Person","range":"Person"}}
{"node":{"id":10,"type":"Person"}}
{"node":{"id":11,"type":"Person"}}
"#;

fn fresh(tag: &str) -> (std::path::PathBuf, Db) {
    let dir = std::env::temp_dir()
        .join(format!("stroma_float_{tag}_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(SCHEMA).unwrap();
    (dir, db)
}

fn request(id: u64, amount: f64) -> String {
    format!(
        "{{\"node\":{{\"id\":{id},\"type\":\"Request\"}}}}\n\
         {{\"fact\":{{\"subject\":{id},\"predicate\":\"amount\",\"object\":{{\"float\":{amount:?}}}}}}}\n\
         {{\"fact\":{{\"subject\":{id},\"predicate\":\"requester\",\"object\":{{\"node\":10}}}}}}\n\
         {{\"fact\":{{\"subject\":{id},\"predicate\":\"approved-by\",\"object\":{{\"node\":10}}}}}}\n"
    )
}

fn amount_of(db: &Db, id: u64) -> f64 {
    db.query(&json!({"op": "point", "subject": id, "predicate": "amount"}))
        .unwrap()["one"]["float"]
        .as_f64()
        .unwrap()
}

#[test]
fn float_values_round_trip_through_wal_and_snapshot() {
    let (dir, db) = fresh("roundtrip");
    let big = 16_777_217.0_f64; // 2^24 + 1, not representable in f32
    let cents = 1_234_567.89_f64;
    db.ingest_str(&request(1, big)).unwrap();
    db.ingest_str(&request(2, cents)).unwrap();
    // an edge property keyed through the bare-number path
    db.ingest_str(
        "{\"fact\":{\"subject\":10,\"predicate\":\"delegates-to\",\"object\":{\"node\":11},\"props\":{\"limit\":1234567.89,\"cap\":{\"float\":16777217.0}}}}\n",
    )
    .unwrap();

    let check = |db: &Db| {
        assert_eq!(amount_of(db, 1).to_bits(), big.to_bits());
        assert_eq!(amount_of(db, 2).to_bits(), cents.to_bits());
        assert_eq!(
            db.query(&json!({"op":"edge_props","subject":10,"predicate":"delegates-to","object":{"node":11}}))
                .unwrap(),
            json!({"props":{"limit":{"float":cents},"cap":{"float":big}}})
        );
    };
    check(&db);

    // replayed from the WAL
    drop(db); // release the directory lock
    let db = Db::open(&dir).unwrap();
    check(&db);

    // read from a compaction snapshot, then replayed from it
    db.compact().unwrap();
    check(&db);
    drop(db);
    let db = Db::open(&dir).unwrap();
    check(&db);
}

#[test]
fn range_bounds_distinguish_values_beyond_f32_precision() {
    let (_dir, db) = fresh("bands");
    db.ingest_str(&request(1, 16_777_216.0)).unwrap();
    db.ingest_str(&request(2, 16_777_217.0)).unwrap();
    db.ingest_str(&request(3, 1_234_567.88)).unwrap();
    db.ingest_str(&request(4, 1_234_567.89)).unwrap();

    // which subjects satisfy `cond` as the rule's scope
    let in_scope = |cond: serde_json::Value| -> Vec<u64> {
        let r = db
            .query(&json!({"op": "conformance", "rule": {
                "subject_type": "Request",
                "scope": cond,
                "required": {"hops": [{"predicate": "requester"}]},
                "actual": "approved-by"
            }, "only": ["OK"]}))
            .unwrap();
        r["verdicts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["subject"].as_u64().unwrap())
            .collect()
    };

    // 2^24 is a band edge: the value one above it must fall outside an inclusive upper bound
    assert_eq!(
        in_scope(json!({"predicate": "amount", "gte": 16777216, "lte": 16777216})),
        vec![1]
    );
    assert_eq!(
        in_scope(json!({"predicate": "amount", "gt": 16777216})),
        vec![2]
    );
    assert_eq!(
        in_scope(json!({"predicate": "amount", "gte": {"float": 16777217.0}})),
        vec![2]
    );
    // one cent apart: a bound at .89 separates .88 from .89 in both directions
    assert_eq!(
        in_scope(json!({"predicate": "amount", "gt": 1000000, "lt": 1234567.89})),
        vec![3]
    );
    assert_eq!(
        in_scope(json!({"predicate": "amount", "gte": 1234567.89, "lt": 2000000})),
        vec![4]
    );
    assert_eq!(
        in_scope(json!({"predicate": "amount", "between": [1234567.89, 1234567.89]})),
        vec![4]
    );
    // exact equality against the stored float
    assert_eq!(
        in_scope(json!({"predicate": "amount", "equals": {"float": 1234567.89}})),
        vec![4]
    );
    assert_eq!(
        in_scope(json!({"predicate": "amount", "equals": {"float": 16777217.0}})),
        vec![2]
    );
}

/// Any finite `f64` of either sign, plus money-like amounts in cents up to 10^12.
fn finite_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        prop::num::f64::NORMAL | prop::num::f64::SUBNORMAL | prop::num::f64::ZERO,
        (0i64..100_000_000_000_000).prop_map(|cents| cents as f64 / 100.0),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]
    // every finite f64 survives JSON ingest → fold → WAL replay bit-exact, and an inclusive
    // one-point band at that value selects exactly it
    #[test]
    fn any_finite_f64_round_trips(xs in proptest::collection::vec(finite_f64(), 1..8)) {
        let (dir, db) = fresh("prop");
        for (i, x) in xs.iter().enumerate() {
            db.ingest_str(&request(100 + i as u64, *x)).unwrap();
        }
        drop(db); // release the directory lock
        let db = Db::open(&dir).unwrap();
        for (i, x) in xs.iter().enumerate() {
            let id = 100 + i as u64;
            prop_assert_eq!(amount_of(&db, id).to_bits(), x.to_bits());
            let r = db
                .query(&json!({"op": "conformance", "rule": {
                    "subject_type": "Request",
                    "scope": {"predicate": "amount", "between": [x, x]},
                    "required": {"hops": [{"predicate": "requester"}]},
                    "actual": "approved-by"
                }, "subjects": [id]}))
                .unwrap();
            prop_assert_eq!(r["verdicts"][0]["verdict"].as_str(), Some("OK"));
        }
    }
}
