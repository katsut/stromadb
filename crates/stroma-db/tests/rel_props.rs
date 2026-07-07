//! End-to-end relationship-property expand: declare predicates carrying `symmetric`, `inverse`, and
//! `transitive` via `pred_def`, then assert the `expand` op evaluates them at query time. Covers the
//! forward-reference case (`child-of` declares `inverse:"parent-of"` before `parent-of` is defined),
//! undirected symmetric reads, inverse reads (reverse of the named predicate), and a depth-bounded
//! transitive closure — all across a durability reopen. No properties are ever pre-materialized.

use serde_json::json;
use stroma_db::Db;

// `child-of` is declared *before* `parent-of` to exercise the forward-reference resolution of its
// `inverse`. `knows` is symmetric; `ancestor-of` is transitive.
const FIXTURE: &str = r#"
{"type_def":{"name":"Person"}}
{"pred_def":{"name":"child-of","cardinality":"many","domain":"Person","range":"Person","inverse":"parent-of"}}
{"pred_def":{"name":"parent-of","cardinality":"many","domain":"Person","range":"Person"}}
{"pred_def":{"name":"knows","cardinality":"many","domain":"Person","range":"Person","symmetric":true}}
{"pred_def":{"name":"ancestor-of","cardinality":"many","domain":"Person","range":"Person","transitive":true}}
{"node":{"id":1,"type":"Person"}}
{"node":{"id":2,"type":"Person"}}
{"node":{"id":3,"type":"Person"}}
{"node":{"id":4,"type":"Person"}}
{"node":{"id":10,"type":"Person"}}
{"node":{"id":11,"type":"Person"}}
{"fact":{"subject":1,"predicate":"knows","object":{"node":2}}}
{"fact":{"subject":3,"predicate":"knows","object":{"node":1}}}
{"fact":{"subject":10,"predicate":"parent-of","object":{"node":11}}}
{"fact":{"subject":1,"predicate":"ancestor-of","object":{"node":2}}}
{"fact":{"subject":2,"predicate":"ancestor-of","object":{"node":3}}}
{"fact":{"subject":3,"predicate":"ancestor-of","object":{"node":4}}}
"#;

fn nodes(r: &serde_json::Value) -> Vec<u64> {
    let mut v: Vec<u64> = r["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n.as_u64().unwrap())
        .collect();
    v.sort_unstable();
    v
}

#[test]
fn expand_op_honors_declared_rel_props() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_relprops_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(FIXTURE).unwrap();

    // reopen so the read is served from the replayed schema + WAL (props survive the round-trip, and
    // the forward-referenced inverse resolves the same way on replay).
    let db = Db::open(&dir).unwrap();

    // symmetric: expanding `knows` on 1 is undirected — the forward edge (1→2) and the reverse (3→1).
    let r = db
        .query(&json!({"op":"expand","subject":1,"predicate":"knows"}))
        .unwrap();
    assert_eq!(nodes(&r), vec![2, 3]);

    // inverse: `child-of` stores no edges; expanding it on 11 yields 11's parents = {10} (reverse of
    // the stored `parent-of` edges), even though `parent-of` was declared after `child-of`.
    let r = db
        .query(&json!({"op":"expand","subject":11,"predicate":"child-of"}))
        .unwrap();
    assert_eq!(nodes(&r), vec![10]);
    // `parent-of` still expands directly (forward).
    let r = db
        .query(&json!({"op":"expand","subject":10,"predicate":"parent-of"}))
        .unwrap();
    assert_eq!(nodes(&r), vec![11]);

    // transitive: `ancestor-of` 1 → 2 → 3 → 4 gives the full closure by default (max_depth 16).
    let r = db
        .query(&json!({"op":"expand","subject":1,"predicate":"ancestor-of"}))
        .unwrap();
    assert_eq!(nodes(&r), vec![2, 3, 4]);
    // explicit max_depth bounds the closure.
    let r = db
        .query(&json!({"op":"expand","subject":1,"predicate":"ancestor-of","max_depth":1}))
        .unwrap();
    assert_eq!(nodes(&r), vec![2]);
    let r = db
        .query(&json!({"op":"expand","subject":1,"predicate":"ancestor-of","max_depth":2}))
        .unwrap();
    assert_eq!(nodes(&r), vec![2, 3]);

    // a property-free predicate behaves as the plain 1-hop expand (parent-of, single hop).
    let r = db
        .query(&json!({"op":"expand","subject":1,"predicate":"parent-of"}))
        .unwrap();
    assert_eq!(nodes(&r), Vec::<u64>::new());

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}
