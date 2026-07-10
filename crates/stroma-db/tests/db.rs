//! Directory-backed DB: init → ingest → embed → query (point/expand/search + authz), across a reopen.

use serde_json::json;
use stroma_db::Db;

fn tmp() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("stroma_db_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d.join("db")
}

#[test]
fn ingest_embed_query_reopen() {
    let dir = tmp();
    Db::init(&dir).unwrap();

    let db = Db::open(&dir).unwrap();
    let s = db
        .ingest_str(concat!(
            "{\"type_def\":{\"name\":\"Person\"}}\n",
            "{\"type_def\":{\"name\":\"Project\"}}\n",
            "{\"pred_def\":{\"name\":\"works-on\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Project\"}}\n",
            "{\"pred_def\":{\"name\":\"age\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"int\"}}\n",
            "{\"node\":{\"id\":1,\"type\":\"Person\",\"label\":0}}\n",
            "{\"node\":{\"id\":2,\"type\":\"Project\",\"label\":0}}\n",
            "{\"node\":{\"id\":3,\"type\":\"Project\",\"label\":3}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"works-on\",\"object\":{\"node\":2}}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"works-on\",\"object\":{\"node\":3}}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"age\",\"object\":{\"int\":34}}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"age\",\"object\":{\"int\":35},\"valid_from\":1}}\n",
            "{\"retract\":{\"subject\":1,\"predicate\":\"works-on\",\"object\":{\"node\":3}}}\n",
        ))
        .unwrap();
    assert_eq!((s.facts, s.retracts), (4, 1));
    db.embed_str("{\"node\":1,\"vector\":[1,0,0,0]}\n{\"node\":2,\"vector\":[0.9,0.1,0,0]}\n{\"node\":3,\"vector\":[0,1,0,0]}\n").unwrap();

    // reopen: catalog replay + embedding reload must reconstruct the same state
    let db = Db::open(&dir).unwrap();

    // LWW: later valid_from wins (a current One read now also carries additive `confidence`)
    assert_eq!(
        db.query(&json!({"op":"point","subject":1,"predicate":"age"}))
            .unwrap()["one"],
        json!({"int":35})
    );
    // retract removed node 3
    assert_eq!(
        db.query(&json!({"op":"expand","subject":1,"predicate":"works-on"}))
            .unwrap(),
        json!({"nodes":[2]})
    );
    // typed hybrid search returns both projects (Person node 1 excluded by type)
    let r = db
        .query(&json!({"op":"search","type":"Project","vector":[1,0,0,0],"k":5}))
        .unwrap();
    assert_eq!(r["ids"], json!([2, 3]));
    // authz: label 3 denied when only label 0 allowed
    let r = db
        .query(&json!({"op":"search","type":"Project","vector":[1,0,0,0],"k":5,"allowed_labels":1}))
        .unwrap();
    assert_eq!(r["ids"], json!([2]));

    // durable_head counts every changelog record: 6 node ops (3 nodes × {type, label}) + 4 facts + 1
    // retract = 11 (node type/label assignments are now folded through the changelog, not a side file).
    assert_eq!(db.stats()["facts"]["durable_head"], json!(11));
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn valid_to_ingest_and_asof_read() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_validto_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    // A one-cardinality membership valid over [100, 200): ends at 200.
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Project\"}}\n",
        "{\"pred_def\":{\"name\":\"member-of\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range\":\"Project\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":9,\"type\":\"Project\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"member-of\",\"object\":{\"node\":9},\"valid_from\":100,\"valid_to\":200}}\n",
    ))
    .unwrap();

    // reopen so the read is served from the replayed WAL (exercises valid_to durability round-trip)
    let db = Db::open(&dir).unwrap();

    let asof = |at: i64| {
        db.query(&json!({"op":"point","subject":1,"predicate":"member-of","valid_at":at}))
            .unwrap()
    };
    assert_eq!(asof(50), json!({ "one": null })); // before the interval
    assert_eq!(asof(100), json!({ "one": { "node": 9 } })); // lower bound inclusive
    assert_eq!(asof(150), json!({ "one": { "node": 9 } })); // inside
    assert_eq!(asof(200), json!({ "one": null })); // upper bound exclusive = "no longer a member"
    // without valid_at, point returns the asserted current value (wall-clock-free).
    assert_eq!(
        db.query(&json!({"op":"point","subject":1,"predicate":"member-of"}))
            .unwrap()["one"],
        json!({ "node": 9 })
    );
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn close_ends_a_one_value() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_close_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    // assigned-to = project 9 from valid-time 100, then closed (no successor) at 200.
    let s = db
        .ingest_str(concat!(
            "{\"type_def\":{\"name\":\"Person\"}}\n",
            "{\"type_def\":{\"name\":\"Project\"}}\n",
            "{\"pred_def\":{\"name\":\"assigned-to\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range\":\"Project\"}}\n",
            "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
            "{\"node\":{\"id\":9,\"type\":\"Project\"}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"assigned-to\",\"object\":{\"node\":9},\"valid_from\":100}}\n",
            "{\"close\":{\"subject\":1,\"predicate\":\"assigned-to\",\"valid_from\":200,\"source\":\"hr\"}}\n",
        ))
        .unwrap();
    assert_eq!((s.facts, s.closes), (1, 1));

    // reopen so the close is served from the replayed WAL (durability round-trip)
    let db = Db::open(&dir).unwrap();

    // head: the close is the latest write, so the current value is absent
    assert_eq!(
        db.query(&json!({"op":"point","subject":1,"predicate":"assigned-to"}))
            .unwrap(),
        json!({ "one": null })
    );
    let asof = |at: i64| {
        db.query(&json!({"op":"point","subject":1,"predicate":"assigned-to","valid_at":at}))
            .unwrap()
    };
    assert_eq!(asof(50), json!({ "one": null })); // before the fact
    assert_eq!(asof(100), json!({ "one": { "node": 9 } })); // fact valid_from inclusive
    assert_eq!(asof(199), json!({ "one": { "node": 9 } })); // still in effect
    assert_eq!(asof(200), json!({ "one": null })); // closed at 200
    assert_eq!(asof(250), json!({ "one": null })); // stays closed (no successor)
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn close_asof_is_arrival_order_independent() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_close_rev_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    // same two records as `close_ends_a_one_value`, arriving in reverse: close first, fact second.
    // As-of reads resolve by valid-time among covering rows, so they must not change with arrival
    // order. (The head is arrival-ordered by design — LWW order key — so it is not asserted here.)
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Project\"}}\n",
        "{\"pred_def\":{\"name\":\"assigned-to\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range\":\"Project\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":9,\"type\":\"Project\"}}\n",
        "{\"close\":{\"subject\":1,\"predicate\":\"assigned-to\",\"valid_from\":200}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"assigned-to\",\"object\":{\"node\":9},\"valid_from\":100}}\n",
    ))
    .unwrap();

    let asof = |at: i64| {
        db.query(&json!({"op":"point","subject":1,"predicate":"assigned-to","valid_at":at}))
            .unwrap()
    };
    assert_eq!(asof(50), json!({ "one": null }));
    assert_eq!(asof(100), json!({ "one": { "node": 9 } }));
    assert_eq!(asof(199), json!({ "one": { "node": 9 } }));
    assert_eq!(asof(200), json!({ "one": null }));
    assert_eq!(asof(250), json!({ "one": null }));
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn close_with_no_prior_fact() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_close_bare_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Project\"}}\n",
        "{\"pred_def\":{\"name\":\"assigned-to\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range\":\"Project\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"close\":{\"subject\":1,\"predicate\":\"assigned-to\",\"valid_from\":200}}\n",
    ))
    .unwrap();

    // a close with nothing to close: head absent, and no instant resolves to a value
    assert_eq!(
        db.query(&json!({"op":"point","subject":1,"predicate":"assigned-to"}))
            .unwrap(),
        json!({ "one": null })
    );
    let asof = |at: i64| {
        db.query(&json!({"op":"point","subject":1,"predicate":"assigned-to","valid_at":at}))
            .unwrap()
    };
    assert_eq!(asof(100), json!({ "one": null })); // before the close's valid_from
    assert_eq!(asof(200), json!({ "one": null }));
    assert_eq!(asof(300), json!({ "one": null }));
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn retract_on_one_predicate_is_an_error() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_retract_one_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Project\"}}\n",
        "{\"pred_def\":{\"name\":\"assigned-to\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range\":\"Project\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":9,\"type\":\"Project\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"assigned-to\",\"object\":{\"node\":9}}}\n",
    ))
    .unwrap();

    // retract resolves OR-Set tags (many-only), so on a one-predicate it is rejected, not a no-op
    let err = db
        .ingest_str(
            "{\"retract\":{\"subject\":1,\"predicate\":\"assigned-to\",\"object\":{\"node\":9}}}\n",
        )
        .unwrap_err();
    assert!(
        err.contains("close") && err.contains("cardinality-one"),
        "error must point at the close record: {err}"
    );
    // the value is untouched by the rejected retract
    assert_eq!(
        db.query(&json!({"op":"point","subject":1,"predicate":"assigned-to"}))
            .unwrap()["one"],
        json!({ "node": 9 })
    );
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn close_validation_errors() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_close_val_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Project\"}}\n",
        "{\"pred_def\":{\"name\":\"works-on\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Project\"}}\n",
    ))
    .unwrap();

    // close on a many-predicate is rejected (use retract for a many-edge)
    let err = db
        .ingest_str("{\"close\":{\"subject\":1,\"predicate\":\"works-on\"}}\n")
        .unwrap_err();
    assert!(
        err.contains("cardinality-many") && err.contains("retract"),
        "unexpected error: {err}"
    );
    // close on an undeclared predicate is rejected
    let err = db
        .ingest_str("{\"close\":{\"subject\":1,\"predicate\":\"nope\"}}\n")
        .unwrap_err();
    assert!(err.contains("unknown predicate"), "unexpected error: {err}");
    // a name that is interned but not a predicate (a type) is rejected the same way
    let err = db
        .ingest_str("{\"close\":{\"subject\":1,\"predicate\":\"Person\"}}\n")
        .unwrap_err();
    assert!(err.contains("unknown predicate"), "unexpected error: {err}");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn absent_edge_retract_is_not_counted() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_retract_noop_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Project\"}}\n",
        "{\"pred_def\":{\"name\":\"works-on\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Project\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":9,\"type\":\"Project\"}}\n",
    ))
    .unwrap();

    // retracting an edge that was never asserted is a no-op and must not count as a retract
    let s = db
        .ingest_str(
            "{\"retract\":{\"subject\":1,\"predicate\":\"works-on\",\"object\":{\"node\":9}}}\n",
        )
        .unwrap();
    assert_eq!(s.retracts, 0);

    // a retract that removes a present edge still counts
    db.ingest_str(
        "{\"fact\":{\"subject\":1,\"predicate\":\"works-on\",\"object\":{\"node\":9}}}\n",
    )
    .unwrap();
    let s = db
        .ingest_str(
            "{\"retract\":{\"subject\":1,\"predicate\":\"works-on\",\"object\":{\"node\":9}}}\n",
        )
        .unwrap();
    assert_eq!(s.retracts, 1);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn reset_clears_the_database() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_reset_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"knows\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Person\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":2}}}\n",
    ))
    .unwrap();
    // 2 node type ops (nodes 1, 2 — no labels) + 1 fact = 3 changelog records.
    assert_eq!(db.stats()["facts"]["durable_head"], json!(3));

    db.reset().unwrap();

    // empty after reset: the fact is gone and the predicate is unknown
    assert_eq!(db.stats()["facts"]["durable_head"], json!(0));
    assert!(
        db.query(&json!({"op":"expand","subject":1,"predicate":"knows"}))
            .is_err(),
        "predicate should be unknown after reset"
    );

    // the db is usable again after reset — a fresh schema (incl. a different cardinality) loads clean,
    // and the state survives a reopen
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"knows\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Person\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":2}}}\n",
    ))
    .unwrap();
    let db = Db::open(&dir).unwrap();
    assert_eq!(
        db.query(&json!({"op":"point","subject":1,"predicate":"knows"}))
            .unwrap()["one"],
        json!({"node":2})
    );
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn redefining_predicate_cardinality_is_rejected() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_carddef_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Project\"}}\n",
        "{\"pred_def\":{\"name\":\"assigned-to\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Project\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Project\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"assigned-to\",\"object\":{\"node\":2}}}\n",
    ))
    .unwrap();

    // re-sending the same definition is idempotent (allowed)
    assert!(
        db.ingest_str("{\"pred_def\":{\"name\":\"assigned-to\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Project\"}}\n")
            .is_ok()
    );

    // redefining it with a different cardinality is a clean error, not a panic / 500
    let err = db
        .ingest_str("{\"pred_def\":{\"name\":\"assigned-to\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range\":\"Project\"}}\n")
        .unwrap_err();
    assert!(
        err.contains("already defined with cardinality"),
        "unexpected error: {err}"
    );

    // the original many-edge still works after the rejected redefinition
    assert_eq!(
        db.query(&json!({"op":"expand","subject":1,"predicate":"assigned-to"}))
            .unwrap(),
        json!({"nodes":[2]})
    );
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn edge_props_ingest_and_read() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_edgeprops_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Skill\"}}\n",
        "{\"pred_def\":{\"name\":\"has-skill\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Skill\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":20,\"type\":\"Skill\"}}\n",
        "{\"node\":{\"id\":21,\"type\":\"Skill\"}}\n",
        // has-skill edge carrying a level and a role; a second skill with no props
        "{\"fact\":{\"subject\":1,\"predicate\":\"has-skill\",\"object\":{\"node\":20},\"props\":{\"level\":5,\"role\":\"expert\"}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"has-skill\",\"object\":{\"node\":21}}}\n",
        // LWW overwrite of the level on the same edge
        "{\"fact\":{\"subject\":1,\"predicate\":\"has-skill\",\"object\":{\"node\":20},\"props\":{\"level\":4}}}\n",
    ))
    .unwrap();

    // reopen: the edge properties must survive the WAL round-trip (replayed, not derived from schema)
    let db = Db::open(&dir).unwrap();

    // the skill edge still expands
    assert_eq!(
        db.query(&json!({"op":"expand","subject":1,"predicate":"has-skill"}))
            .unwrap(),
        json!({"nodes":[20,21]})
    );
    // level LWW-overwritten to 4; role kept
    assert_eq!(
        db.query(
            &json!({"op":"edge_props","subject":1,"predicate":"has-skill","object":{"node":20}})
        )
        .unwrap(),
        json!({"props":{"level":{"int":4},"role":{"text":"expert"}}})
    );
    // an edge with no properties returns an empty map
    assert_eq!(
        db.query(
            &json!({"op":"edge_props","subject":1,"predicate":"has-skill","object":{"node":21}})
        )
        .unwrap(),
        json!({"props":{}})
    );
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn neighborhood_khop_and_authz() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_nbhd_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    // chain 1 -> 2 -> 3 -> 4 via `knows`; node 3 is restricted (label 3)
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"knows\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\",\"label\":0}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Person\",\"label\":0}}\n",
        "{\"node\":{\"id\":3,\"type\":\"Person\",\"label\":3}}\n",
        "{\"node\":{\"id\":4,\"type\":\"Person\",\"label\":0}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":2}}}\n",
        "{\"fact\":{\"subject\":2,\"predicate\":\"knows\",\"object\":{\"node\":3}}}\n",
        "{\"fact\":{\"subject\":3,\"predicate\":\"knows\",\"object\":{\"node\":4}}}\n",
    ))
    .unwrap();

    let depths = |r: &serde_json::Value| -> std::collections::BTreeMap<u64, u64> {
        r["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| (n["id"].as_u64().unwrap(), n["depth"].as_u64().unwrap()))
            .collect()
    };

    // hops=1 (all predicates): focus + direct neighbours only
    let r = db
        .query(&json!({"op":"neighborhood","subject":1,"hops":1}))
        .unwrap();
    assert_eq!(depths(&r), [(1, 0), (2, 1)].into_iter().collect());

    // hops=3: the whole reachable chain with correct BFS depth
    let r = db
        .query(&json!({"op":"neighborhood","subject":1,"hops":3}))
        .unwrap();
    assert_eq!(
        depths(&r),
        [(1, 0), (2, 1), (3, 2), (4, 3)].into_iter().collect()
    );
    assert_eq!(r["edges"].as_array().unwrap().len(), 3);

    // undirected: from the middle node, reach the incoming (2->3) *and* outgoing (3->4) neighbours
    let r = db
        .query(&json!({"op":"neighborhood","subject":3,"hops":1}))
        .unwrap();
    assert_eq!(depths(&r), [(3, 0), (2, 1), (4, 1)].into_iter().collect());

    // authz: label 3 denied → node 3 pruned, so node 4 is unreachable through it
    let r = db
        .query(&json!({"op":"neighborhood","subject":1,"hops":3,"allowed_labels":1}))
        .unwrap();
    assert_eq!(depths(&r), [(1, 0), (2, 1)].into_iter().collect());
    assert_eq!(r["edges"], json!([[1, 2, 1]]));

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn node_detail_props_and_authz() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_node_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Project\"}}\n",
        "{\"pred_def\":{\"name\":\"works-on\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Project\"}}\n",
        "{\"pred_def\":{\"name\":\"age\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"int\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\",\"label\":0}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Project\",\"label\":0}}\n",
        "{\"node\":{\"id\":3,\"type\":\"Project\",\"label\":3}}\n",
        "{\"node\":{\"id\":9,\"type\":\"Person\",\"label\":3}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"works-on\",\"object\":{\"node\":2}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"works-on\",\"object\":{\"node\":3}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"age\",\"object\":{\"int\":34}}}\n",
    ))
    .unwrap();

    // full detail: type + label + One (age) and Many (works-on) props
    let r = db.query(&json!({"op":"node","subject":1})).unwrap();
    assert_eq!(r["id"], json!(1));
    assert_eq!(r["type"], json!("Person"));
    assert_eq!(r["label"], json!(0));
    let props = r["props"].as_array().unwrap();
    let age = props.iter().find(|p| p["predicate"] == "age").unwrap();
    assert_eq!(age["card"], json!("one"));
    assert_eq!(age["value"], json!({ "int": 34 }));
    let wo = props.iter().find(|p| p["predicate"] == "works-on").unwrap();
    assert_eq!(wo["card"], json!("many"));
    let mut objs: Vec<u64> = wo["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["node"].as_u64().unwrap())
        .collect();
    objs.sort_unstable();
    assert_eq!(objs, vec![2, 3]);

    // post-authz: restricted node (label 3) is not leaked when only label 0 is allowed
    let r = db
        .query(&json!({"op":"node","subject":9,"allowed_labels":1}))
        .unwrap();
    assert_eq!(r["denied"], json!(true));
    assert!(r.get("props").is_none());

    // node detail carries the stored embedding when present
    db.embed_str("{\"node\":1,\"vector\":[1,0,0,0]}\n").unwrap();
    let r = db.query(&json!({"op":"node","subject":1})).unwrap();
    assert_eq!(r["dim"], json!(4));
    assert_eq!(r["embedding"], json!([1.0, 0.0, 0.0, 0.0]));
    // a node without an embedding reports none
    let r = db.query(&json!({"op":"node","subject":2})).unwrap();
    assert_eq!(r["embedding"], json!(null));

    // schema op: predicate vocabulary + labels in use
    let r = db.query(&json!({"op":"schema"})).unwrap();
    let names: Vec<&str> = r["predicates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"works-on") && names.contains(&"age"));
    let wo = r["predicates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "works-on")
        .unwrap();
    assert_eq!(wo["card"], json!("many"));
    assert_eq!(wo["range"], json!({ "type": "Project" }));
    // labels 0 and 3 are assigned in this graph
    assert_eq!(r["labels"], json!([0, 3]));

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn graph_all_nodes_and_authz() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_graph_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    // 1 -> 2 -> 3 via `knows`; node 3 restricted (label 3)
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"knows\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"name\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\"}}\n",
        "{\"pred_def\":{\"name\":\"reports-to\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\",\"label\":0}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Person\",\"label\":0}}\n",
        "{\"node\":{\"id\":3,\"type\":\"Person\",\"label\":3}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"name\",\"object\":{\"text\":\"Root\"}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":2}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"reports-to\",\"object\":{\"node\":2}}}\n",
        "{\"fact\":{\"subject\":2,\"predicate\":\"knows\",\"object\":{\"node\":3}}}\n",
    ))
    .unwrap();

    let ids = |r: &serde_json::Value| -> Vec<u64> {
        let mut v: Vec<u64> = r["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["id"].as_u64().unwrap())
            .collect();
        v.sort_unstable();
        v
    };

    // no authz: every node, every edge; node 1 carries its display name
    let r = db.query(&json!({"op":"graph"})).unwrap();
    assert_eq!(ids(&r), vec![1, 2, 3]);
    assert_eq!(r["edges"].as_array().unwrap().len(), 2);
    assert_eq!(r["truncated"], json!(false));
    let n1 = r["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["id"] == 1)
        .unwrap();
    assert_eq!(n1["name"], json!("Root"));
    // edge strength = distinct predicates connecting the pair: (1,2) via knows+reports-to = 2
    let e12 = r["edges"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e[0] == 1 && e[1] == 2)
        .unwrap();
    assert_eq!(e12[2], json!(2));

    // authz: node 3 (label 3) hidden, and edges touching it dropped; (1,2) strength still 2
    let r = db.query(&json!({"op":"graph","allowed_labels":1})).unwrap();
    assert_eq!(ids(&r), vec![1, 2]);
    assert_eq!(r["edges"], json!([[1, 2, 2]]));

    // cap: max_nodes truncates and flags it
    let r = db.query(&json!({"op":"graph","max_nodes":2})).unwrap();
    assert_eq!(r["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(r["truncated"], json!(true));

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn overview_type_aggregate() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_ovw_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Project\"}}\n",
        "{\"type_def\":{\"name\":\"Team\"}}\n",
        "{\"pred_def\":{\"name\":\"works-on\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Project\"}}\n",
        "{\"pred_def\":{\"name\":\"member-of\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range\":\"Team\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\",\"label\":0}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Person\",\"label\":0}}\n",
        "{\"node\":{\"id\":10,\"type\":\"Project\",\"label\":0}}\n",
        "{\"node\":{\"id\":100,\"type\":\"Team\",\"label\":0}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"works-on\",\"object\":{\"node\":10}}}\n",
        "{\"fact\":{\"subject\":2,\"predicate\":\"works-on\",\"object\":{\"node\":10}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"member-of\",\"object\":{\"node\":100}}}\n",
        "{\"fact\":{\"subject\":2,\"predicate\":\"member-of\",\"object\":{\"node\":100}}}\n",
    ))
    .unwrap();

    let r = db.query(&json!({"op":"overview"})).unwrap();
    assert_eq!(r["overview"], json!(true));
    let node = |name: &str| -> serde_json::Value {
        r["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["name"] == name)
            .unwrap()
            .clone()
    };
    // one super-node per type, member counts, and a sample member id
    assert_eq!(node("Person")["count"], json!(2));
    assert_eq!(node("Project")["count"], json!(1));
    assert_eq!(node("Team")["count"], json!(1));
    assert_eq!(node("Person")["sample"], json!(1));
    // inter-type edges only: Person–Project and Person–Team (no intra-type self edge)
    assert_eq!(r["edges"].as_array().unwrap().len(), 2);

    // composable pipeline: source (node 1) → expand works-on → filter type Project
    let ids = |r: &serde_json::Value| -> Vec<u64> {
        let mut v: Vec<u64> = r["ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_u64().unwrap())
            .collect();
        v.sort_unstable();
        v
    };
    let r = db
        .query(&json!({"op":"pipeline","source":{"nodes":[1]}}))
        .unwrap();
    assert_eq!(ids(&r), vec![1]); // source = node 1
    let r = db
        .query(&json!({"op":"pipeline","source":{"nodes":[1]},"steps":[{"expand":"works-on"}]}))
        .unwrap();
    assert_eq!(ids(&r), vec![10]); // 1 -> works-on -> project 10
    let r = db
        .query(&json!({"op":"pipeline","source":{"nodes":[1]},"steps":[{"expand":"works-on"},{"filter_type":"Person"}]}))
        .unwrap();
    assert_eq!(ids(&r), Vec::<u64>::new()); // project 10 is not a Person → filtered out

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn retrieve_context_current_value_chronological() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_ctx_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Doc\"}}\n",
        "{\"pred_def\":{\"name\":\"content\",\"cardinality\":\"one\",\"domain\":\"Doc\",\"range_value\":\"text\"}}\n",
        "{\"pred_def\":{\"name\":\"at\",\"cardinality\":\"one\",\"domain\":\"Doc\",\"range_value\":\"int\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Doc\"}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Doc\"}}\n",
        "{\"node\":{\"id\":3,\"type\":\"Doc\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"content\",\"object\":{\"text\":\"alpha\"}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"at\",\"object\":{\"int\":1000000}}}\n",
        "{\"fact\":{\"subject\":2,\"predicate\":\"content\",\"object\":{\"text\":\"beta\"}}}\n",
        "{\"fact\":{\"subject\":2,\"predicate\":\"at\",\"object\":{\"int\":3000000}}}\n",
        "{\"fact\":{\"subject\":3,\"predicate\":\"content\",\"object\":{\"text\":\"gamma\"}}}\n",
        "{\"fact\":{\"subject\":3,\"predicate\":\"at\",\"object\":{\"int\":2000000}}}\n",
        // supersede doc 1's content (LWW) — retrieve must return the current value
        "{\"fact\":{\"subject\":1,\"predicate\":\"content\",\"object\":{\"text\":\"alpha-v2\"},\"valid_from\":1}}\n",
    ))
    .unwrap();
    db.embed_str("{\"node\":1,\"vector\":[1,0,0,0]}\n{\"node\":2,\"vector\":[0.99,0.01,0,0]}\n{\"node\":3,\"vector\":[0.98,0.02,0,0]}\n").unwrap();

    let r = db
        .query(&json!({"op":"retrieve_context","type":"Doc","vector":[1,0,0,0],"content":"content","date":"at","k":5,"as_of":4000000}))
        .unwrap();
    let hits = r["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 3);
    // chronological (oldest → newest)
    assert_eq!(
        hits.iter()
            .map(|h| h["date"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        vec![1000000, 2000000, 3000000]
    );
    // current-value bias: doc 1's superseded content is "alpha-v2", not "alpha"
    assert_eq!(hits[0]["content"], json!("alpha-v2"));
    assert_eq!(hits[1]["content"], json!("gamma"));
    assert_eq!(hits[2]["content"], json!("beta"));
    // each hit carries a calendar stamp; the assembled block is chronological
    assert!(hits.iter().all(|h| h["stamp"].is_string()));
    let ctx = r["context"].as_str().unwrap();
    assert!(ctx.find("alpha-v2").unwrap() < ctx.find("gamma").unwrap());
    assert!(ctx.find("gamma").unwrap() < ctx.find("beta").unwrap());
    assert_eq!(r["as_of"], json!(4000000));

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn provenance_capture_survives_reopen() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_prov_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Doc\"}}\n",
        "{\"pred_def\":{\"name\":\"title\",\"cardinality\":\"one\",\"domain\":\"Doc\",\"range_value\":\"text\"}}\n",
        "{\"pred_def\":{\"name\":\"note\",\"cardinality\":\"one\",\"domain\":\"Doc\",\"range_value\":\"text\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Doc\"}}\n",
        // two competing One writes on (1, title) from different sources; the later write wins.
        "{\"fact\":{\"subject\":1,\"predicate\":\"title\",\"object\":{\"text\":\"v1\"},\"valid_from\":10,\"source\":\"doc-A\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"title\",\"object\":{\"text\":\"v2\"},\"valid_from\":20,\"source\":\"doc-B\"}}\n",
        // a fact with no source: provenance must be absent (unset sentinel), shape unchanged.
        "{\"fact\":{\"subject\":1,\"predicate\":\"note\",\"object\":{\"text\":\"x\"}}}\n",
    ))
    .unwrap();

    // point carries the winner's source as `provenance`
    let r = db
        .query(&json!({"op":"point","subject":1,"predicate":"title"}))
        .unwrap();
    assert_eq!(r["one"], json!({ "text": "v2" }));
    assert_eq!(r["provenance"], json!("doc-B"));

    // a source-less One value omits provenance entirely (the additive `confidence` is still present
    // and reports the source-less value as tier "low").
    let r = db
        .query(&json!({"op":"point","subject":1,"predicate":"note"}))
        .unwrap();
    assert_eq!(r["one"], json!({ "text": "x" }));
    assert!(r.get("provenance").is_none());
    assert_eq!(r["confidence"]["tier"], json!("low"));

    // node detail carries the One value's source; the source-less One has no `source`
    let r = db.query(&json!({"op":"node","subject":1})).unwrap();
    let props = r["props"].as_array().unwrap();
    let title = props.iter().find(|p| p["predicate"] == "title").unwrap();
    assert_eq!(title["source"], json!("doc-B"));
    let note = props.iter().find(|p| p["predicate"] == "note").unwrap();
    assert!(note.get("source").is_none());

    // reopen (WAL replay + schema/source_def re-intern): provenance must survive
    let db = Db::open(&dir).unwrap();
    let r = db
        .query(&json!({"op":"point","subject":1,"predicate":"title"}))
        .unwrap();
    assert_eq!(r["one"], json!({ "text": "v2" }));
    assert_eq!(r["provenance"], json!("doc-B"));

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn coarse_confidence_from_provenance() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_conf_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Doc\"}}\n",
        "{\"pred_def\":{\"name\":\"title\",\"cardinality\":\"one\",\"domain\":\"Doc\",\"range_value\":\"text\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Doc\"}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Doc\"}}\n",
        "{\"node\":{\"id\":3,\"type\":\"Doc\"}}\n",
        // subject 1: the SAME value from two distinct sources → corroborated (high).
        "{\"fact\":{\"subject\":1,\"predicate\":\"title\",\"object\":{\"text\":\"v1\"},\"valid_from\":10,\"source\":\"doc-A\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"title\",\"object\":{\"text\":\"v1\"},\"valid_from\":20,\"source\":\"doc-B\"}}\n",
        // subject 2: a single sourced value → medium.
        "{\"fact\":{\"subject\":2,\"predicate\":\"title\",\"object\":{\"text\":\"v2\"},\"valid_from\":10,\"source\":\"doc-A\"}}\n",
        // subject 3: no source → low.
        "{\"fact\":{\"subject\":3,\"predicate\":\"title\",\"object\":{\"text\":\"v3\"},\"valid_from\":10}}\n",
    ))
    .unwrap();

    // corroborated → high; raw signals expose corroboration == sources == 2; no `now` → no age.
    let r = db
        .query(&json!({"op":"point","subject":1,"predicate":"title"}))
        .unwrap();
    assert_eq!(r["confidence"]["tier"], json!("high"));
    assert_eq!(r["confidence"]["corroboration"], json!(2));
    assert_eq!(r["confidence"]["sources"], json!(2));
    assert!(r["confidence"].get("age").is_none());

    // single source → medium
    let r = db
        .query(&json!({"op":"point","subject":2,"predicate":"title"}))
        .unwrap();
    assert_eq!(r["confidence"]["tier"], json!("medium"));
    assert_eq!(r["confidence"]["corroboration"], json!(1));

    // source-less → low
    let r = db
        .query(&json!({"op":"point","subject":3,"predicate":"title"}))
        .unwrap();
    assert_eq!(r["confidence"]["tier"], json!("low"));
    assert_eq!(r["confidence"]["corroboration"], json!(0));

    // corroborated but stale (now ≫ valid_from, small max_age) → low; age is exposed.
    let r = db
        .query(&json!({"op":"point","subject":1,"predicate":"title","now":1000,"max_age":100}))
        .unwrap();
    assert_eq!(r["confidence"]["tier"], json!("low"));
    assert_eq!(r["confidence"]["age"], json!(980)); // 1000 - valid_from(20)

    // an as-of read omits confidence entirely (identical old shape).
    let r = db
        .query(&json!({"op":"point","subject":1,"predicate":"title","valid_at":15}))
        .unwrap();
    assert!(r.get("confidence").is_none());

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}
