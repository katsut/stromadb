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

    let mut db = Db::open(&dir).unwrap();
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

    // LWW: later valid_from wins
    assert_eq!(
        db.query(&json!({"op":"point","subject":1,"predicate":"age"}))
            .unwrap(),
        json!({"one":{"int":35}})
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

    assert_eq!(db.stats()["facts"]["durable_head"], json!(5));
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn neighborhood_khop_and_authz() {
    let dir = std::env::temp_dir().join(format!("stroma_nbhd_test_{}", std::process::id())).join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let mut db = Db::open(&dir).unwrap();
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
        r["nodes"].as_array().unwrap().iter().map(|n| (n["id"].as_u64().unwrap(), n["depth"].as_u64().unwrap())).collect()
    };

    // hops=1 (all predicates): focus + direct neighbours only
    let r = db.query(&json!({"op":"neighborhood","subject":1,"hops":1})).unwrap();
    assert_eq!(depths(&r), [(1, 0), (2, 1)].into_iter().collect());

    // hops=3: the whole reachable chain with correct BFS depth
    let r = db.query(&json!({"op":"neighborhood","subject":1,"hops":3})).unwrap();
    assert_eq!(depths(&r), [(1, 0), (2, 1), (3, 2), (4, 3)].into_iter().collect());
    assert_eq!(r["edges"].as_array().unwrap().len(), 3);

    // authz: label 3 denied → node 3 pruned, so node 4 is unreachable through it
    let r = db.query(&json!({"op":"neighborhood","subject":1,"hops":3,"allowed_labels":1})).unwrap();
    assert_eq!(depths(&r), [(1, 0), (2, 1)].into_iter().collect());
    assert_eq!(r["edges"], json!([[1, 2]]));

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn retrieve_context_current_value_chronological() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_ctx_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let mut db = Db::open(&dir).unwrap();
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
