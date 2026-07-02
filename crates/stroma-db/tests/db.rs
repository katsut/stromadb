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
