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
    let dir = std::env::temp_dir()
        .join(format!("stroma_nbhd_test_{}", std::process::id()))
        .join("db");
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
    let mut db = Db::open(&dir).unwrap();
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
    let mut db = Db::open(&dir).unwrap();
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
    let mut db = Db::open(&dir).unwrap();
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
