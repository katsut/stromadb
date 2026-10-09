//! `search` with `exact: true`: brute-force k-NN over the stored rows, type- and authz-scoped,
//! latest row per node.

use serde_json::{Value, json};
use stromadb_store::Db;

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("stroma_exact_search_{}_{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d.join("db")
}

fn ids(r: &Value) -> Vec<u64> {
    r["ids"]
        .as_array()
        .map(|a| a.iter().filter_map(Value::as_u64).collect())
        .unwrap_or_default()
}

/// 200 Comments spread on the unit circle in dims 0/1, and 3 Clauses (one sensitive) in dims 2/3.
fn setup(dir: &std::path::Path) -> Db {
    Db::init(dir).unwrap();
    let db = Db::open(dir).unwrap();
    let mut lines = vec![
        "{\"type_def\":{\"name\":\"Comment\"}}".to_string(),
        "{\"type_def\":{\"name\":\"Clause\"}}".to_string(),
    ];
    for n in 1..=200u64 {
        lines.push(format!("{{\"node\":{{\"id\":{n},\"type\":\"Comment\"}}}}"));
    }
    lines.push("{\"node\":{\"id\":1001,\"type\":\"Clause\"}}".to_string());
    lines.push("{\"node\":{\"id\":1002,\"type\":\"Clause\"}}".to_string());
    lines.push("{\"node\":{\"id\":1003,\"type\":\"Clause\",\"label\":3}}".to_string());
    db.ingest_str(&(lines.join("\n") + "\n")).unwrap();
    let mut emb = String::new();
    for n in 1..=200u64 {
        let a = n as f32 * 0.03;
        emb.push_str(&format!(
            "{{\"node\":{n},\"vector\":[{},{},0,0]}}\n",
            a.cos(),
            a.sin()
        ));
    }
    emb.push_str("{\"node\":1001,\"vector\":[0,0,1,0]}\n");
    emb.push_str("{\"node\":1002,\"vector\":[0,0,0.6,0.8]}\n");
    emb.push_str("{\"node\":1003,\"vector\":[0,0,0.8,0.6]}\n");
    db.embed_str(&emb).unwrap();
    db
}

#[test]
fn exact_search_ranks_every_row_of_the_type() {
    let dir = tmp("rank");
    let db = setup(&dir);
    // a query between the comment circle and the clauses: only Clauses come back, nearest first
    let r = db
        .query(
            &json!({"op":"search","type":"Clause","vector":[0.3,0,0.95,0.05],"k":5,"exact":true}),
        )
        .unwrap();
    assert_eq!(ids(&r), vec![1001, 1003, 1002]);
    let scores: Vec<f64> = r["scores"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_f64)
        .collect();
    assert!(scores.windows(2).all(|w| w[0] >= w[1]), "{scores:?}");
    // an identical vector scores 1 (distance 0)
    let r = db
        .query(&json!({"op":"search","type":"Clause","vector":[0,0,1,0],"k":1,"exact":true}))
        .unwrap();
    assert_eq!(ids(&r), vec![1001]);
    assert_eq!(r["scores"][0].as_f64(), Some(1.0));
    // k bounds the answer; the comment nearest to angle 0.9 rad is node 30
    let q = [0.9f32.cos(), 0.9f32.sin()];
    let r = db
        .query(&json!({"op":"search","type":"Comment","vector":[q[0],q[1],0,0],"k":3,"exact":true}))
        .unwrap();
    assert_eq!(ids(&r).len(), 3);
    assert_eq!(ids(&r)[0], 30);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn exact_search_is_authz_scoped() {
    let dir = tmp("authz");
    let db = setup(&dir);
    let r = db
        .query(&json!({"op":"search","type":"Clause","vector":[0,0,0.8,0.6],"k":5,"exact":true,"allowed_labels":1}))
        .unwrap();
    assert_eq!(ids(&r), vec![1002, 1001]);
    let r = db
        .query(&json!({"op":"search","type":"Clause","vector":[0,0,0.8,0.6],"k":5,"exact":true}))
        .unwrap();
    assert_eq!(ids(&r)[0], 1003);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn exact_search_uses_the_latest_row_of_a_node_across_reopen() {
    let dir = tmp("latest");
    let db = setup(&dir);
    // re-embed 1001 far away: its old row no longer matches, and it appears once
    db.embed_str("{\"node\":1001,\"vector\":[0,0,0,1]}\n")
        .unwrap();
    drop(db);
    let db = Db::open(&dir).unwrap();
    let r = db
        .query(&json!({"op":"search","type":"Clause","vector":[0,0,1,0],"k":5,"exact":true}))
        .unwrap();
    assert_eq!(ids(&r), vec![1003, 1002, 1001]);
    // node detail and the `similar` pipeline seed read the same latest row
    let n = db.query(&json!({"op":"node","subject":1001})).unwrap();
    assert_eq!(n["embedding"], json!([0.0, 0.0, 0.0, 1.0]));
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn search_rejects_a_query_of_the_wrong_dimension() {
    let dir = tmp("dim");
    let db = setup(&dir);
    for exact in [true, false] {
        let e = db
            .query(&json!({"op":"search","type":"Clause","vector":[1,0],"k":5,"exact":exact}))
            .unwrap_err();
        assert!(e.contains("dimension mismatch"), "{e}");
        // 1e100 overflows f32 to infinity
        let e = db
            .query(
                &json!({"op":"search","type":"Clause","vector":[1e100,0,0,0],"k":5,"exact":exact}),
            )
            .unwrap_err();
        assert!(e.contains("non-finite"), "{e}");
    }
    let r = db
        .query(&json!({"op":"search","type":"Clause","vector":[0,0,1,0],"k":0,"exact":true}))
        .unwrap();
    assert_eq!(ids(&r), Vec::<u64>::new());
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn exact_search_on_a_database_without_embeddings_is_empty() {
    let dir = tmp("empty");
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(
        "{\"type_def\":{\"name\":\"Clause\"}}\n{\"node\":{\"id\":1,\"type\":\"Clause\"}}\n",
    )
    .unwrap();
    let r = db
        .query(&json!({"op":"search","type":"Clause","vector":[1,0,0,0],"k":5,"exact":true}))
        .unwrap();
    assert_eq!(ids(&r), Vec::<u64>::new());
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}
