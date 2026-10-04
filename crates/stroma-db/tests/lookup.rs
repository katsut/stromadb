//! `lookup`: exact-value node resolution (external key → node id) over the query op and the MCP
//! tool — current vs as-of values, the type filter, the limit, the ABAC label mask, the batched
//! forms, per-fact labels, and reopen / compaction.

use serde_json::{Value, json};
use stromadb_store::{Db, mcp};

fn fresh(tag: &str) -> (std::path::PathBuf, Db) {
    let dir = std::env::temp_dir()
        .join(format!("stroma_lookup_test_{}_{}", tag, std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Issue\"}}\n",
        "{\"type_def\":{\"name\":\"Page\"}}\n",
        "{\"pred_def\":{\"name\":\"issue-key\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range_value\":\"text\"}}\n",
        "{\"pred_def\":{\"name\":\"title\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range_value\":\"text\"}}\n",
        "{\"pred_def\":{\"name\":\"priority\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range_value\":\"int\"}}\n",
        "{\"pred_def\":{\"name\":\"tag\",\"cardinality\":\"many\",\"domain\":\"Issue\",\"range_value\":\"text\"}}\n",
        "{\"node\":{\"id\":10,\"type\":\"Issue\",\"label\":0}}\n",
        "{\"node\":{\"id\":11,\"type\":\"Issue\",\"label\":3}}\n",
        "{\"node\":{\"id\":12,\"type\":\"Issue\",\"label\":0}}\n",
        // node 10 was renamed PROJ-1 → PROJ-9 at t=100
        "{\"fact\":{\"subject\":10,\"predicate\":\"issue-key\",\"object\":{\"text\":\"PROJ-1\"},\"valid_from\":10}}\n",
        "{\"fact\":{\"subject\":10,\"predicate\":\"issue-key\",\"object\":{\"text\":\"PROJ-9\"},\"valid_from\":100}}\n",
        "{\"fact\":{\"subject\":10,\"predicate\":\"title\",\"object\":{\"text\":\"Fix login\"}}}\n",
        "{\"fact\":{\"subject\":11,\"predicate\":\"issue-key\",\"object\":{\"text\":\"PROJ-2\"}}}\n",
        "{\"fact\":{\"subject\":12,\"predicate\":\"issue-key\",\"object\":{\"text\":\"PROJ-3\"}}}\n",
        // a page shares a key value with an issue: only the type filter tells them apart
        "{\"fact\":{\"subject\":20,\"predicate\":\"issue-key\",\"object\":{\"text\":\"PROJ-3\"}}}\n",
        "{\"fact\":{\"subject\":10,\"predicate\":\"priority\",\"object\":{\"int\":2}}}\n",
        "{\"fact\":{\"subject\":12,\"predicate\":\"priority\",\"object\":{\"int\":2}}}\n",
        "{\"fact\":{\"subject\":20,\"predicate\":\"priority\",\"object\":{\"int\":2}}}\n",
        // typed after its facts (the domain check applies to already-typed subjects only)
        "{\"node\":{\"id\":20,\"type\":\"Page\",\"label\":0}}\n",
    ))
    .unwrap();
    (dir, db)
}

fn ids(r: &Value) -> Vec<u64> {
    r["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_u64().unwrap())
        .collect()
}

#[test]
fn lookup_matches_the_current_value_exactly() {
    let (dir, db) = fresh("current");
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","value":"PROJ-9"}))
        .unwrap();
    assert_eq!(ids(&r), vec![10], "{r}");
    assert_eq!(r["nodes"][0]["type"], json!("Issue"));
    assert_eq!(r["nodes"][0]["display"], json!("Fix login"));
    assert_eq!(r["truncated"], json!(false));
    // the superseded key no longer matches the current value
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","value":"PROJ-1"}))
        .unwrap();
    assert!(ids(&r).is_empty(), "{r}");
    // exact, not substring / case-insensitive
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","value":"proj-9"}))
        .unwrap();
    assert!(ids(&r).is_empty(), "{r}");
    // unknown key → empty, not an error
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","value":"NOPE-1"}))
        .unwrap();
    assert!(ids(&r).is_empty() && r["truncated"] == json!(false), "{r}");
    // `equals` alias and the typed object form
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","equals":{"text":"PROJ-9"}}))
        .unwrap();
    assert_eq!(ids(&r), vec![10], "{r}");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn lookup_as_of_reads_the_value_in_effect() {
    let (dir, db) = fresh("asof");
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","value":"PROJ-1","valid_at":50}))
        .unwrap();
    assert_eq!(ids(&r), vec![10], "{r}");
    assert_eq!(r["valid_at"], json!(50));
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","value":"PROJ-9","valid_at":50}))
        .unwrap();
    assert!(ids(&r).is_empty(), "{r}");
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","value":"PROJ-9","valid_at":150}))
        .unwrap();
    assert_eq!(ids(&r), vec![10], "{r}");
    // before any version was valid
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","value":"PROJ-1","valid_at":5}))
        .unwrap();
    assert!(ids(&r).is_empty(), "{r}");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn lookup_type_filter_limit_and_mask() {
    let (dir, db) = fresh("filters");
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","value":"PROJ-3"}))
        .unwrap();
    assert_eq!(ids(&r), vec![12, 20], "{r}");
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","value":"PROJ-3","type":"Issue"}))
        .unwrap();
    assert_eq!(ids(&r), vec![12], "{r}");

    // int value (bare number), limit + truncated
    let r = db
        .query(&json!({"op":"lookup","predicate":"priority","value":2,"limit":2}))
        .unwrap();
    assert_eq!(ids(&r), vec![10, 12], "{r}");
    assert_eq!(r["truncated"], json!(true));
    // the limit is capped at 100
    let r = db
        .query(&json!({"op":"lookup","predicate":"priority","value":2,"limit":100000}))
        .unwrap();
    assert_eq!(ids(&r), vec![10, 12, 20], "{r}");
    assert_eq!(r["truncated"], json!(false));

    // ABAC: node 11 carries label 3; a mask without bit 3 does not see it
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","value":"PROJ-2","allowed_labels":1}))
        .unwrap();
    assert!(ids(&r).is_empty(), "{r}");
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","value":"PROJ-2","allowed_labels":8}))
        .unwrap();
    assert_eq!(ids(&r), vec![11], "{r}");

    // errors: unknown predicate / type, many-cardinality predicate, missing value
    let bad = [
        json!({"op":"lookup","predicate":"nope","value":"x"}),
        json!({"op":"lookup","predicate":"issue-key","value":"x","type":"Nope"}),
        json!({"op":"lookup","predicate":"tag","value":"x"}),
        json!({"op":"lookup","predicate":"issue-key"}),
    ];
    for req in bad {
        assert!(db.query(&req).is_err(), "expected an error for {req}");
    }
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

fn mcp_call(db: &Db, scope: &mcp::Scope, tool: &str, args: Value) -> (bool, String) {
    let msg = json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                     "params":{"name":tool,"arguments":args}});
    let resp = mcp::handle_message_scoped(db, &msg, scope).unwrap();
    let is_err = resp["result"]["isError"].as_bool().unwrap_or(false);
    let text = resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("")
        .to_string();
    (is_err, text)
}

#[test]
fn lookup_is_an_mcp_tool_under_the_scope_cap() {
    let (dir, db) = fresh("mcp");
    let list =
        mcp::handle_message(&db, &json!({"jsonrpc":"2.0","id":1,"method":"tools/list"})).unwrap();
    let tool = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "lookup")
        .expect("lookup tool listed")
        .clone();
    assert!(
        tool["description"]
            .as_str()
            .unwrap()
            .contains("external key"),
        "{tool}"
    );
    assert_eq!(
        tool["inputSchema"]["required"],
        json!(["predicate", "value"])
    );

    let all = mcp::Scope::default();
    let (err, text) = mcp_call(
        &db,
        &all,
        "lookup",
        json!({"predicate":"issue-key","value":"PROJ-2"}),
    );
    assert!(!err, "{text}");
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(ids(&v), vec![11], "{text}");

    // a capped token cannot widen itself to the labelled node
    let capped = mcp::Scope {
        allowed_labels: Some(1),
        ..Default::default()
    };
    let (err, text) = mcp_call(
        &db,
        &capped,
        "lookup",
        json!({"predicate":"issue-key","value":"PROJ-2","allowed_labels":u64::MAX}),
    );
    assert!(!err, "{text}");
    let v: Value = serde_json::from_str(&text).unwrap();
    assert!(ids(&v).is_empty(), "{text}");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

fn results(r: &Value) -> Vec<Vec<u64>> {
    r["results"].as_array().unwrap().iter().map(ids).collect()
}

#[test]
fn lookup_batches_values_and_instants_in_request_order() {
    let (dir, db) = fresh("batch");
    // `values`: many values under one top-level valid_at (or none = current)
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key",
                       "values":["PROJ-9","PROJ-1",{"text":"PROJ-3"},"NOPE-1"]}))
        .unwrap();
    assert_eq!(
        results(&r),
        vec![vec![10], vec![], vec![12, 20], vec![]],
        "{r}"
    );
    assert!(r["results"][0].get("valid_at").is_none(), "{r}");
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","valid_at":50,
                       "values":["PROJ-9","PROJ-1"]}))
        .unwrap();
    assert_eq!(results(&r), vec![vec![], vec![10]], "{r}");
    assert_eq!(r["results"][1]["valid_at"], json!(50));

    // `queries`: each item its own value and instant; the top-level valid_at is the default
    let r = db
        .query(
            &json!({"op":"lookup","predicate":"issue-key","valid_at":150,"queries":[
                {"value":"PROJ-1","valid_at":50},
                {"value":"PROJ-1"},
                {"equals":"PROJ-9"},
                {"value":"PROJ-1","valid_at":5},
            ]}),
        )
        .unwrap();
    assert_eq!(results(&r), vec![vec![10], vec![], vec![10], vec![]], "{r}");
    assert_eq!(r["results"][1]["valid_at"], json!(150));

    // each item is the single-form answer: type filter, limit and truncation apply per item
    let r = db
        .query(
            &json!({"op":"lookup","predicate":"issue-key","type":"Issue","limit":1,
                       "values":["PROJ-3","PROJ-9"]}),
        )
        .unwrap();
    assert_eq!(results(&r), vec![vec![12], vec![10]], "{r}");
    let single = db
        .query(&json!({"op":"lookup","predicate":"issue-key","type":"Issue","limit":1,"value":"PROJ-3"}))
        .unwrap();
    assert_eq!(r["results"][0], single);

    // the node mask applies to every item
    let r = db
        .query(
            &json!({"op":"lookup","predicate":"issue-key","allowed_labels":1,
                       "values":["PROJ-2","PROJ-9"]}),
        )
        .unwrap();
    assert_eq!(results(&r), vec![vec![], vec![10]], "{r}");

    let too_many: Vec<Value> = (0..1001).map(|i| json!(format!("K-{i}"))).collect();
    let bad = [
        json!({"op":"lookup","predicate":"issue-key","value":"PROJ-9","values":["PROJ-9"]}),
        json!({"op":"lookup","predicate":"issue-key","values":["PROJ-9"],"queries":[]}),
        json!({"op":"lookup","predicate":"issue-key","values":"PROJ-9"}),
        json!({"op":"lookup","predicate":"issue-key","queries":[{"valid_at":5}]}),
        json!({"op":"lookup","predicate":"issue-key","values":too_many}),
    ];
    for req in bad {
        assert!(db.query(&req).is_err(), "expected an error for {req}");
    }
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn lookup_never_matches_a_hidden_fact_in_either_form() {
    let (dir, db) = fresh("fact_label");
    // node 12 is re-keyed PROJ-3 → PROJ-7 at t=200 by a fact labeled 2
    db.ingest_str(
        "{\"fact\":{\"subject\":12,\"predicate\":\"issue-key\",\"object\":{\"text\":\"PROJ-7\"},\"valid_from\":200,\"label\":2}}\n",
    )
    .unwrap();
    let public = |req: Value| {
        let mut req = req;
        req["allowed_labels"] = json!(1);
        db.query(&req).unwrap()
    };
    // the hidden head gives way to the visible row: PROJ-3 is still current for this reader
    let r = public(json!({"op":"lookup","predicate":"issue-key","values":["PROJ-7","PROJ-3"]}));
    assert_eq!(results(&r), vec![vec![], vec![12, 20]], "{r}");
    let r = public(json!({"op":"lookup","predicate":"issue-key","value":"PROJ-7","valid_at":250}));
    assert!(ids(&r).is_empty(), "{r}");
    let r = db
        .query(&json!({"op":"lookup","predicate":"issue-key","queries":[
            {"value":"PROJ-7","valid_at":250},{"value":"PROJ-3","valid_at":250},{"value":"PROJ-3","valid_at":150}]}))
        .unwrap();
    assert_eq!(results(&r), vec![vec![12], vec![20], vec![12, 20]], "{r}");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn lookup_answers_the_same_across_reopen_and_compaction() {
    let (dir, db) = fresh("reopen");
    // more history after the fixture: a re-key and a close
    db.ingest_str(concat!(
        "{\"fact\":{\"subject\":11,\"predicate\":\"issue-key\",\"object\":{\"text\":\"PROJ-5\"},\"valid_from\":300}}\n",
        "{\"close\":{\"subject\":12,\"predicate\":\"issue-key\",\"valid_from\":400}}\n",
    ))
    .unwrap();
    let probe = json!({"op":"lookup","predicate":"issue-key","queries":[
        {"value":"PROJ-1","valid_at":50}, {"value":"PROJ-9"}, {"value":"PROJ-2"},
        {"value":"PROJ-2","valid_at":250}, {"value":"PROJ-5"}, {"value":"PROJ-3"},
        {"value":"PROJ-3","valid_at":350}, {"value":"PROJ-3","valid_at":450},
    ]});
    let before = db.query(&probe).unwrap();
    assert_eq!(
        results(&before),
        vec![
            vec![10],
            vec![10],
            vec![],
            vec![11],
            vec![11],
            vec![20],
            vec![12, 20],
            vec![20]
        ],
        "{before}"
    );
    drop(db);
    let db = Db::open(&dir).unwrap();
    assert_eq!(db.query(&probe).unwrap(), before, "reopen (WAL replay)");
    db.compact().unwrap();
    assert_eq!(db.query(&probe).unwrap(), before, "after compaction");
    drop(db);
    let db = Db::open(&dir).unwrap();
    assert_eq!(
        db.query(&probe).unwrap(),
        before,
        "reopen from the compaction snapshot"
    );
    drop(db);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}
