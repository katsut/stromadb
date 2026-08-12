//! Per-client scope: default provenance stamping (`ingest_str_as`), the label cap on reads, and
//! the read-only bit — over the direct API and through the shared MCP dispatch, so both the HTTP
//! transport and the stdio binary inherit the same behavior.

use serde_json::{Value, json};
use stromadb_store::{Db, mcp};

fn fresh(tag: &str) -> (std::path::PathBuf, Db) {
    let dir = std::env::temp_dir()
        .join(format!("stroma_scope_test_{}_{}", tag, std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Doc\"}}\n",
        "{\"pred_def\":{\"name\":\"title\",\"cardinality\":\"one\",\"domain\":\"Doc\",\"range_value\":\"text\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Doc\",\"label\":0}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Doc\",\"label\":3}}\n",
        "{\"fact\":{\"subject\":2,\"predicate\":\"title\",\"object\":{\"text\":\"secret\"}}}\n",
    ))
    .unwrap();
    (dir, db)
}

#[test]
fn default_source_stamps_unsourced_writes_only() {
    let (dir, db) = fresh("src");

    // un-sourced line → stamped with the default; explicit source → wins
    db.ingest_str_as(
        concat!(
            "{\"fact\":{\"subject\":1,\"predicate\":\"title\",\"object\":{\"text\":\"a\"}}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"title\",\"object\":{\"text\":\"b\"},\"valid_from\":10,\"source\":\"hr\"}}\n",
        ),
        Some("support-agent"),
    )
    .unwrap();
    let r = db
        .query(&json!({"op":"point","subject":1,"predicate":"title"}))
        .unwrap();
    assert_eq!(r["provenance"], json!("hr"), "explicit source wins: {r}");
    // supersede without a source through a named scope → the token name is the provenance
    db.ingest_str_as(
        "{\"fact\":{\"subject\":1,\"predicate\":\"title\",\"object\":{\"text\":\"c\"},\"valid_from\":20}}\n",
        Some("support-agent"),
    )
    .unwrap();
    let r = db
        .query(&json!({"op":"point","subject":1,"predicate":"title"}))
        .unwrap();
    assert_eq!(r["provenance"], json!("support-agent"));
    // a plain ingest afterwards is unaffected (the default does not stick)
    db.ingest_str(
        "{\"fact\":{\"subject\":1,\"predicate\":\"title\",\"object\":{\"text\":\"d\"},\"valid_from\":30}}\n",
    )
    .unwrap();
    let r = db
        .query(&json!({"op":"point","subject":1,"predicate":"title"}))
        .unwrap();
    assert!(r.get("provenance").is_none(), "no default outside _as: {r}");

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
fn scope_caps_reads_and_blocks_readonly_writes_over_mcp() {
    let (dir, db) = fresh("mcp");

    // unrestricted: node 2 (label 3) is visible to a search-free read like `point`
    let all = mcp::Scope::default();
    let (err, text) = mcp_call(&db, &all, "point", json!({"subject":2,"predicate":"title"}));
    assert!(!err && text.contains("secret"), "{text}");

    // label-capped to bit 0 only: label-3 subjects vanish from label-scoped reads
    let capped = mcp::Scope {
        allowed_labels: Some(0b1),
        ..Default::default()
    };
    let (err, text) = mcp_call(
        &db,
        &capped,
        "point",
        json!({"subject":2,"predicate":"title"}),
    );
    assert!(!err, "{text}");
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        v["denied"],
        json!(true),
        "label-capped point must deny the subject: {v}"
    );
    let (err, text) = mcp_call(
        &db,
        &capped,
        "timeline",
        json!({"subject":2,"hops":["title"]}),
    );
    assert!(!err, "{text}");
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        v["segments"].as_array().unwrap().len(),
        0,
        "label-capped timeline must hide label-3 history: {v}"
    );

    // a capped request cannot widen itself: asking for all labels still intersects to bit 0
    let (err, text) = mcp_call(
        &db,
        &capped,
        "timeline",
        json!({"subject":2,"hops":["title"],"allowed_labels":4294967295u32}),
    );
    assert!(!err, "{text}");
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        v["segments"].as_array().unwrap().len(),
        0,
        "request must not widen the cap: {v}"
    );

    // read-only: ingest is refused with a clear message; nothing lands
    let ro = mcp::Scope {
        read_only: true,
        ..Default::default()
    };
    let head_before = db.stats()["facts"]["durable_head"].clone();
    let (err, text) = mcp_call(
        &db,
        &ro,
        "ingest",
        json!({"jsonl":"{\"fact\":{\"subject\":1,\"predicate\":\"title\",\"object\":{\"text\":\"x\"}}}"}),
    );
    assert!(err && text.contains("read-only"), "{text}");
    assert_eq!(db.stats()["facts"]["durable_head"], head_before);

    // a named writable scope stamps provenance through MCP ingest
    let named = mcp::Scope {
        default_source: Some("agent-a".into()),
        ..Default::default()
    };
    let (err, text) = mcp_call(
        &db,
        &named,
        "ingest",
        json!({"jsonl":"{\"fact\":{\"subject\":1,\"predicate\":\"title\",\"object\":{\"text\":\"y\"},\"valid_from\":99}}"}),
    );
    assert!(!err, "{text}");
    let r = db
        .query(&json!({"op":"point","subject":1,"predicate":"title"}))
        .unwrap();
    assert_eq!(r["provenance"], json!("agent-a"));

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}
