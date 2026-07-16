//! End-to-end completeness op: ingest a small typed graph and assert the "expected-but-absent" set —
//! for each node of a type, the request's required predicates that currently have no value. Covers a
//! one-cardinality (`assigned-to`) and a many-cardinality (`reviewers`) required predicate plus
//! post-authz hiding. Deterministic (sorted by node id, missing list in request order); no reasoner.

use std::collections::BTreeMap;

use serde_json::json;
use stromadb_store::Db;

// A tiny issue-tracker graph. `status`/`assigned-to`/`reviewers` are the required predicates the op
// checks for. Issue 1004 is hidden behind sensitivity label 1 (the post-authz case).
//   1001: status + assigned-to + reviewers   → complete (omitted)
//   1002: status only                        → missing assigned-to, reviewers
//   1003: assigned-to + reviewers, no status → missing status
//   1004: status only, label 1               → missing assigned-to, reviewers (hidden under label 0)
const FIXTURE: &str = r#"
{"type_def":{"name":"Person"}}
{"type_def":{"name":"Issue"}}
{"pred_def":{"name":"title","cardinality":"one","domain":"Issue","range_value":"text"}}
{"pred_def":{"name":"status","cardinality":"one","domain":"Issue","range_value":"text"}}
{"pred_def":{"name":"assigned-to","cardinality":"one","domain":"Issue","range":"Person"}}
{"pred_def":{"name":"reviewers","cardinality":"many","domain":"Issue","range":"Person"}}
{"node":{"id":10,"type":"Person","label":0}}
{"node":{"id":11,"type":"Person","label":0}}
{"node":{"id":1001,"type":"Issue","label":0}}
{"node":{"id":1002,"type":"Issue","label":0}}
{"node":{"id":1003,"type":"Issue","label":0}}
{"node":{"id":1004,"type":"Issue","label":1}}
{"fact":{"subject":1001,"predicate":"status","object":{"text":"open"}}}
{"fact":{"subject":1001,"predicate":"assigned-to","object":{"node":10}}}
{"fact":{"subject":1001,"predicate":"reviewers","object":{"node":11}}}
{"fact":{"subject":1002,"predicate":"status","object":{"text":"open"}}}
{"fact":{"subject":1003,"predicate":"assigned-to","object":{"node":10}}}
{"fact":{"subject":1003,"predicate":"reviewers","object":{"node":10}}}
{"fact":{"subject":1003,"predicate":"reviewers","object":{"node":11}}}
{"fact":{"subject":1004,"predicate":"status","object":{"text":"open"}}}
"#;

fn incomplete_map(r: &serde_json::Value) -> BTreeMap<u64, Vec<String>> {
    r["incomplete"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| {
            let node = i["node"].as_u64().unwrap();
            let missing = i["missing"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| m.as_str().unwrap().to_string())
                .collect();
            (node, missing)
        })
        .collect()
}

#[test]
fn completeness_over_fixture() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_completeness_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(FIXTURE).unwrap();

    // reopen so the read is served from the replayed WAL (the assertions survive the durability
    // round-trip).
    let db = Db::open(&dir).unwrap();

    let req = json!({
        "op": "completeness",
        "type": "Issue",
        "required": ["status", "assigned-to", "reviewers"]
    });

    // default authz (all labels): every incomplete issue, sorted by node id, missing in request order.
    let got = incomplete_map(&db.query(&req).unwrap());
    let want: BTreeMap<u64, Vec<String>> = [
        (1002u64, vec!["assigned-to", "reviewers"]),
        (1003, vec!["status"]),
        (1004, vec!["assigned-to", "reviewers"]),
    ]
    .into_iter()
    .map(|(k, v)| (k, v.into_iter().map(str::to_string).collect()))
    .collect();
    assert_eq!(got, want);

    // post-authz: a principal allowed only label 0 must not see the label-1 issue 1004.
    let mut scoped = req.clone();
    scoped["allowed_labels"] = json!(0b1);
    let got_scoped = incomplete_map(&db.query(&scoped).unwrap());
    assert!(!got_scoped.contains_key(&1004));
    assert_eq!(
        got_scoped.keys().copied().collect::<Vec<_>>(),
        vec![1002, 1003]
    );

    // an unknown predicate name is a clear error, not a panic.
    let bad = json!({
        "op": "completeness",
        "type": "Issue",
        "required": ["status", "no-such-predicate"]
    });
    let err = db.query(&bad).unwrap_err();
    assert!(err.contains("no-such-predicate"), "unexpected error: {err}");

    // an unknown type is a clear error too.
    let bad_type = json!({ "op": "completeness", "type": "Nope", "required": ["status"] });
    let err = db.query(&bad_type).unwrap_err();
    assert!(err.contains("Nope"), "unexpected error: {err}");

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}
