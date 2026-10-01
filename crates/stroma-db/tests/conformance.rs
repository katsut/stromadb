//! End-to-end conformance op: ingest the backlog release-approval fixture and evaluate a declared
//! rule into deterministic per-subject verdicts (OK / ABSENT / MISMATCH / NOT_APPLICABLE), including
//! the as-of hop (a manager that changes over valid-time: an approval before the change is OK, after
//! it is a MISMATCH). The rule composes existing read primitives — no reasoner.

use std::collections::BTreeMap;

use serde_json::json;
use stromadb_store::{Db, mcp};

// The fixture schema + data (backlog release-approval). `manager-of` for the Platform department (1)
// transfers from Alice(10) to Carol(12) at valid-time 5000, which is what the as-of hop turns on.
const FIXTURE: &str = r#"
{"type_def":{"name":"Person"}}
{"type_def":{"name":"Department"}}
{"type_def":{"name":"Project"}}
{"type_def":{"name":"Issue"}}
{"pred_def":{"name":"name","cardinality":"one","domain":"Person","range_value":"text"}}
{"pred_def":{"name":"dept-name","cardinality":"one","domain":"Department","range_value":"text"}}
{"pred_def":{"name":"project-name","cardinality":"one","domain":"Project","range_value":"text"}}
{"pred_def":{"name":"title","cardinality":"one","domain":"Issue","range_value":"text"}}
{"pred_def":{"name":"member-of","cardinality":"one","domain":"Person","range":"Department"}}
{"pred_def":{"name":"manager-of","cardinality":"one","domain":"Department","range":"Person"}}
{"pred_def":{"name":"project-dept","cardinality":"one","domain":"Project","range":"Department"}}
{"pred_def":{"name":"in-project","cardinality":"one","domain":"Issue","range":"Project"}}
{"pred_def":{"name":"assigned-to","cardinality":"one","domain":"Issue","range":"Person"}}
{"pred_def":{"name":"issue-type","cardinality":"one","domain":"Issue","range_value":"text"}}
{"pred_def":{"name":"status","cardinality":"one","domain":"Issue","range_value":"text"}}
{"pred_def":{"name":"approved-by","cardinality":"one","domain":"Issue","range":"Person"}}
{"pred_def":{"name":"approved-at","cardinality":"one","domain":"Issue","range_value":"int"}}
{"node":{"id":1,"type":"Department","label":0}}
{"node":{"id":2,"type":"Department","label":0}}
{"node":{"id":10,"type":"Person","label":0}}
{"node":{"id":11,"type":"Person","label":0}}
{"node":{"id":12,"type":"Person","label":0}}
{"node":{"id":101,"type":"Person","label":0}}
{"node":{"id":102,"type":"Person","label":0}}
{"node":{"id":201,"type":"Person","label":0}}
{"node":{"id":202,"type":"Person","label":0}}
{"node":{"id":301,"type":"Project","label":0}}
{"node":{"id":302,"type":"Project","label":0}}
{"node":{"id":1001,"type":"Issue","label":0}}
{"node":{"id":1002,"type":"Issue","label":0}}
{"node":{"id":1003,"type":"Issue","label":0}}
{"node":{"id":1004,"type":"Issue","label":0}}
{"node":{"id":1005,"type":"Issue","label":0}}
{"node":{"id":1006,"type":"Issue","label":0}}
{"fact":{"subject":1,"predicate":"dept-name","object":{"text":"Platform"}}}
{"fact":{"subject":2,"predicate":"dept-name","object":{"text":"Product"}}}
{"fact":{"subject":10,"predicate":"name","object":{"text":"Alice"}}}
{"fact":{"subject":11,"predicate":"name","object":{"text":"Bob"}}}
{"fact":{"subject":12,"predicate":"name","object":{"text":"Carol"}}}
{"fact":{"subject":101,"predicate":"name","object":{"text":"Dave"}}}
{"fact":{"subject":102,"predicate":"name","object":{"text":"Erin"}}}
{"fact":{"subject":201,"predicate":"name","object":{"text":"Frank"}}}
{"fact":{"subject":202,"predicate":"name","object":{"text":"Grace"}}}
{"fact":{"subject":301,"predicate":"project-name","object":{"text":"Apollo"}}}
{"fact":{"subject":302,"predicate":"project-name","object":{"text":"Beacon"}}}
{"fact":{"subject":101,"predicate":"member-of","object":{"node":1},"valid_from":1000}}
{"fact":{"subject":102,"predicate":"member-of","object":{"node":1},"valid_from":1000}}
{"fact":{"subject":201,"predicate":"member-of","object":{"node":2},"valid_from":1000}}
{"fact":{"subject":202,"predicate":"member-of","object":{"node":2},"valid_from":1000}}
{"fact":{"subject":10,"predicate":"member-of","object":{"node":1},"valid_from":1000}}
{"fact":{"subject":11,"predicate":"member-of","object":{"node":2},"valid_from":1000}}
{"fact":{"subject":12,"predicate":"member-of","object":{"node":1},"valid_from":1000}}
{"fact":{"subject":1,"predicate":"manager-of","object":{"node":10},"valid_from":1000}}
{"fact":{"subject":2,"predicate":"manager-of","object":{"node":11},"valid_from":1000}}
{"fact":{"subject":1,"predicate":"manager-of","object":{"node":12},"valid_from":5000}}
{"fact":{"subject":301,"predicate":"project-dept","object":{"node":1}}}
{"fact":{"subject":302,"predicate":"project-dept","object":{"node":2}}}
{"fact":{"subject":1001,"predicate":"title","object":{"text":"Apollo v1.2 release"}}}
{"fact":{"subject":1001,"predicate":"in-project","object":{"node":301}}}
{"fact":{"subject":1001,"predicate":"assigned-to","object":{"node":101}}}
{"fact":{"subject":1001,"predicate":"issue-type","object":{"text":"release"}}}
{"fact":{"subject":1001,"predicate":"status","object":{"text":"open"},"valid_from":1100}}
{"fact":{"subject":1001,"predicate":"approved-by","object":{"node":10},"valid_from":1200}}
{"fact":{"subject":1001,"predicate":"approved-at","object":{"int":1200}}}
{"fact":{"subject":1001,"predicate":"status","object":{"text":"released"},"valid_from":1300}}
{"fact":{"subject":1002,"predicate":"title","object":{"text":"Beacon hotfix release"}}}
{"fact":{"subject":1002,"predicate":"in-project","object":{"node":302}}}
{"fact":{"subject":1002,"predicate":"assigned-to","object":{"node":201}}}
{"fact":{"subject":1002,"predicate":"issue-type","object":{"text":"release"}}}
{"fact":{"subject":1002,"predicate":"status","object":{"text":"open"},"valid_from":1400}}
{"fact":{"subject":1002,"predicate":"approved-by","object":{"node":11},"valid_from":1500}}
{"fact":{"subject":1002,"predicate":"approved-at","object":{"int":1500}}}
{"fact":{"subject":1002,"predicate":"status","object":{"text":"released"},"valid_from":1600}}
{"fact":{"subject":1003,"predicate":"title","object":{"text":"Apollo v1.3 release"}}}
{"fact":{"subject":1003,"predicate":"in-project","object":{"node":301}}}
{"fact":{"subject":1003,"predicate":"assigned-to","object":{"node":102}}}
{"fact":{"subject":1003,"predicate":"issue-type","object":{"text":"release"}}}
{"fact":{"subject":1003,"predicate":"status","object":{"text":"open"},"valid_from":1700}}
{"fact":{"subject":1003,"predicate":"status","object":{"text":"released"},"valid_from":1900}}
{"fact":{"subject":1004,"predicate":"title","object":{"text":"Beacon v2 release"}}}
{"fact":{"subject":1004,"predicate":"in-project","object":{"node":302}}}
{"fact":{"subject":1004,"predicate":"assigned-to","object":{"node":202}}}
{"fact":{"subject":1004,"predicate":"issue-type","object":{"text":"release"}}}
{"fact":{"subject":1004,"predicate":"status","object":{"text":"open"},"valid_from":2000}}
{"fact":{"subject":1004,"predicate":"approved-by","object":{"node":101},"valid_from":2100}}
{"fact":{"subject":1004,"predicate":"approved-at","object":{"int":2100}}}
{"fact":{"subject":1004,"predicate":"status","object":{"text":"released"},"valid_from":2200}}
{"fact":{"subject":1005,"predicate":"title","object":{"text":"Apollo v1.4 release"}}}
{"fact":{"subject":1005,"predicate":"in-project","object":{"node":301}}}
{"fact":{"subject":1005,"predicate":"assigned-to","object":{"node":101}}}
{"fact":{"subject":1005,"predicate":"issue-type","object":{"text":"release"}}}
{"fact":{"subject":1005,"predicate":"status","object":{"text":"open"},"valid_from":5500}}
{"fact":{"subject":1005,"predicate":"approved-by","object":{"node":10},"valid_from":6000}}
{"fact":{"subject":1005,"predicate":"approved-at","object":{"int":6000}}}
{"fact":{"subject":1005,"predicate":"status","object":{"text":"released"},"valid_from":6100}}
{"fact":{"subject":1006,"predicate":"title","object":{"text":"Apollo internal cleanup"}}}
{"fact":{"subject":1006,"predicate":"in-project","object":{"node":301}}}
{"fact":{"subject":1006,"predicate":"assigned-to","object":{"node":101}}}
{"fact":{"subject":1006,"predicate":"issue-type","object":{"text":"task"}}}
{"fact":{"subject":1006,"predicate":"status","object":{"text":"open"},"valid_from":6200}}
{"fact":{"subject":1006,"predicate":"status","object":{"text":"released"},"valid_from":6300}}
{"node":{"id":1007,"type":"Issue","label":0}}
{"fact":{"subject":1007,"predicate":"title","object":{"text":"Apollo v1.5 release"}}}
{"fact":{"subject":1007,"predicate":"in-project","object":{"node":301}}}
{"fact":{"subject":1007,"predicate":"assigned-to","object":{"node":102}}}
{"fact":{"subject":1007,"predicate":"issue-type","object":{"text":"release"}}}
{"fact":{"subject":1007,"predicate":"status","object":{"text":"open"},"valid_from":5300}}
{"fact":{"subject":1007,"predicate":"approved-by","object":{"node":12},"valid_from":5500}}
{"fact":{"subject":1007,"predicate":"approved-at","object":{"int":5500}}}
{"fact":{"subject":1007,"predicate":"status","object":{"text":"released"},"valid_from":5600}}
{"node":{"id":1008,"type":"Issue","label":0}}
{"fact":{"subject":1008,"predicate":"title","object":{"text":"Apollo v1.1 late-release"}}}
{"fact":{"subject":1008,"predicate":"in-project","object":{"node":301}}}
{"fact":{"subject":1008,"predicate":"assigned-to","object":{"node":101}}}
{"fact":{"subject":1008,"predicate":"issue-type","object":{"text":"release"}}}
{"fact":{"subject":1008,"predicate":"status","object":{"text":"open"},"valid_from":1050}}
{"fact":{"subject":1008,"predicate":"approved-by","object":{"node":10},"valid_from":1200}}
{"fact":{"subject":1008,"predicate":"approved-at","object":{"int":1200}}}
{"fact":{"subject":1008,"predicate":"status","object":{"text":"released"},"valid_from":6000}}
"#;

// The rule body shared by the inline op and the stored `rule_def` (identical evaluation semantics).
fn rule_body() -> serde_json::Value {
    json!({
        "subject_type": "Issue",
        "scope":     { "predicate": "issue-type", "equals": "release" },
        "required":  { "hops": [
            { "predicate": "assigned-to" },
            { "predicate": "member-of" },
            { "predicate": "manager-of", "as_of": "approved-at" }
        ] },
        "actual":      "approved-by",
        "absent_when": { "predicate": "status", "equals": "released" }
    })
}

fn rule() -> serde_json::Value {
    json!({ "op": "conformance", "rule": rule_body() })
}

fn verdict_map(r: &serde_json::Value) -> BTreeMap<u64, String> {
    r["verdicts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            (
                v["subject"].as_u64().unwrap(),
                v["verdict"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[test]
fn conformance_verdicts_over_fixture() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_conformance_test_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(FIXTURE).unwrap();

    // reopen so the read is served from the replayed WAL — the as-of hop depends on the manager-of
    // valid-time history surviving the durability round-trip.
    drop(db); // release the directory lock
    let db = Db::open(&dir).unwrap();

    let r = db.query(&rule()).unwrap();
    let got: BTreeMap<u64, String> = r["verdicts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            (
                v["subject"].as_u64().unwrap(),
                v["verdict"].as_str().unwrap().to_string(),
            )
        })
        .collect();

    let want: BTreeMap<u64, String> = [
        (1001, "OK"),
        (1002, "OK"),
        (1003, "ABSENT"),
        (1004, "MISMATCH"),
        (1005, "MISMATCH"),
        (1006, "NOT_APPLICABLE"),
        (1007, "OK"),
        (1008, "OK"),
    ]
    .into_iter()
    .map(|(k, v)| (k, v.to_string()))
    .collect();
    assert_eq!(got, want);

    // spot-check the as-of hop wiring: 1005 (approved after the 5000 transfer) is anchored at 6000
    // and its required manager is Carol(12), not the approver Alice(10); 1008 anchors at approval
    // time 1200 (before the transfer) and is OK even though it released after it.
    let verdicts = r["verdicts"].as_array().unwrap();
    let v1005 = verdicts.iter().find(|v| v["subject"] == 1005).unwrap();
    assert_eq!(v1005["as_of"], json!(6000));
    assert_eq!(v1005["required"], json!({ "node": 12 }));
    assert_eq!(v1005["actual"], json!({ "node": 10 }));
    let v1008 = verdicts.iter().find(|v| v["subject"] == 1008).unwrap();
    assert_eq!(v1008["as_of"], json!(1200));

    // mismatch sub-classification: 1005 approver (Alice) was the dept manager before the transfer but
    // not at approval time → stale; 1004 approver (Dave) was never a manager → wrong. OK carries no kind.
    assert_eq!(v1005["kind"], json!("stale"));
    let v1004 = verdicts.iter().find(|v| v["subject"] == 1004).unwrap();
    assert_eq!(v1004["kind"], json!("wrong"));
    assert_eq!(v1008["kind"], json!(null));

    // 1003 is an absence: no actual, and required could not be derived (no approval time to anchor).
    let v1003 = verdicts.iter().find(|v| v["subject"] == 1003).unwrap();
    assert_eq!(v1003["actual"], json!(null));

    // unknown predicate names are a clear error, not a panic.
    let bad = json!({
        "op": "conformance",
        "rule": {
            "subject_type": "Issue",
            "required": { "hops": [ { "predicate": "no-such-predicate" } ] },
            "actual": "approved-by"
        }
    });
    let err = db.query(&bad).unwrap_err();
    assert!(err.contains("no-such-predicate"), "unexpected error: {err}");

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

// The "author the rule once, evaluate it by name" boundary: declare the release-approval rule once as
// a `rule_def`, then evaluate it by `rule_name` — same verdicts as the inline rule, and still so after
// a reopen (the rule survives via rules.jsonl replay). An unknown `rule_name` is a clear error.
#[test]
fn conformance_by_stored_rule_name() {
    let dir = std::env::temp_dir()
        .join(format!(
            "stroma_conformance_named_test_{}",
            std::process::id()
        ))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(FIXTURE).unwrap();

    // declare the rule once, by name (durably appended to rules.jsonl).
    let rule_def = json!({ "rule_def": { "name": "release-approval", "rule": rule_body() } });
    db.ingest_str(&rule_def.to_string()).unwrap();

    let by_name = json!({ "op": "conformance", "rule_name": "release-approval" });

    // evaluating by name yields exactly the inline verdicts.
    let inline = verdict_map(&db.query(&rule()).unwrap());
    let named = verdict_map(&db.query(&by_name).unwrap());
    assert_eq!(named, inline);

    // reopen: the stored rule is replayed from rules.jsonl and still evaluates by name.
    drop(db); // release the directory lock
    let db = Db::open(&dir).unwrap();
    let named_after_reopen = verdict_map(&db.query(&by_name).unwrap());
    assert_eq!(named_after_reopen, inline);

    // an unknown rule name is a clear error, not a panic.
    let err = db
        .query(&json!({ "op": "conformance", "rule_name": "no-such-rule" }))
        .unwrap_err();
    assert!(err.contains("no-such-rule"), "unexpected error: {err}");

    // neither rule nor rule_name → a clear error.
    let err = db.query(&json!({ "op": "conformance" })).unwrap_err();
    assert!(
        err.contains("rule") && err.contains("rule_name"),
        "unexpected error: {err}"
    );

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

// Live maintenance end-to-end: watch a stored rule, write, poll the diffs — the maintained map
// stays equal to the one-shot op, changes come out as old→new verdict pairs (including the
// one-upstream-write cascade through the as-of hop), and the watch is invalidated by a rule
// re-declaration and by a reopen (it is in-memory by design).
#[test]
fn conformance_watch_streams_verdict_diffs() {
    let dir = std::env::temp_dir()
        .join(format!(
            "stroma_conformance_live_test_{}",
            std::process::id()
        ))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(FIXTURE).unwrap();
    let rule_def = json!({ "rule_def": { "name": "release-approval", "rule": rule_body() } });
    db.ingest_str(&rule_def.to_string()).unwrap();

    // only stored rules can be watched
    let err = db
        .query(&json!({"op":"conformance_watch","rule_name":"no-such"}))
        .unwrap_err();
    assert!(err.contains("no-such"), "unexpected: {err}");

    // watch: full verdicts + cursor, identical to the one-shot op
    let w = db
        .query(&json!({"op":"conformance_watch","rule_name":"release-approval"}))
        .unwrap();
    let baseline = verdict_map(&db.query(&rule()).unwrap());
    assert_eq!(verdict_map(&w), baseline);
    let cursor = w["cursor"].as_u64().unwrap();

    // nothing changed yet
    let c = db
        .query(&json!({"op":"conformance_changes","rule_name":"release-approval","cursor":cursor}))
        .unwrap();
    assert_eq!(c["changes"], json!([]));

    // an approval lands on the ABSENT issue (1003, anchored at 1200 → Alice is the manager then)
    db.ingest_str(concat!(
        "{\"fact\":{\"subject\":1003,\"predicate\":\"approved-by\",\"object\":{\"node\":10},\"valid_from\":1200}}\n",
        "{\"fact\":{\"subject\":1003,\"predicate\":\"approved-at\",\"object\":{\"int\":1200}}}\n",
    ))
    .unwrap();
    let c = db
        .query(&json!({"op":"conformance_changes","rule_name":"release-approval","cursor":cursor}))
        .unwrap();
    let changes = c["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0]["subject"], json!(1003));
    assert_eq!(changes[0]["old"]["verdict"], json!("ABSENT"));
    assert_eq!(changes[0]["new"]["verdict"], json!("OK"));
    let cursor = c["cursor"].as_u64().unwrap();

    // ONE upstream write cascades: a late-arriving manager transfer of the Platform department at
    // valid-time 1100 re-decides every issue anchored in [1100, 5000) — 1001, 1003, 1008 flip
    // OK → MISMATCH (stale: Alice held the role, but not as-of their anchors any more), while
    // anchors ≥ 5000 (1005, 1007) and the other department's issues stay put.
    db.ingest_str(
        "{\"fact\":{\"subject\":1,\"predicate\":\"manager-of\",\"object\":{\"node\":102},\"valid_from\":1100}}\n",
    )
    .unwrap();
    let c = db
        .query(&json!({"op":"conformance_changes","rule_name":"release-approval","cursor":cursor}))
        .unwrap();
    let mut flipped: Vec<u64> = c["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["subject"].as_u64().unwrap())
        .collect();
    flipped.sort_unstable();
    assert_eq!(flipped, vec![1001, 1003, 1008]);
    for d in c["changes"].as_array().unwrap() {
        assert_eq!(d["old"]["verdict"], json!("OK"));
        assert_eq!(d["new"]["verdict"], json!("MISMATCH"));
        assert_eq!(d["new"]["kind"], json!("stale"));
    }
    let cursor = c["cursor"].as_u64().unwrap();

    // the maintained map still equals a full one-shot evaluation
    let w = db
        .query(&json!({"op":"conformance_watch","rule_name":"release-approval"}))
        .unwrap();
    assert_eq!(verdict_map(&w), verdict_map(&db.query(&rule()).unwrap()));

    // post-authz on the read side: hide 1001 behind label 3 — a masked watcher no longer sees it
    db.ingest_str("{\"node\":{\"id\":1001,\"label\":3}}\n")
        .unwrap();
    let w = db
        .query(&json!({"op":"conformance_watch","rule_name":"release-approval","allowed_labels":1}))
        .unwrap();
    assert!(
        !verdict_map(&w).contains_key(&1001),
        "label-3 subject must be hidden from a label-0 watcher"
    );

    // re-declaring the rule invalidates the watch (the watcher re-registers against the new rule)
    db.ingest_str(&rule_def.to_string()).unwrap();
    let err = db
        .query(&json!({"op":"conformance_changes","rule_name":"release-approval","cursor":cursor}))
        .unwrap_err();
    assert!(err.contains("not watched"), "unexpected: {err}");

    // the watch is in-memory: gone after a reopen, and re-watchable
    drop(db); // release the directory lock
    let db = Db::open(&dir).unwrap();
    let err = db
        .query(&json!({"op":"conformance_changes","rule_name":"release-approval","cursor":cursor}))
        .unwrap_err();
    assert!(err.contains("not watched"), "unexpected: {err}");
    let w = db
        .query(&json!({"op":"conformance_watch","rule_name":"release-approval"}))
        .unwrap();
    assert_eq!(verdict_map(&w), verdict_map(&db.query(&rule()).unwrap()));

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

// The two rule-expressiveness extensions end-to-end through the JSON boundary: a node-valued scope
// (`equals: {"node": N}` — the documented object form) and `distinct_from` (a must-differ derived
// path, e.g. a self-approval ban), including the stored `rule_def` replay of the new field.
#[test]
fn node_scope_and_distinct_from_via_json() {
    let dir = std::env::temp_dir()
        .join(format!(
            "stroma_conformance_distinct_test_{}",
            std::process::id()
        ))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(FIXTURE).unwrap();
    // one extra issue: a Beacon release its own assignee approved (the self-approval violation).
    db.ingest_str(concat!(
        "{\"node\":{\"id\":1009,\"type\":\"Issue\",\"label\":0}}\n",
        "{\"fact\":{\"subject\":1009,\"predicate\":\"in-project\",\"object\":{\"node\":302}}}\n",
        "{\"fact\":{\"subject\":1009,\"predicate\":\"assigned-to\",\"object\":{\"node\":202}}}\n",
        "{\"fact\":{\"subject\":1009,\"predicate\":\"issue-type\",\"object\":{\"text\":\"release\"}}}\n",
        "{\"fact\":{\"subject\":1009,\"predicate\":\"approved-by\",\"object\":{\"node\":202},\"valid_from\":2400}}\n",
        "{\"fact\":{\"subject\":1009,\"predicate\":\"approved-at\",\"object\":{\"int\":2400}}}\n",
        "{\"fact\":{\"subject\":1009,\"predicate\":\"status\",\"object\":{\"text\":\"released\"},\"valid_from\":2500}}\n",
    ))
    .unwrap();

    // node-valued scope: only Beacon (project 302) issues are judged; everything else is out.
    let scoped = json!({ "op": "conformance", "rule": {
        "subject_type": "Issue",
        "scope":     { "predicate": "in-project", "equals": { "node": 302 } },
        "required":  { "hops": [
            { "predicate": "assigned-to" },
            { "predicate": "member-of" },
            { "predicate": "manager-of", "as_of": "approved-at" }
        ] },
        "actual":      "approved-by",
        "absent_when": { "predicate": "status", "equals": "released" }
    }});
    let got = verdict_map(&db.query(&scoped).unwrap());
    assert_eq!(got[&1002], "OK"); // approved by Beacon's manager
    assert_eq!(got[&1004], "MISMATCH"); // approved by a non-manager
    assert_eq!(got[&1009], "MISMATCH"); // approved by the assignee (not the manager)
    for out_of_scope in [1001u64, 1003, 1005, 1006, 1007, 1008] {
        assert_eq!(got[&out_of_scope], "NOT_APPLICABLE", "issue {out_of_scope}");
    }

    // distinct_from without required: the only declaration is "not approved by the assignee".
    let ban = json!({
        "subject_type": "Issue",
        "distinct_from": { "hops": [ { "predicate": "assigned-to" } ] },
        "actual":        "approved-by",
        "absent_when":   { "predicate": "status", "equals": "released" }
    });
    let r = db
        .query(&json!({ "op": "conformance", "rule": ban }))
        .unwrap();
    let got = verdict_map(&r);
    assert_eq!(got[&1009], "MISMATCH"); // the self-approval
    for fine in [1001u64, 1002, 1004, 1005, 1007, 1008] {
        assert_eq!(got[&fine], "OK", "issue {fine}"); // approved, by someone else
    }
    assert_eq!(got[&1003], "ABSENT"); // released with no approval still gaps
    let v1009 = r["verdicts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["subject"] == 1009)
        .unwrap();
    assert_eq!(v1009["kind"], json!("wrong")); // a collision that holds now is never stale
    assert_eq!(v1009["distinct"], json!({ "node": 202 }));
    assert_eq!(v1009["required"], json!(null)); // no equality expectation was declared

    // the new field survives the stored-rule path: declare by rule_def, reopen, evaluate by name.
    let rule_def = json!({ "rule_def": { "name": "self-approval-ban", "rule": ban } });
    db.ingest_str(&rule_def.to_string()).unwrap();
    drop(db); // release the directory lock
    let db = Db::open(&dir).unwrap();
    let named = verdict_map(
        &db.query(&json!({ "op": "conformance", "rule_name": "self-approval-ban" }))
            .unwrap(),
    );
    assert_eq!(named, got);

    // a rule declaring neither path is rejected with a clear error.
    let err = db
        .query(&json!({ "op": "conformance", "rule": {
            "subject_type": "Issue", "actual": "approved-by"
        }}))
        .unwrap_err();
    assert!(
        err.contains("required") && err.contains("distinct_from"),
        "unexpected error: {err}"
    );

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

fn fixture_db(tag: &str) -> (std::path::PathBuf, Db) {
    let dir = std::env::temp_dir()
        .join(format!(
            "stroma_conformance_{}_test_{}",
            tag,
            std::process::id()
        ))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(FIXTURE).unwrap();
    (dir, db)
}

fn subjects_of(r: &serde_json::Value) -> Vec<u64> {
    r["verdicts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["subject"].as_u64().unwrap())
        .collect()
}

// Subject-scoped evaluation: only the listed subjects are judged, with exactly the verdicts a full
// evaluation gives them; ids that are not subjects of the rule's type are simply absent.
#[test]
fn conformance_for_given_subjects() {
    let (dir, db) = fixture_db("subjects");
    let full = db.query(&rule()).unwrap();
    // the unfiltered op keeps its shape and returns every row (compatibility)
    assert_eq!(full["verdicts"].as_array().unwrap().len(), 8);
    assert_eq!(full["total"], json!(8));
    assert_eq!(full["returned"], json!(8));
    assert_eq!(full["truncated"], json!(false));
    assert_eq!(
        full["counts"],
        json!({"OK": 4, "ABSENT": 1, "MISMATCH": 2, "NOT_APPLICABLE": 1})
    );

    let mut req = rule();
    req["subject"] = json!(1005);
    let one = db.query(&req).unwrap();
    assert_eq!(subjects_of(&one), vec![1005], "{one}");
    let full_1005 = full["verdicts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["subject"] == 1005)
        .unwrap();
    assert_eq!(&one["verdicts"][0], full_1005);

    // a list: sorted + deduplicated; a Person (10) and an unknown id are absent; an out-of-scope
    // subject still answers NOT_APPLICABLE when asked for explicitly
    let mut req = rule();
    req["subjects"] = json!([1006, 1004, 1004, 10, 999_999]);
    let some = db.query(&req).unwrap();
    assert_eq!(subjects_of(&some), vec![1004, 1006], "{some}");
    assert_eq!(some["verdicts"][1]["verdict"], json!("NOT_APPLICABLE"));
    assert_eq!(some["counts"]["MISMATCH"], json!(1));
    assert_eq!(some["counts"]["NOT_APPLICABLE"], json!(1));
    assert_eq!(some["counts"]["OK"], json!(0));

    // a masked subject is absent, like on every read
    db.ingest_str("{\"node\":{\"id\":1004,\"type\":\"Issue\",\"label\":2}}\n")
        .unwrap();
    let mut req = rule();
    req["subjects"] = json!([1004, 1006]);
    req["allowed_labels"] = json!(1);
    let masked = db.query(&req).unwrap();
    assert_eq!(subjects_of(&masked), vec![1006], "{masked}");

    // bad shapes are clear errors
    for bad in [
        json!({"subjects": 1004}),
        json!({"subjects": ["x"]}),
        json!({"subject": "x"}),
        json!({"only": ["BROKEN"]}),
        json!({"only": "OK"}),
    ] {
        let mut req = rule();
        for (k, v) in bad.as_object().unwrap() {
            req[k] = v.clone();
        }
        assert!(db.query(&req).is_err(), "expected an error for {bad}");
    }
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

// Bounded results: `only` filters outcomes, `limit`/`offset` page the kept rows, `total` counts
// them and `truncated` says whether rows remain; `counts` stays over every evaluated subject.
#[test]
fn conformance_only_limit_offset() {
    let (dir, db) = fixture_db("paging");
    let mut req = rule();
    req["only"] = json!(["MISMATCH", "ABSENT"]);
    let r = db.query(&req).unwrap();
    assert_eq!(subjects_of(&r), vec![1003, 1004, 1005], "{r}");
    assert_eq!(r["total"], json!(3));
    assert_eq!(r["counts"]["OK"], json!(4), "counts are before `only`");

    let mut req = rule();
    req["limit"] = json!(3);
    let p1 = db.query(&req).unwrap();
    assert_eq!(subjects_of(&p1), vec![1001, 1002, 1003]);
    assert_eq!(p1["total"], json!(8));
    assert_eq!(p1["returned"], json!(3));
    assert_eq!(p1["truncated"], json!(true));
    req["offset"] = json!(6);
    let p3 = db.query(&req).unwrap();
    assert_eq!(subjects_of(&p3), vec![1007, 1008]);
    assert_eq!(p3["truncated"], json!(false));
    req["offset"] = json!(50);
    let past = db.query(&req).unwrap();
    assert!(subjects_of(&past).is_empty());
    assert_eq!(past["truncated"], json!(false));
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

fn mcp_conformance(db: &Db, args: serde_json::Value) -> serde_json::Value {
    let msg = json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                     "params":{"name":"conformance","arguments":args}});
    let resp = mcp::handle_message(db, &msg).unwrap();
    assert_ne!(resp["result"]["isError"], json!(true), "{resp}");
    serde_json::from_str(resp["result"]["content"][0]["text"].as_str().unwrap()).unwrap()
}

// The MCP tool bounds a full evaluation by default (small limit, NOT_APPLICABLE omitted but
// counted), returns exactly the requested rows for `subjects`, and documents both.
#[test]
fn conformance_mcp_defaults_are_bounded() {
    let (dir, db) = fixture_db("mcp");
    let full = mcp_conformance(&db, json!({"rule": rule_body()}));
    assert!(
        !subjects_of(&full).contains(&1006),
        "NOT_APPLICABLE omitted by default: {full}"
    );
    assert_eq!(full["total"], json!(7));
    assert_eq!(full["counts"]["NOT_APPLICABLE"], json!(1));

    let na = mcp_conformance(
        &db,
        json!({"rule": rule_body(), "only": ["NOT_APPLICABLE"]}),
    );
    assert_eq!(subjects_of(&na), vec![1006]);

    let one = mcp_conformance(&db, json!({"rule": rule_body(), "subjects": [1006]}));
    assert_eq!(
        subjects_of(&one),
        vec![1006],
        "subject-scoped keeps every row"
    );

    let page = mcp_conformance(&db, json!({"rule": rule_body(), "limit": 2}));
    assert_eq!(page["returned"], json!(2));
    assert_eq!(page["truncated"], json!(true));

    // the default limit applies when none is given
    let mut many = String::new();
    for i in 0..(mcp::MCP_CONFORMANCE_LIMIT + 20) {
        let id = 50_000 + i;
        many.push_str(&format!(
            "{{\"node\":{{\"id\":{id},\"type\":\"Issue\",\"label\":0}}}}\n{{\"fact\":{{\"subject\":{id},\"predicate\":\"issue-type\",\"object\":{{\"text\":\"release\"}}}}}}\n"
        ));
    }
    db.ingest_str(&many).unwrap();
    let bounded = mcp_conformance(&db, json!({"rule": rule_body()}));
    assert_eq!(bounded["returned"], json!(mcp::MCP_CONFORMANCE_LIMIT));
    assert_eq!(bounded["truncated"], json!(true));
    // the HTTP/query op itself is unchanged: every row
    let all = db.query(&rule()).unwrap();
    assert_eq!(all["returned"], all["total"]);
    assert_eq!(all["truncated"], json!(false));

    // tool list + instructions describe the subjects parameter and the key → id step
    let list =
        mcp::handle_message(&db, &json!({"jsonrpc":"2.0","id":2,"method":"tools/list"})).unwrap();
    let tool = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "conformance")
        .unwrap()
        .clone();
    for p in ["subjects", "only", "limit", "offset"] {
        assert!(
            tool["inputSchema"]["properties"].get(p).is_some(),
            "missing {p}"
        );
    }
    let init = mcp::handle_message(
        &db,
        &json!({"jsonrpc":"2.0","id":3,"method":"initialize","params":{}}),
    )
    .unwrap();
    let text = init["result"]["instructions"].as_str().unwrap();
    assert!(
        text.contains("`lookup`") && text.contains("subjects: [id]"),
        "{text}"
    );
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

// An amount-banded approval table. Team 1 (manager 10) sits in division 2 (manager 11), which sits
// in company 3 (manager 12); requester 20 is in team 1. Every request is approved at 1200.
const BANDED: &str = r#"
{"type_def":{"name":"Person"}}
{"type_def":{"name":"Department"}}
{"type_def":{"name":"Request"}}
{"pred_def":{"name":"member-of","cardinality":"one","domain":"Person","range":"Department"}}
{"pred_def":{"name":"parent","cardinality":"one","domain":"Department","range":"Department"}}
{"pred_def":{"name":"manager-of","cardinality":"one","domain":"Department","range":"Person"}}
{"pred_def":{"name":"requester","cardinality":"one","domain":"Request","range":"Person"}}
{"pred_def":{"name":"approved-by","cardinality":"one","domain":"Request","range":"Person"}}
{"pred_def":{"name":"approved-at","cardinality":"one","domain":"Request","range_value":"int"}}
{"pred_def":{"name":"amount","cardinality":"one","domain":"Request","range_value":"int"}}
{"node":{"id":1,"type":"Department"}}
{"node":{"id":2,"type":"Department"}}
{"node":{"id":3,"type":"Department"}}
{"node":{"id":10,"type":"Person"}}
{"node":{"id":11,"type":"Person"}}
{"node":{"id":12,"type":"Person"}}
{"node":{"id":20,"type":"Person"}}
{"fact":{"subject":1,"predicate":"parent","object":{"node":2}}}
{"fact":{"subject":2,"predicate":"parent","object":{"node":3}}}
{"fact":{"subject":1,"predicate":"manager-of","object":{"node":10}}}
{"fact":{"subject":2,"predicate":"manager-of","object":{"node":11}}}
{"fact":{"subject":3,"predicate":"manager-of","object":{"node":12}}}
{"fact":{"subject":20,"predicate":"member-of","object":{"node":1}}}
{"node":{"id":101,"type":"Request"}}
{"fact":{"subject":101,"predicate":"requester","object":{"node":20}}}
{"fact":{"subject":101,"predicate":"amount","object":{"int":300000}}}
{"fact":{"subject":101,"predicate":"approved-at","object":{"int":1200}}}
{"fact":{"subject":101,"predicate":"approved-by","object":{"node":10}}}
{"node":{"id":102,"type":"Request"}}
{"fact":{"subject":102,"predicate":"requester","object":{"node":20}}}
{"fact":{"subject":102,"predicate":"amount","object":{"int":1500000}}}
{"fact":{"subject":102,"predicate":"approved-at","object":{"int":1200}}}
{"fact":{"subject":102,"predicate":"approved-by","object":{"node":10}}}
{"node":{"id":103,"type":"Request"}}
{"fact":{"subject":103,"predicate":"requester","object":{"node":20}}}
{"fact":{"subject":103,"predicate":"amount","object":{"int":1500000}}}
{"fact":{"subject":103,"predicate":"approved-at","object":{"int":1200}}}
{"fact":{"subject":103,"predicate":"approved-by","object":{"node":11}}}
{"node":{"id":104,"type":"Request"}}
{"fact":{"subject":104,"predicate":"requester","object":{"node":20}}}
{"fact":{"subject":104,"predicate":"amount","object":{"int":5000000}}}
{"fact":{"subject":104,"predicate":"approved-at","object":{"int":1200}}}
{"fact":{"subject":104,"predicate":"approved-by","object":{"node":12}}}
{"node":{"id":105,"type":"Request"}}
{"fact":{"subject":105,"predicate":"requester","object":{"node":20}}}
{"fact":{"subject":105,"predicate":"amount","object":{"int":400000}}}
{"fact":{"subject":105,"predicate":"amount","object":{"int":1500000},"valid_from":2000}}
{"fact":{"subject":105,"predicate":"approved-at","object":{"int":1200}}}
{"fact":{"subject":105,"predicate":"approved-by","object":{"node":10}}}
"#;

fn banded_rule(as_of: bool) -> serde_json::Value {
    let when = |mut c: serde_json::Value| {
        if as_of {
            c["as_of"] = json!("approved-at");
        }
        c
    };
    json!({
        "subject_type": "Request",
        "cases": [
            { "when": when(json!({"predicate": "amount", "lte": 500000})),
              "required": {"hops": [{"predicate":"requester"},{"predicate":"member-of"},{"predicate":"manager-of"}]} },
            { "when": when(json!({"predicate": "amount", "gt": 500000, "lte": {"int": 2000000}})),
              "required": {"hops": [{"predicate":"requester"},{"predicate":"member-of"},{"predicate":"parent"},{"predicate":"manager-of"}]} },
            { "required": {"hops": [{"predicate":"requester"},{"predicate":"member-of"},{"predicate":"parent"},{"predicate":"parent"},{"predicate":"manager-of"}]} }
        ],
        "actual": "approved-by"
    })
}

fn verdict_case_map(r: &serde_json::Value) -> BTreeMap<u64, (String, serde_json::Value)> {
    r["verdicts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            (
                v["subject"].as_u64().unwrap(),
                (
                    v["verdict"].as_str().unwrap().to_string(),
                    v["case"].clone(),
                ),
            )
        })
        .collect()
}

// Amount-banded rules end-to-end: a three-band `cases` rule yields OK / MISMATCH per band with the
// matched case on each row; a request whose amount was raised into a higher band after its
// approval is judged against the band in effect at the approval instant when the conditions say
// `as_of`, and against the current band otherwise; a stored banded rule survives a reopen; a live
// watch re-decides a subject when its amount is revised across a band.
#[test]
fn banded_rule_over_amount_ranges() {
    let dir = std::env::temp_dir()
        .join(format!("stroma_conformance_banded_{}", std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(BANDED).unwrap();

    let eval = |db: &Db, rule: serde_json::Value| {
        verdict_case_map(
            &db.query(&json!({"op": "conformance", "rule": rule}))
                .unwrap(),
        )
    };
    let row = |v: &str, c: serde_json::Value| (v.to_string(), c);
    let at_approval = eval(&db, banded_rule(true));
    assert_eq!(at_approval[&101], row("OK", json!(0)));
    assert_eq!(at_approval[&102], row("MISMATCH", json!(1)));
    assert_eq!(at_approval[&103], row("OK", json!(1)));
    assert_eq!(at_approval[&104], row("OK", json!(2)));
    // 400,000 at the approval instant: the team manager's approval was right
    assert_eq!(at_approval[&105], row("OK", json!(0)));
    // read at the current value instead, the raise to 1,500,000 calls for the division manager
    let current = eval(&db, banded_rule(false));
    assert_eq!(current[&105], row("MISMATCH", json!(1)));

    // a rule without cases reports no case
    let plain = db
        .query(&json!({"op": "conformance", "rule": {
            "subject_type": "Request",
            "scope": {"predicate": "amount", "between": [1000000, 2000000]},
            "required": {"hops": [{"predicate":"requester"},{"predicate":"member-of"},{"predicate":"parent"},{"predicate":"manager-of"}]},
            "actual": "approved-by"
        }}))
        .unwrap();
    let plain = verdict_case_map(&plain);
    assert_eq!(plain[&101], row("NOT_APPLICABLE", json!(null)));
    assert_eq!(plain[&103], row("OK", json!(null)));
    assert_eq!(plain[&105], row("MISMATCH", json!(null)));

    // malformed conditions are clear errors
    let bad = |cond: serde_json::Value| {
        db.query(&json!({"op": "conformance", "rule": {
            "subject_type": "Request",
            "scope": cond,
            "required": {"hops": [{"predicate":"requester"}]},
            "actual": "approved-by"
        }}))
        .unwrap_err()
    };
    assert!(bad(json!({"predicate": "amount", "gt": "big"})).contains("finite number"));
    assert!(bad(json!({"predicate": "amount", "gt": 1, "gte": 2})).contains("both gt and gte"));
    assert!(bad(json!({"predicate": "amount", "equals": 1, "lt": 2})).contains("exactly one test"));
    assert!(bad(json!({"predicate": "amount"})).contains("exactly one test"));
    assert!(bad(json!({"predicate": "amount", "between": [1]})).contains("[lo, hi]"));
    assert!(
        bad(json!({"predicate": "amount", "lt": 2, "as_of": "no-such-anchor"}))
            .contains("no-such-anchor")
    );
    let mut both = banded_rule(true);
    both["required"] = json!({"hops": [{"predicate":"requester"}]});
    let err = db
        .query(&json!({"op": "conformance", "rule": both}))
        .unwrap_err();
    assert!(err.contains("both required and cases"), "{err}");

    // stored banded rule: survives a reopen and is watchable
    let def = json!({"rule_def": {"name": "approval-bands", "rule": banded_rule(true)}});
    db.ingest_str(&def.to_string()).unwrap();
    drop(db); // release the directory lock
    let db = Db::open(&dir).unwrap();
    let by_name = json!({"op": "conformance", "rule_name": "approval-bands"});
    assert_eq!(verdict_case_map(&db.query(&by_name).unwrap()), at_approval);
    let w = db
        .query(&json!({"op": "conformance_watch", "rule_name": "approval-bands"}))
        .unwrap();
    let cursor = w["cursor"].as_u64().unwrap();
    // a retroactive correction: request 101 was really 900,000 from before its approval
    db.ingest_str(
        "{\"fact\":{\"subject\":101,\"predicate\":\"amount\",\"object\":{\"int\":900000},\"valid_from\":1000}}\n",
    )
    .unwrap();
    let c = db
        .query(
            &json!({"op": "conformance_changes", "rule_name": "approval-bands", "cursor": cursor}),
        )
        .unwrap();
    let changes = c["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 1, "{changes:?}");
    assert_eq!(changes[0]["subject"], json!(101));
    assert_eq!(changes[0]["old"]["verdict"], json!("OK"));
    assert_eq!(changes[0]["old"]["case"], json!(0));
    assert_eq!(changes[0]["new"]["verdict"], json!("MISMATCH"));
    assert_eq!(changes[0]["new"]["case"], json!(1));
    assert_eq!(changes[0]["new"]["required"], json!({"node": 11}));

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}
