//! End-to-end conformance op: ingest the backlog release-approval fixture and evaluate a declared
//! rule into deterministic per-subject verdicts (OK / ABSENT / MISMATCH / NOT_APPLICABLE), including
//! the as-of hop (a manager that changes over valid-time: an approval before the change is OK, after
//! it is a MISMATCH). The rule composes existing read primitives — no reasoner.

use std::collections::BTreeMap;

use serde_json::json;
use stromadb_store::Db;

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
