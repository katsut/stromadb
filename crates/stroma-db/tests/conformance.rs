//! End-to-end conformance op: ingest the backlog release-approval fixture and evaluate a declared
//! rule into deterministic per-subject verdicts (OK / ABSENT / MISMATCH / NOT_APPLICABLE), including
//! the as-of hop (a manager that changes over valid-time: an approval before the change is OK, after
//! it is a MISMATCH). The rule composes existing read primitives — no reasoner.

use std::collections::BTreeMap;

use serde_json::json;
use stroma_db::Db;

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

fn rule() -> serde_json::Value {
    json!({
        "op": "conformance",
        "rule": {
            "subject_type": "Issue",
            "scope":     { "predicate": "issue-type", "equals": "release" },
            "required":  { "hops": [
                { "predicate": "assigned-to" },
                { "predicate": "member-of" },
                { "predicate": "manager-of", "as_of": "approved-at" }
            ] },
            "actual":      "approved-by",
            "absent_when": { "predicate": "status", "equals": "released" }
        }
    })
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
