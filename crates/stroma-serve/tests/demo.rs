//! The bundled demo dataset must keep firing every headline read: multi-segment timelines, an
//! as-of read that differs from the current value, mixed conformance verdicts (OK + self-approval
//! wrong + stale + absent + not-applicable), distinct confidence tiers, and offline vector search.
//! These tests pin that contract so a data edit can't quietly turn the demo into a blank.

use serde_json::json;
use stromadb_serve::demo;
use stromadb_store::Db;

fn demo_db(tag: &str) -> (std::path::PathBuf, Db) {
    let dir = std::env::temp_dir()
        .join(format!("stroma_demo_test_{}_{}", tag, std::process::id()))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(demo::GRAPH_JSONL).unwrap();
    db.embed_str(demo::EMBED_JSONL).unwrap();
    (dir, db)
}

#[test]
fn demo_timeline_and_asof_fire() {
    let (dir, db) = demo_db("tl");

    // printed query 2: Alice's manager over time — three intervals (Bob → Dave → Eve)
    let r = db
        .query(&json!({"op":"timeline","subject":1,"hops":["member-of","manager-of"]}))
        .unwrap();
    let segs = r["segments"].as_array().unwrap();
    assert_eq!(
        segs.len(),
        3,
        "demo timeline must stay multi-segment: {segs:?}"
    );
    assert_eq!(segs[0]["value"], json!({"node": 2}));
    assert_eq!(segs[1]["value"], json!({"node": 4}));
    assert_eq!(segs[2]["value"], json!({"node": 5}));
    assert_eq!(segs[2]["valid_to"], json!(null));

    // printed query 1: as-of 2024-09-01 Alice was in Sales; today she is in Operations
    let r = db
        .query(&json!({"op":"point","subject":1,"predicate":"member-of","valid_at":1725148800}))
        .unwrap();
    assert_eq!(r["one"], json!({"node": 200}));
    let r = db
        .query(&json!({"op":"point","subject":1,"predicate":"member-of"}))
        .unwrap();
    assert_eq!(r["one"], json!({"node": 300}));

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn demo_conformance_verdicts_stay_mixed() {
    let (dir, db) = demo_db("cf");

    // printed query 3: the stored rule yields exactly the planted mix
    let r = db
        .query(&json!({"op":"conformance","rule_name":"release-approval"}))
        .unwrap();
    let vs = r["verdicts"].as_array().unwrap();
    let by = |id: u64| vs.iter().find(|v| v["subject"] == json!(id)).unwrap();
    assert_eq!(by(1001)["verdict"], json!("OK"));
    assert_eq!(by(1002)["verdict"], json!("MISMATCH"), "self-approval");
    assert_eq!(by(1002)["kind"], json!("wrong"));
    assert_eq!(by(1003)["verdict"], json!("MISMATCH"), "former manager");
    assert_eq!(by(1003)["kind"], json!("stale"));
    assert_eq!(
        by(1004)["verdict"],
        json!("ABSENT"),
        "released without sign-off"
    );
    assert_eq!(
        by(1005)["verdict"],
        json!("NOT_APPLICABLE"),
        "not a release"
    );

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn demo_confidence_tiers_and_search_fire() {
    let (dir, db) = demo_db("cs");

    // the up-front source_def lines registered every provenance source (SPEC §2 — stromadb#226)
    let sources = db.stats()["sources"].clone();
    for s in ["hr", "wiki", "approval-log"] {
        assert!(
            sources.as_array().unwrap().iter().any(|v| v == s),
            "missing source {s}: {sources}"
        );
    }

    // corroborated (hr + wiki) → high; single source → medium; source-less → low
    let tier = |id: u64| {
        db.query(&json!({"op":"point","subject":id,"predicate":"name"}))
            .unwrap()["confidence"]["tier"]
            .clone()
    };
    assert_eq!(tier(1), json!("high"));
    assert_eq!(tier(2), json!("medium"));
    assert_eq!(tier(3), json!("low"));

    // offline vector search: a policy-cluster query lands policy docs, not incident docs
    let r = db
        .query(&json!({"op":"search","type":"Doc","vector":[0.9,0.85,0.1,0.05,0.05,0.1,0.03,0.05],"k":3}))
        .unwrap();
    let ids: Vec<u64> = r["ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    assert_eq!(ids.len(), 3);
    assert!(
        ids.iter().all(|id| (2001..=2003).contains(id)),
        "policy cluster expected: {ids:?}"
    );

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}
