//! Per-fact access labels: a fact may carry a `label`, a predicate may declare a `label_floor`, and
//! every read applies the caller's `allowed_labels` per fact while the node itself stays visible.
//! Covers each read op and MCP tool, conformance `hidden_by_label`, floor changes on existing
//! facts, durability across reopen and compaction, and a property test: every read under a mask
//! equals the same read over a store into which the masked facts were never written.

use std::sync::atomic::{AtomicU64, Ordering};

use proptest::prelude::*;
use serde_json::{Value, json};
use stromadb_store::{Db, mcp};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn fresh(tag: &str) -> (std::path::PathBuf, Db) {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir()
        .join(format!(
            "stroma_fact_labels_{}_{}_{}",
            tag,
            std::process::id(),
            n
        ))
        .join("db");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    (dir, db)
}

const SCHEMA: &str = concat!(
    "{\"type_def\":{\"name\":\"Person\"}}\n",
    "{\"type_def\":{\"name\":\"Team\"}}\n",
    "{\"pred_def\":{\"name\":\"name\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\",\"display\":true}}\n",
    "{\"pred_def\":{\"name\":\"title\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\"}}\n",
    "{\"pred_def\":{\"name\":\"email\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\"}}\n",
    "{\"pred_def\":{\"name\":\"member-of\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Team\"}}\n",
    "{\"pred_def\":{\"name\":\"reports-to\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
);

/// Alice (1) and Bob (2) are public nodes; Alice's email is labeled 2, her first email is not.
fn people() -> (std::path::PathBuf, Db) {
    let (dir, db) = fresh("people");
    db.ingest_str(SCHEMA).unwrap();
    db.ingest_str(concat!(
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":10,\"type\":\"Team\"}}\n",
        "{\"node\":{\"id\":11,\"type\":\"Team\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"name\",\"object\":{\"text\":\"Alice\"}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"email\",\"object\":{\"text\":\"alice@old.example.com\"},\"valid_from\":10}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"email\",\"object\":{\"text\":\"alice@example.com\"},\"valid_from\":20,\"label\":2}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"member-of\",\"object\":{\"node\":10}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"member-of\",\"object\":{\"node\":11},\"label\":2,\"props\":{\"role\":\"lead\"}}}\n",
        "{\"fact\":{\"subject\":2,\"predicate\":\"name\",\"object\":{\"text\":\"Bob\"}}}\n",
        "{\"fact\":{\"subject\":2,\"predicate\":\"email\",\"object\":{\"text\":\"bob@example.com\"},\"label\":2}}\n",
    ))
    .unwrap();
    (dir, db)
}

const PUBLIC: u32 = 0b1;
const ALL: u32 = u32::MAX;

fn q(db: &Db, mut req: Value, labels: u32) -> Value {
    req["allowed_labels"] = json!(labels);
    db.query(&req).unwrap()
}

#[test]
fn a_node_stays_visible_while_its_labeled_email_is_hidden() {
    let (dir, db) = people();
    let point = json!({"op":"point","subject":1,"predicate":"email"});
    assert_eq!(
        q(&db, point.clone(), ALL)["one"],
        json!({"text":"alice@example.com"})
    );
    // the hidden head gives way to the latest visible row
    let r = q(&db, point.clone(), PUBLIC);
    assert_eq!(r["one"], json!({"text":"alice@old.example.com"}), "{r}");
    assert_eq!(r["valid_from"], json!(10));
    // Bob's only email is hidden: absent, exactly like a never-written key
    let r = q(
        &db,
        json!({"op":"point","subject":2,"predicate":"email"}),
        PUBLIC,
    );
    assert_eq!(r, json!({"one": null}));
    // the node itself is not denied
    let r = q(&db, json!({"op":"node","subject":2}), PUBLIC);
    assert!(r.get("denied").is_none(), "{r}");
    let preds: Vec<&str> = r["props"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["predicate"].as_str().unwrap())
        .collect();
    assert_eq!(preds, vec!["name"], "{r}");
    // find never matches a hidden literal
    let r = q(&db, json!({"op":"find","text":"bob@"}), PUBLIC);
    assert_eq!(r["nodes"], json!([]), "{r}");
    let r = q(&db, json!({"op":"find","text":"bob@"}), ALL);
    assert_eq!(r["nodes"][0]["id"], json!(2));
    // edge props follow their edge: the hidden membership's role is absent
    let props = json!({"op":"edge_props","subject":1,"predicate":"member-of","object":{"node":11}});
    assert_eq!(
        q(&db, props.clone(), ALL)["props"],
        json!({"role":{"text":"lead"}})
    );
    assert_eq!(q(&db, props, PUBLIC)["props"], json!({}));
    drop(db);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn expand_does_not_cross_a_hidden_edge() {
    let (dir, db) = people();
    let req = json!({"op":"expand","subject":1,"predicate":"member-of"});
    assert_eq!(q(&db, req.clone(), ALL)["nodes"], json!([10, 11]));
    assert_eq!(q(&db, req.clone(), PUBLIC)["nodes"], json!([10]));
    let mut asof = req.clone();
    asof["valid_at"] = json!(5);
    assert_eq!(q(&db, asof, PUBLIC)["nodes"], json!([10]));
    // the whole-graph and neighbourhood views drop the hidden edge, not the node
    let g = q(
        &db,
        json!({"op":"neighborhood","subject":1,"hops":1}),
        PUBLIC,
    );
    let ids: Vec<u64> = g["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_u64().unwrap())
        .collect();
    assert!(ids.contains(&10) && !ids.contains(&11), "{g}");
    let p = q(
        &db,
        json!({"op":"pipeline","source":{"nodes":[1]},"steps":[{"expand":"member-of"}]}),
        PUBLIC,
    );
    assert_eq!(p["ids"], json!([10]), "{p}");
    drop(db);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn timeline_hides_hidden_versions() {
    let (dir, db) = people();
    let req = json!({"op":"timeline","subject":1,"hops":["email"]});
    let all = q(&db, req.clone(), ALL);
    assert_eq!(all["segments"].as_array().unwrap().len(), 2, "{all}");
    let public = q(&db, req, PUBLIC);
    assert_eq!(
        public["segments"],
        json!([{"value":{"text":"alice@old.example.com"},"valid_from":10,"valid_to":null}]),
        "{public}"
    );
    let r = q(
        &db,
        json!({"op":"point","subject":1,"predicate":"email","valid_at":25}),
        PUBLIC,
    );
    assert_eq!(r["one"], json!({"text":"alice@old.example.com"}));
    drop(db);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn lookup_does_not_match_hidden_values() {
    let (dir, db) = people();
    let req = json!({"op":"lookup","predicate":"email","value":"bob@example.com"});
    assert_eq!(q(&db, req.clone(), ALL)["nodes"][0]["id"], json!(2));
    assert_eq!(q(&db, req.clone(), PUBLIC)["nodes"], json!([]));
    let mut asof = req;
    asof["valid_at"] = json!(100);
    assert_eq!(q(&db, asof, PUBLIC)["nodes"], json!([]));
    // the older, visible value is matchable as the current one under the mask
    let old = json!({"op":"lookup","predicate":"email","value":"alice@old.example.com"});
    assert_eq!(q(&db, old.clone(), PUBLIC)["nodes"][0]["id"], json!(1));
    assert_eq!(q(&db, old, ALL)["nodes"], json!([]));
    drop(db);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn display_names_fall_back_when_the_display_predicate_is_hidden() {
    let (dir, db) = fresh("display");
    db.ingest_str(SCHEMA).unwrap();
    db.ingest_str(concat!(
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"name\",\"object\":{\"text\":\"Alice Liddell\"},\"label\":3}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"title\",\"object\":{\"text\":\"Engineer\"}}}\n",
    ))
    .unwrap();
    let req = json!({"op":"type_nodes","type":"Person"});
    assert_eq!(
        q(&db, req.clone(), ALL)["nodes"][0]["name"],
        json!("Alice Liddell")
    );
    assert_eq!(q(&db, req, PUBLIC)["nodes"][0]["name"], json!("Engineer"));
    let r = q(
        &db,
        json!({"op":"lookup","predicate":"title","value":"Engineer"}),
        PUBLIC,
    );
    assert_eq!(r["nodes"][0]["display"], json!("Engineer"));
    drop(db);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn a_floor_applies_to_existing_facts_and_is_sticky() {
    let (dir, db) = people();
    let name = json!({"op":"point","subject":1,"predicate":"name"});
    assert_eq!(q(&db, name.clone(), PUBLIC)["one"], json!({"text":"Alice"}));
    // declaring a floor on `name` re-labels every existing name fact, history untouched
    let head = db.durable_head();
    db.ingest_str("{\"pred_def\":{\"name\":\"name\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\",\"display\":true,\"label_floor\":2}}\n")
        .unwrap();
    assert_eq!(db.durable_head(), head, "a floor change writes no fact");
    assert_eq!(q(&db, name.clone(), PUBLIC)["one"], json!(null));
    assert_eq!(q(&db, name.clone(), 0b101)["one"], json!({"text":"Alice"}));
    let s = db.query(&json!({"op":"schema"})).unwrap();
    let def = s["predicates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "name")
        .unwrap()
        .clone();
    assert_eq!(def["label_floor"], json!(2));
    assert_eq!(s["labels"], json!([2]), "{s}");
    // re-sending the def without label_floor keeps it (a schema re-send must not lower it)
    db.ingest_str("{\"pred_def\":{\"name\":\"name\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\",\"display\":true}}\n")
        .unwrap();
    assert_eq!(q(&db, name.clone(), PUBLIC)["one"], json!(null));
    // the floor survives a reopen (schema replay)
    drop(db);
    let db = Db::open(&dir).unwrap();
    assert_eq!(q(&db, name.clone(), PUBLIC)["one"], json!(null));
    // an explicit null clears it
    db.ingest_str("{\"pred_def\":{\"name\":\"name\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\",\"label_floor\":null}}\n")
        .unwrap();
    assert_eq!(q(&db, name, PUBLIC)["one"], json!({"text":"Alice"}));
    drop(db);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn labels_survive_reopen_and_compaction() {
    let (dir, db) = people();
    let reads = |db: &Db| {
        vec![
            q(
                db,
                json!({"op":"point","subject":1,"predicate":"email"}),
                PUBLIC,
            ),
            q(
                db,
                json!({"op":"point","subject":2,"predicate":"email"}),
                PUBLIC,
            ),
            q(
                db,
                json!({"op":"expand","subject":1,"predicate":"member-of"}),
                PUBLIC,
            ),
            q(
                db,
                json!({"op":"timeline","subject":1,"hops":["email"]}),
                PUBLIC,
            ),
            q(
                db,
                json!({"op":"point","subject":1,"predicate":"email"}),
                ALL,
            ),
        ]
    };
    let before = reads(&db);
    let stats = db.stats();
    assert_eq!(stats["fact_labels"], json!({"2": 3}), "{stats}");
    drop(db);
    let db = Db::open(&dir).unwrap();
    assert_eq!(reads(&db), before, "reopen (WAL replay)");
    db.compact().unwrap();
    assert_eq!(reads(&db), before, "after compaction");
    drop(db);
    let db = Db::open(&dir).unwrap();
    assert_eq!(reads(&db), before, "reopen from the compaction snapshot");
    assert_eq!(db.stats()["fact_labels"], json!({"2": 3}));
    drop(db);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn a_relabel_appends_and_an_identical_resend_is_suppressed() {
    let (dir, db) = people();
    let line = |label: &str| {
        format!(
            "{{\"fact\":{{\"subject\":2,\"predicate\":\"email\",\"object\":{{\"text\":\"bob@example.com\"}}{label}}}}}\n"
        )
    };
    // identical (same label 2) → suppressed
    let s = db.ingest_str(&line(",\"label\":2")).unwrap();
    assert_eq!((s.facts, s.suppressed), (0, 1));
    // the same value without the label → appends: it is a new, public row
    let s = db.ingest_str(&line("")).unwrap();
    assert_eq!((s.facts, s.suppressed), (1, 0));
    let r = q(
        &db,
        json!({"op":"point","subject":2,"predicate":"email"}),
        PUBLIC,
    );
    assert_eq!(r["one"], json!({"text":"bob@example.com"}));
    // a many element re-sent with a new label appends too
    let s = db
        .ingest_str("{\"fact\":{\"subject\":1,\"predicate\":\"member-of\",\"object\":{\"node\":10},\"label\":1}}\n")
        .unwrap();
    assert_eq!(s.facts, 1);
    // a labeled close hides the close row with the value it ends
    db.ingest_str(
        "{\"close\":{\"subject\":2,\"predicate\":\"email\",\"valid_from\":50,\"label\":2}}\n",
    )
    .unwrap();
    let pt = json!({"op":"point","subject":2,"predicate":"email"});
    assert_eq!(
        q(&db, pt.clone(), ALL),
        json!({"one": null, "closed_from": 50})
    );
    assert_eq!(q(&db, pt, PUBLIC)["one"], json!({"text":"bob@example.com"}));
    // labels outside 0..=31 are rejected
    let err = db.ingest_str(&line(",\"label\":32")).unwrap_err();
    assert!(err.contains("fact.label"), "{err}");
    let err = db
        .ingest_str("{\"pred_def\":{\"name\":\"title\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\",\"label_floor\":\"high\"}}\n")
        .unwrap_err();
    assert!(err.contains("label_floor"), "{err}");
    drop(db);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

const RULE: &str = "{\"rule_def\":{\"name\":\"manager-email\",\"rule\":{\"subject_type\":\"Person\",\"required\":{\"hops\":[{\"predicate\":\"reports-to\"},{\"predicate\":\"email\"}]},\"actual\":\"title\"}}}\n";

/// Carol (3) reports to Bob (2, hidden email) and Dave (4) to Erin (5, public email).
fn org() -> (std::path::PathBuf, Db) {
    let (dir, db) = fresh("org");
    db.ingest_str(SCHEMA).unwrap();
    db.ingest_str(RULE).unwrap();
    db.ingest_str(concat!(
        "{\"node\":{\"id\":2,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":3,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":4,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":5,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":6,\"type\":\"Team\"}}\n",
        "{\"fact\":{\"subject\":2,\"predicate\":\"email\",\"object\":{\"text\":\"bob@example.com\"},\"label\":2}}\n",
        "{\"fact\":{\"subject\":5,\"predicate\":\"email\",\"object\":{\"text\":\"erin@example.com\"}}}\n",
        "{\"fact\":{\"subject\":3,\"predicate\":\"reports-to\",\"object\":{\"node\":2}}}\n",
        "{\"fact\":{\"subject\":3,\"predicate\":\"title\",\"object\":{\"text\":\"bob@example.com\"}}}\n",
        "{\"fact\":{\"subject\":4,\"predicate\":\"reports-to\",\"object\":{\"node\":5}}}\n",
        "{\"fact\":{\"subject\":4,\"predicate\":\"title\",\"object\":{\"text\":\"nobody@example.com\"}}}\n",
    ))
    .unwrap();
    (dir, db)
}

fn verdict_of(r: &Value, subject: u64) -> Value {
    r["verdicts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["subject"] == subject)
        .cloned()
        .unwrap_or(Value::Null)
}

#[test]
fn conformance_reports_hidden_by_label_instead_of_a_verdict_or_a_value() {
    let (dir, db) = org();
    let req = json!({"op":"conformance","rule_name":"manager-email"});
    let all = q(&db, req.clone(), ALL);
    assert_eq!(verdict_of(&all, 3)["verdict"], json!("OK"), "{all}");
    assert_eq!(verdict_of(&all, 4)["verdict"], json!("MISMATCH"));
    let public = q(&db, req.clone(), PUBLIC);
    let carol = verdict_of(&public, 3);
    assert_eq!(carol["verdict"], json!("NOT_APPLICABLE"), "{public}");
    assert_eq!(carol["reason"], json!("hidden_by_label"));
    assert_eq!(carol["required"], json!(null));
    assert_eq!(carol["actual"], json!(null));
    // a subject that reads no hidden fact keeps its verdict
    assert_eq!(verdict_of(&public, 4)["verdict"], json!("MISMATCH"));
    assert_eq!(public["reasons"]["hidden_by_label"], json!(1));
    // the subject-scoped form: node-attribute reasons come first, then hidden_by_label
    let mut sub = req.clone();
    sub["subjects"] = json!([3, 6, 999]);
    let r = q(&db, sub, PUBLIC);
    assert_eq!(verdict_of(&r, 3)["reason"], json!("hidden_by_label"), "{r}");
    assert_eq!(verdict_of(&r, 6)["reason"], json!("not_subject_type"));
    assert_eq!(verdict_of(&r, 999)["reason"], json!("unknown_subject"));
    // a floor on `title` (the actual) withholds every judged subject
    db.ingest_str("{\"pred_def\":{\"name\":\"title\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\",\"label_floor\":4}}\n")
        .unwrap();
    let r = q(&db, req, PUBLIC);
    assert!(
        r["verdicts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["reason"] == "hidden_by_label"),
        "{r}"
    );
    drop(db);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn watched_conformance_masks_verdicts_and_journals_a_relabel() {
    let (dir, db) = org();
    let watch = |labels: u32| {
        q(
            &db,
            json!({"op":"conformance_watch","rule_name":"manager-email"}),
            labels,
        )
    };
    let w = watch(PUBLIC);
    assert_eq!(verdict_of(&w, 3)["reason"], json!("hidden_by_label"), "{w}");
    assert_eq!(verdict_of(&w, 4)["verdict"], json!("MISMATCH"));
    let cursor = w["cursor"].clone();
    // labeling Erin's email does not change Dave's verdict, but hides it from a public reader
    db.ingest_str("{\"fact\":{\"subject\":5,\"predicate\":\"email\",\"object\":{\"text\":\"erin@example.com\"},\"label\":2}}\n")
        .unwrap();
    let changes = |labels: u32| {
        q(
            &db,
            json!({"op":"conformance_changes","rule_name":"manager-email","cursor":cursor}),
            labels,
        )["changes"]
            .clone()
    };
    let public = changes(PUBLIC);
    assert_eq!(public.as_array().unwrap().len(), 1, "{public}");
    assert_eq!(public[0]["subject"], json!(4));
    assert_eq!(public[0]["old"]["verdict"], json!("MISMATCH"));
    assert_eq!(public[0]["new"]["reason"], json!("hidden_by_label"));
    // an unmasked reader sees no change at all
    assert_eq!(changes(ALL), json!([]));
    // a floor change invalidates the watch: the watcher re-watches
    db.ingest_str("{\"pred_def\":{\"name\":\"title\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\",\"label_floor\":4}}\n")
        .unwrap();
    let err = db
        .query(&json!({"op":"conformance_changes","rule_name":"manager-email","cursor":cursor}))
        .unwrap_err();
    assert!(err.contains("not watched"), "{err}");
    drop(db);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn mcp_tools_apply_the_fact_mask_of_a_capped_scope() {
    let (dir, db) = people();
    let scope = mcp::Scope {
        allowed_labels: Some(PUBLIC as u64),
        ..Default::default()
    };
    let call = |name: &str, args: Value| {
        let msg = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":args}});
        let r = mcp::handle_message_scoped(&db, &msg, &scope).unwrap();
        let text = r["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_string();
        serde_json::from_str::<Value>(&text).unwrap()
    };
    let r = call("point", json!({"subject":2,"predicate":"email"}));
    assert_eq!(r["one"], json!(null), "{r}");
    let r = call(
        "lookup",
        json!({"predicate":"email","value":"bob@example.com"}),
    );
    assert_eq!(r["nodes"], json!([]), "{r}");
    let r = call("expand", json!({"subject":1,"predicate":"member-of"}));
    assert_eq!(r["nodes"], json!([10]), "{r}");
    drop(db);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

// ---- property: a masked read equals the read over a store without the masked facts ----------

#[derive(Clone, Debug)]
enum Step {
    Fact {
        s: u64,
        p: usize,
        v: u64,
        label: Option<u8>,
    },
    Close {
        s: u64,
        p: usize,
        v: u64,
        label: Option<u8>,
    },
    Retract {
        s: u64,
        v: u64,
    },
}

/// (name, cardinality-one, node-valued)
const PREDS: [(&str, bool, bool); 4] = [
    ("name", true, false),
    ("email", true, false),
    ("reports-to", true, true),
    ("member-of", false, true),
];
const TEXTS: [&str; 3] = ["a@example.com", "b@example.com", "Carol"];

fn label() -> impl Strategy<Value = Option<u8>> {
    prop_oneof![Just(None), (0u8..4).prop_map(Some)]
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        5 => (1u64..5, 0usize..4, 0u64..3, label()).prop_map(|(s, p, v, label)| Step::Fact { s, p, v, label }),
        2 => (1u64..5, 0usize..4, 0u64..3, label()).prop_map(|(s, p, v, label)| Step::Close { s, p, v, label }),
        1 => (1u64..5, 0u64..3).prop_map(|(s, v)| Step::Retract { s, v }),
    ]
}

fn object(p: usize, v: u64) -> Value {
    if PREDS[p].2 {
        json!({"node": 1 + v})
    } else {
        json!({"text": TEXTS[v as usize]})
    }
}

/// The JSONL for `steps`; with `keep`, only the steps whose row it admits. Every row write gets a
/// distinct `valid_from` so no write is ever suppressed as a no-op re-send in either store.
fn lines(steps: &[Step], keep: &dyn Fn(usize, Option<u8>) -> bool) -> String {
    let mut out = String::new();
    for (i, st) in steps.iter().enumerate() {
        let vf = 10 * (i as i64 + 1);
        let line = match st {
            Step::Fact { s, p, v, label } if keep(*p, *label) => {
                let mut f = json!({"subject": s, "predicate": PREDS[*p].0, "object": object(*p, *v), "valid_from": vf});
                if let Some(l) = label {
                    f["label"] = json!(l);
                }
                json!({ "fact": f })
            }
            Step::Close { s, p, v, label } if keep(*p, *label) => {
                let mut c = json!({"subject": s, "predicate": PREDS[*p].0, "valid_from": vf});
                if !PREDS[*p].1 {
                    c["object"] = object(*p, *v);
                }
                if let Some(l) = label {
                    c["label"] = json!(l);
                }
                json!({ "close": c })
            }
            Step::Retract { s, v } => {
                json!({"retract": {"subject": s, "predicate": "member-of", "object": object(3, *v)}})
            }
            _ => continue,
        };
        out.push_str(&line.to_string());
        out.push('\n');
    }
    out
}

fn schema_with_floor(floor: Option<u8>) -> String {
    let mut s = String::from(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Team\"}}\n",
        "{\"pred_def\":{\"name\":\"name\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\",\"display\":true}}\n",
        "{\"pred_def\":{\"name\":\"reports-to\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"member-of\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
    ));
    let floor = floor
        .map(|f| format!(",\"label_floor\":{f}"))
        .unwrap_or_default();
    s.push_str(&format!(
        "{{\"pred_def\":{{\"name\":\"email\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\"{floor}}}}}\n"
    ));
    for n in 1..=5 {
        s.push_str(&format!(
            "{{\"node\":{{\"id\":{n},\"type\":\"Person\"}}}}\n"
        ));
    }
    s
}

/// Every read op over every subject and predicate, at the current time and at several instants.
fn reads(db: &Db, labels: u32) -> Vec<Value> {
    let mut out = Vec::new();
    for s in 1..=5u64 {
        for (p, one, _) in PREDS {
            out.push(q(
                db,
                json!({"op":"point","subject":s,"predicate":p}),
                labels,
            ));
            for at in [5, 25, 55, 95, 1000] {
                out.push(q(
                    db,
                    json!({"op":"point","subject":s,"predicate":p,"valid_at":at}),
                    labels,
                ));
            }
            if one {
                out.push(q(
                    db,
                    json!({"op":"timeline","subject":s,"hops":[p]}),
                    labels,
                ));
            }
        }
        for p in ["reports-to", "member-of"] {
            out.push(q(
                db,
                json!({"op":"expand","subject":s,"predicate":p}),
                labels,
            ));
            out.push(q(
                db,
                json!({"op":"expand","subject":s,"predicate":p,"valid_at":55}),
                labels,
            ));
        }
        out.push(q(
            db,
            json!({"op":"timeline","subject":s,"hops":["reports-to","email"]}),
            labels,
        ));
        out.push(q(db, json!({"op":"node","subject":s}), labels));
        out.push(q(
            db,
            json!({"op":"neighborhood","subject":s,"hops":2}),
            labels,
        ));
    }
    for t in TEXTS {
        out.push(q(db, json!({"op":"find","text":t}), labels));
        for p in ["name", "email"] {
            out.push(q(
                db,
                json!({"op":"lookup","predicate":p,"value":t}),
                labels,
            ));
            out.push(q(
                db,
                json!({"op":"lookup","predicate":p,"value":t,"valid_at":55}),
                labels,
            ));
        }
    }
    out.push(q(db, json!({"op":"type_nodes","type":"Person"}), labels));
    out.push(q(db, json!({"op":"graph"}), labels));
    out.push(q(db, json!({"op":"overview"}), labels));
    out.push(q(
        db,
        json!({"op":"completeness","type":"Person","required":["name","email","member-of"]}),
        labels,
    ));
    // `neighborhood`, `graph`, `overview` list nodes from hash maps: compare them order-free
    out.into_iter().map(canonical).collect()
}

fn canonical(mut v: Value) -> Value {
    if let Some(nodes) = v.get_mut("nodes").and_then(Value::as_array_mut) {
        nodes.sort_by_key(|n| n.to_string());
    }
    if let Some(edges) = v.get_mut("edges").and_then(Value::as_array_mut) {
        edges.sort_by_key(|e| e.to_string());
    }
    v
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]
    #[test]
    fn a_masked_read_equals_the_read_over_the_masked_out_store(
        steps in proptest::collection::vec(step(), 1..28),
        floor in prop_oneof![Just(None), (0u8..4).prop_map(Some)],
        mask in prop_oneof![Just(u32::MAX), 0u32..16],
    ) {
        let allowed = |l: u8| (mask >> l) & 1 == 1;
        let visible = |p: usize, label: Option<u8>| {
            let f = if PREDS[p].0 == "email" { floor } else { None };
            f.is_none_or(allowed) && label.is_none_or(allowed)
        };
        let (dir_a, a) = fresh("prop_a");
        a.ingest_str(&schema_with_floor(floor)).unwrap();
        a.ingest_str(&lines(&steps, &|_, _| true)).unwrap();
        let (dir_b, b) = fresh("prop_b");
        b.ingest_str(&schema_with_floor(floor)).unwrap();
        b.ingest_str(&lines(&steps, &visible)).unwrap();
        let (ra, rb) = (reads(&a, mask), reads(&b, mask));
        for (x, y) in ra.iter().zip(&rb) {
            prop_assert_eq!(x, y);
        }
        // and the same after A is compacted and reopened (labels in the snapshot codec)
        a.compact().unwrap();
        drop(a);
        let a = Db::open(&dir_a).unwrap();
        prop_assert_eq!(reads(&a, mask), rb);
        drop(a);
        drop(b);
        let _ = std::fs::remove_dir_all(dir_a.parent().unwrap());
        let _ = std::fs::remove_dir_all(dir_b.parent().unwrap());
    }
}
