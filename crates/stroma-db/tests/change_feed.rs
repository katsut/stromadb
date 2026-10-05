//! The change feed (`Db::changes_since`): per batch, the touched nodes with their type, the
//! predicates written and whether the node is new; filtered by node labels and per-fact labels at
//! read time; bounded, with `resync` when a cursor cannot be answered exactly.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Value, json};
use stromadb_store::Db;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn fresh(tag: &str) -> (std::path::PathBuf, Db) {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir()
        .join(format!(
            "stroma_change_feed_{}_{}_{}",
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
    "{\"pred_def\":{\"name\":\"email\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\"}}\n",
    "{\"pred_def\":{\"name\":\"salary\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"int\",\"label_floor\":3}}\n",
    "{\"pred_def\":{\"name\":\"member-of\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Team\"}}\n",
);

fn changes(db: &Db, since: u64, labels: u32) -> Vec<Value> {
    let f = db.changes_since(since, labels);
    assert!(!f.resync, "unexpected resync from {since}");
    f.merged()
}

fn preds(c: &Value) -> Vec<&str> {
    c["predicates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p.as_str().unwrap())
        .collect()
}

#[test]
fn feed_reports_touched_nodes_predicates_and_new_nodes() {
    let (_dir, db) = fresh("basic");
    db.ingest_str(SCHEMA).unwrap();
    let h0 = db.durable_head();
    // schema-only batches touch no node: nothing to report
    assert!(changes(&db, 0, u32::MAX).is_empty());

    let s = db
        .ingest_str(concat!(
            "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
            "{\"node\":{\"id\":10,\"type\":\"Team\"}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"name\",\"object\":{\"text\":\"Alice\"}}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"member-of\",\"object\":{\"node\":10}}}\n",
        ))
        .unwrap();
    let h1 = s.durable_head;
    let feed = db.changes_since(h0, u32::MAX);
    assert_eq!(feed.head, h1);
    assert_eq!(feed.batches.len(), 1);
    let c = feed.merged();
    assert_eq!(c.len(), 2, "{c:?}");
    assert_eq!(c[0]["node"], 1);
    assert_eq!(c[0]["type"], "Person");
    assert_eq!(c[0]["new"], true);
    assert_eq!(preds(&c[0]), ["name", "member-of"]);
    assert_eq!(
        c[1],
        json!({"node": 10, "type": "Team", "predicates": [], "new": true})
    );
    // ids, types and predicate names only, never values
    assert!(!feed.to_json().to_string().contains("Alice"));

    // nothing after the head
    assert!(changes(&db, h1, u32::MAX).is_empty());

    // a later write on an existing node: not new; a suppressed re-send journals nothing
    db.ingest_str(
        "{\"fact\":{\"subject\":1,\"predicate\":\"name\",\"object\":{\"text\":\"Alicia\"}}}\n",
    )
    .unwrap();
    let h2 = db.durable_head();
    db.ingest_str(
        "{\"fact\":{\"subject\":1,\"predicate\":\"name\",\"object\":{\"text\":\"Alicia\"}}}\n",
    )
    .unwrap();
    assert_eq!(db.durable_head(), h2);
    let c = changes(&db, h1, u32::MAX);
    assert_eq!(
        c,
        vec![json!({"node": 1, "type": "Person", "predicates": ["name"], "new": false})]
    );

    // a retract and a close are changes to their key too
    db.ingest_str(concat!(
        "{\"retract\":{\"subject\":1,\"predicate\":\"member-of\",\"object\":{\"node\":10}}}\n",
        "{\"close\":{\"subject\":1,\"predicate\":\"email\",\"valid_from\":5}}\n",
    ))
    .unwrap();
    let c = changes(&db, h2, u32::MAX);
    assert_eq!(c.len(), 1);
    assert_eq!(preds(&c[0]), ["email", "member-of"]);

    // two batches merged per node, but kept apart per batch
    let feed = db.changes_since(h0, u32::MAX);
    assert_eq!(feed.batches.len(), 3);
    let c = feed.merged();
    assert_eq!(c[0]["new"], true);
    assert_eq!(preds(&c[0]), ["name", "member-of", "email"]);
}

#[test]
fn labels_hide_changes_at_read_time() {
    let (_dir, db) = fresh("labels");
    db.ingest_str(SCHEMA).unwrap();
    db.ingest_str(concat!(
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Person\",\"label\":2}}\n",
        "{\"node\":{\"id\":10,\"type\":\"Team\"}}\n",
    ))
    .unwrap();
    let h0 = db.durable_head();
    let public = 0b11; // labels 0 and 1
    let all = u32::MAX;

    // a hidden node's changes are dropped entirely
    db.ingest_str(
        "{\"fact\":{\"subject\":2,\"predicate\":\"name\",\"object\":{\"text\":\"Bob\"}}}\n",
    )
    .unwrap();
    let h1 = db.durable_head();
    assert!(changes(&db, h0, public).is_empty());
    assert_eq!(changes(&db, h0, all).len(), 1);

    // a hidden fact on a visible node: the change is dropped when it is the only fact touched
    db.ingest_str("{\"fact\":{\"subject\":1,\"predicate\":\"email\",\"object\":{\"text\":\"a@example.com\"},\"label\":2}}\n")
        .unwrap();
    let h2 = db.durable_head();
    assert!(changes(&db, h1, public).is_empty());
    assert_eq!(preds(&changes(&db, h1, all)[0]), ["email"]);

    // ... and only the hidden predicate is dropped when visible facts were touched with it
    db.ingest_str(concat!(
        "{\"fact\":{\"subject\":1,\"predicate\":\"name\",\"object\":{\"text\":\"Alice\"}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"email\",\"object\":{\"text\":\"b@example.com\"},\"label\":2}}\n",
    ))
    .unwrap();
    let h3 = db.durable_head();
    assert_eq!(preds(&changes(&db, h2, public)[0]), ["name"]);
    assert_eq!(preds(&changes(&db, h2, all)[0]), ["name", "email"]);

    // a predicate floor hides every write of the predicate
    db.ingest_str("{\"fact\":{\"subject\":1,\"predicate\":\"salary\",\"object\":{\"int\":100}}}\n")
        .unwrap();
    let h4 = db.durable_head();
    assert!(changes(&db, h3, public).is_empty());
    assert!(changes(&db, h3, 0b1111).len() == 1);

    // a visible write on a key that also holds hidden rows is still visible
    db.ingest_str("{\"fact\":{\"subject\":1,\"predicate\":\"email\",\"object\":{\"text\":\"c@example.com\"}}}\n")
        .unwrap();
    let h5 = db.durable_head();
    assert_eq!(preds(&changes(&db, h4, public)[0]), ["email"]);

    // retracting a hidden element is hidden; retracting a visible one is not
    db.ingest_str(
        "{\"fact\":{\"subject\":1,\"predicate\":\"member-of\",\"object\":{\"node\":10},\"label\":2}}\n",
    )
    .unwrap();
    let h6 = db.durable_head();
    assert!(changes(&db, h5, public).is_empty());
    db.ingest_str(
        "{\"retract\":{\"subject\":1,\"predicate\":\"member-of\",\"object\":{\"node\":10}}}\n",
    )
    .unwrap();
    let h7 = db.durable_head();
    assert!(changes(&db, h6, public).is_empty());
    assert_eq!(preds(&changes(&db, h6, all)[0]), ["member-of"]);
    db.ingest_str(
        "{\"fact\":{\"subject\":1,\"predicate\":\"member-of\",\"object\":{\"node\":10}}}\n",
    )
    .unwrap();
    let h8 = db.durable_head();
    db.ingest_str(
        "{\"retract\":{\"subject\":1,\"predicate\":\"member-of\",\"object\":{\"node\":10}}}\n",
    )
    .unwrap();
    assert_eq!(preds(&changes(&db, h7, public)[0]), ["member-of"]);
    assert_eq!(preds(&changes(&db, h8, public)[0]), ["member-of"]);

    // relabeling a node hides its later changes (node labels are read from the current view)
    db.ingest_str("{\"node\":{\"id\":1,\"label\":2}}\n")
        .unwrap();
    assert!(changes(&db, h0, public).is_empty());
}

#[test]
fn resync_on_oversized_batch_reset_and_reopen() {
    let (dir, db) = fresh("resync");
    db.ingest_str(SCHEMA).unwrap();
    let h0 = db.durable_head();
    let big: String = (1..=1100)
        .map(|i| format!("{{\"node\":{{\"id\":{i},\"type\":\"Person\"}}}}\n"))
        .collect();
    db.ingest_str(&big).unwrap();
    let h1 = db.durable_head();
    let f = db.changes_since(h0, u32::MAX);
    assert!(f.resync, "an oversized batch is not journaled");
    assert_eq!(f.head, h1);
    assert!(f.to_json()["resync"].as_bool().unwrap());
    // past it, the journal answers again
    db.ingest_str("{\"node\":{\"id\":5000,\"type\":\"Person\"}}\n")
        .unwrap();
    assert_eq!(changes(&db, h1, u32::MAX).len(), 1);

    // after a reset the head restarts: an older cursor is ahead of it and resyncs
    let h2 = db.durable_head();
    db.reset().unwrap();
    assert!(db.changes_since(h2, u32::MAX).resync);
    db.ingest_str(SCHEMA).unwrap();
    db.ingest_str("{\"node\":{\"id\":1,\"type\":\"Person\"}}\n")
        .unwrap();
    assert_eq!(changes(&db, 0, u32::MAX).len(), 1);

    // the journal is in memory: after a reopen a cursor before the open head resyncs, one at it
    // reads on
    let h3 = db.durable_head();
    drop(db);
    let db = Db::open(&dir).unwrap();
    assert!(db.changes_since(0, u32::MAX).resync);
    assert!(changes(&db, h3, u32::MAX).is_empty());
    db.ingest_str("{\"node\":{\"id\":2,\"type\":\"Person\"}}\n")
        .unwrap();
    assert_eq!(changes(&db, h3, u32::MAX).len(), 1);
}
