//! Lock-free reads during writes (#112 PR2): real threads, real contention, real assertions.
//!
//! A `loom`-lite stand-in — no `loom` dependency; instead we lean on `Db: Send + Sync` plus high
//! volume. `available_parallelism` reader threads loop point/expand/node/graph/overview queries while
//! writer threads continuously ingest new nodes/edges (durable) and embeddings. We assert:
//!   * every query returns `Ok` (no panic, no poisoned-lock crash) throughout the write storm;
//!   * every node type a reader observes is one of the registered types (no torn/garbage catalog id);
//!   * a *pinned* read view is internally consistent — a subject's `expand` neighbours are always a
//!     subset of the whole-graph edges incident to it (a torn/half-published snapshot would break it).
//!
//! Plus a deterministic single-threaded snapshot-isolation test: a view pinned before a write keeps
//! showing the pre-write state while the live view shows the post-write state.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use serde_json::{Value, json};
use stromadb_store::Db;

/// The only registered node type in this workload — every observed node type must be one of these.
const TYPES: [&str; 1] = ["Person"];

fn tmp(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("stroma_conc_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d.join("db")
}

/// Node ids incident to `subject` in a `graph` result's `edges` (`[[a, b, w], ...]`, undirected).
fn graph_neighbors(graph: &Value, subject: u64) -> BTreeSet<u64> {
    graph["edges"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| {
            let a = e[0].as_u64()?;
            let b = e[1].as_u64()?;
            if a == subject {
                Some(b)
            } else if b == subject {
                Some(a)
            } else {
                None
            }
        })
        .collect()
}

#[test]
fn lock_free_reads_during_writes() {
    let dir = tmp("rw");
    Db::init(&dir).unwrap();
    let db = Arc::new(Db::open(&dir).unwrap());

    // schema + a small seeded core: node 1 knows {2, 3, 4}; every node is a Person.
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"knows\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"name\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\",\"label\":0}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Person\",\"label\":0}}\n",
        "{\"node\":{\"id\":3,\"type\":\"Person\",\"label\":0}}\n",
        "{\"node\":{\"id\":4,\"type\":\"Person\",\"label\":0}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"name\",\"object\":{\"text\":\"root\"}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":2}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":3}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":4}}}\n",
    ))
    .unwrap();

    let n_readers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(2, 8);
    const WRITERS: u64 = 2;
    // Each writer batch is one durable ingest (an fsync); keep the count modest so the test stays
    // CI-fast (macOS fsync ≈ 20ms), while the readers still loop tens of thousands of times across
    // the write window (they run `while !done`, so reads span the entire storm).
    const WRITER_ITERS: u64 = 400; // 2 writers × 400 = 800 durable ingests

    let done = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    // --- readers: pin a view, then run several ops entirely on that pinned (consistent) view ---
    for _ in 0..n_readers {
        let db = db.clone();
        let done = done.clone();
        let reads = reads.clone();
        handles.push(std::thread::spawn(move || {
            let mut i = 0u64;
            while !done.load(Ordering::Relaxed) {
                // pin ONE read view; every query below observes the same consistent snapshot.
                let rs = db.read_state();

                // point (Ok even when the value is absent → {"one": null})
                rs.query(&json!({"op":"point","subject":1,"predicate":"name"}))
                    .expect("point must succeed");

                // node detail: any type it reports must be a registered type.
                let node = rs
                    .query(&json!({"op":"node","subject":1}))
                    .expect("node must succeed");
                if let Some(ty) = node["type"].as_str() {
                    assert!(TYPES.contains(&ty), "unregistered node type observed: {ty}");
                }

                // expand a stable subject (its out-neighbours are the seeded {2,3,4}).
                let ex = rs
                    .query(&json!({"op":"expand","subject":1,"predicate":"knows"}))
                    .expect("expand must succeed");
                let ex_nodes: Vec<u64> = ex["nodes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|n| n.as_u64().unwrap())
                    .collect();

                // periodically exercise the heavier whole-graph / overview ops on the SAME view and
                // check the torn-read invariant + type registration across the aggregate.
                if i.is_multiple_of(64) {
                    let g = rs
                        .query(&json!({"op":"graph","max_nodes":1000000}))
                        .expect("graph must succeed");
                    let g_nbrs = graph_neighbors(&g, 1);
                    for n in &ex_nodes {
                        assert!(
                            g_nbrs.contains(n),
                            "torn read: expand neighbour {n} of subject 1 absent from the graph edges \
                             of the same pinned view"
                        );
                    }
                    let ov = rs
                        .query(&json!({"op":"overview"}))
                        .expect("overview must succeed");
                    for node in ov["nodes"].as_array().unwrap() {
                        let name = node["name"].as_str().unwrap();
                        assert!(
                            TYPES.contains(&name) || name == "(untyped)",
                            "overview surfaced an unregistered type: {name}"
                        );
                    }
                }

                reads.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        }));
    }

    // --- writers: continuous durable ingest (new node + name + an edge into node 1) + embeddings ---
    for w in 0..WRITERS {
        let db = db.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..WRITER_ITERS {
                let id = 1_000 + w * 10_000_000 + i; // disjoint high id ranges per writer
                let jsonl = format!(
                    "{{\"node\":{{\"id\":{id},\"type\":\"Person\",\"label\":0}}}}\n\
                     {{\"fact\":{{\"subject\":{id},\"predicate\":\"name\",\"object\":{{\"text\":\"n{id}\"}}}}}}\n\
                     {{\"fact\":{{\"subject\":{id},\"predicate\":\"knows\",\"object\":{{\"node\":1}}}}}}\n"
                );
                db.ingest_str(&jsonl).expect("writer ingest must succeed");
                // sprinkle embeddings in (4-d), exercising embed_str + index rebuild under contention.
                if i.is_multiple_of(50) {
                    let a = (id % 7) as f32 / 7.0;
                    let emb = format!("{{\"node\":{id},\"vector\":[{a},0.0,0.0,1.0]}}\n");
                    db.embed_str(&emb).expect("writer embed must succeed");
                }
            }
        }));
    }

    // writers run their fixed budget; the 2 last handles are the writers. Join writers first, then
    // signal the readers to stop so reads span the entire write storm.
    let reader_handles: Vec<_> = handles.drain(..n_readers).collect();
    for h in handles {
        h.join().unwrap(); // writers
    }
    done.store(true, Ordering::Relaxed);
    for h in reader_handles {
        h.join().unwrap();
    }

    // sanity: the readers genuinely ran a lot of queries concurrently with the writers.
    assert!(
        reads.load(Ordering::Relaxed) > 1_000,
        "expected the readers to complete many iterations under contention"
    );

    // final state is consistent: node 1 still knows exactly its seeded out-neighbours.
    let ex = db
        .query(&json!({"op":"expand","subject":1,"predicate":"knows"}))
        .unwrap();
    assert_eq!(ex, json!({"nodes":[2,3,4]}));

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn snapshot_isolation_pins_pre_write_state() {
    let dir = tmp("iso");
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"knows\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\",\"label\":0}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Person\",\"label\":0}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":2}}}\n",
    ))
    .unwrap();

    // pin a read view BEFORE the next write lands.
    let rs0 = db.read_state();
    assert_eq!(
        rs0.query(&json!({"op":"expand","subject":1,"predicate":"knows"}))
            .unwrap(),
        json!({"nodes":[2]})
    );

    // a write lands: a new node 3 and edge (1 knows 3).
    db.ingest_str(concat!(
        "{\"node\":{\"id\":3,\"type\":\"Person\",\"label\":0}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":3}}}\n",
    ))
    .unwrap();

    // the pinned view still shows the PRE-write state (snapshot isolation).
    assert_eq!(
        rs0.query(&json!({"op":"expand","subject":1,"predicate":"knows"}))
            .unwrap(),
        json!({"nodes":[2]}),
        "pinned view must not see writes that landed after it was pinned"
    );
    // node 3 has no type in the pinned snapshot (it did not exist yet).
    assert_eq!(
        rs0.query(&json!({"op":"node","subject":3})).unwrap()["type"],
        json!(null)
    );

    // the live view (a fresh pin) reflects the post-write state.
    assert_eq!(
        db.query(&json!({"op":"expand","subject":1,"predicate":"knows"}))
            .unwrap(),
        json!({"nodes":[2,3]})
    );
    assert_eq!(
        db.query(&json!({"op":"node","subject":3})).unwrap()["type"],
        json!("Person")
    );

    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}
