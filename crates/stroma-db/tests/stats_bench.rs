//! `stats()` latency under concurrent ingest — a micro benchmark, not a pass/fail gate (wall-clock
//! numbers vary by machine). Run with:
//!   cargo test -p stromadb-store --release --test stats_bench -- --ignored --nocapture

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use stromadb_store::Db;

const NODES: u64 = 8_000;
const FACTS: u64 = 100_000;

fn facts(from: u64, n: u64) -> String {
    let mut s = String::new();
    for i in from..from + n {
        let subject = i % NODES + 1;
        let object = (i * 7919) % NODES + 1;
        writeln!(
            s,
            "{{\"fact\":{{\"subject\":{subject},\"predicate\":\"rel\",\"object\":{{\"node\":{object}}},\"valid_from\":{i}}}}}"
        )
        .unwrap();
    }
    s
}

#[test]
#[ignore = "benchmark: run explicitly with --ignored --nocapture"]
fn stats_latency_under_concurrent_ingest() {
    let dir = std::env::temp_dir().join(format!("stroma_stats_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = Arc::new(Db::open_or_init(&dir).unwrap());
    let mut seed = String::from(concat!(
        "{\"type_def\":{\"name\":\"Item\"}}\n",
        "{\"pred_def\":{\"name\":\"rel\",\"cardinality\":\"many\",\"domain\":\"Item\",\"range\":\"Item\"}}\n",
    ));
    for id in 1..=NODES {
        writeln!(
            seed,
            "{{\"node\":{{\"id\":{id},\"type\":\"Item\",\"label\":{}}}}}",
            id % 4
        )
        .unwrap();
    }
    db.ingest_str(&seed).unwrap();
    // embeddings make a node-touching batch rebuild the vector index under the write lock — the
    // realistic slow writer a stats read must not wait behind
    let mut emb = String::new();
    for id in 1..=NODES {
        let v: Vec<String> = (0..32)
            .map(|d| format!("{:.3}", ((id * 31 + d * 17) % 97) as f32 / 97.0))
            .collect();
        writeln!(emb, "{{\"node\":{id},\"vector\":[{}]}}", v.join(",")).unwrap();
    }
    db.embed_str(&emb).unwrap();
    let mut next = 0;
    while next < FACTS {
        db.ingest_str(&facts(next, 10_000)).unwrap();
        next += 10_000;
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut next = FACTS;
            while !stop.load(Ordering::Relaxed) {
                let mut batch = facts(next, 5_000);
                // a changed node label touches nodes, so the batch also rebuilds the index
                writeln!(
                    batch,
                    "{{\"node\":{{\"id\":1,\"label\":{}}}}}",
                    (next / 5_000) % 4
                )
                .unwrap();
                db.ingest_str(&batch).unwrap();
                next += 5_000;
            }
        })
    };
    std::thread::sleep(Duration::from_millis(200));
    let mut lat: Vec<Duration> = (0..200)
        .map(|_| {
            let t = Instant::now();
            let s = db.stats();
            let d = t.elapsed();
            assert!(s["schema"]["nodes"].as_u64().unwrap() >= NODES);
            std::thread::sleep(Duration::from_millis(5));
            d
        })
        .collect();
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    lat.sort();
    let pct = |p: usize| lat[(lat.len() * p / 100).min(lat.len() - 1)];
    println!(
        "stats() under ingest: p50={:?} p99={:?} max={:?} (nodes={NODES}, facts>={FACTS})",
        pct(50),
        pct(99),
        lat[lat.len() - 1]
    );
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
