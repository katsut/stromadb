//! #112 PR2: read p99 under concurrent writes — the lock-free-read payoff.
//!
//! A warm directory-backed `Arc<Db>` (Doc graph + IVF-PQ embeddings) serves `search` (type-aware
//! hybrid) and `expand` (1-hop graph) reads from the main thread. We measure read p50/p99/p999 in two
//! regimes: (1) the writer idle, and (2) a background thread doing *continuous durable ingest* (each
//! write fsyncs and publishes a fresh read view). Because a read pins the current `Arc<ReadState>` and
//! runs with no lock held, the write storm must not inflate the read tail — the two columns should
//! read essentially flat.
//!
//! Run: `cargo run --release --example rw_p99 -p stroma-db`
//!
//! NOTE: this lives under `stroma-db/examples/` (not `stroma-core/examples/`) because it drives the
//! `Db` API, which is defined in `stroma-db`; `stroma-core` cannot depend on `stroma-db`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use serde_json::{Value, json};
use stroma_db::Db;

const N: u64 = 4_000; // warm graph size (Docs)
const DIM: usize = 64;
const NQ: usize = 128; // distinct query vectors
// enough reads that each measured window lasts a few seconds — long enough for the fsync-bound
// writer (~50 durable ingests/s) to land a meaningful number of writes concurrently with the reads.
const SEARCH_ITERS: usize = 80_000;
const EXPAND_ITERS: usize = 3_000_000;

fn splitmix(s: &mut u64) -> f32 {
    *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *s;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    ((z ^ (z >> 31)) as f32 / u64::MAX as f32) * 2.0 - 1.0
}

fn gen_vec(seed: &mut u64) -> Vec<f32> {
    (0..DIM).map(|_| splitmix(seed)).collect()
}

fn vec_json(v: &[f32]) -> String {
    let body: Vec<String> = v.iter().map(|x| format!("{x:.5}")).collect();
    body.join(",")
}

fn pct(sorted: &[f64], q: f64) -> f64 {
    sorted[((sorted.len() as f64 * q) as usize).min(sorted.len() - 1)]
}

/// Run `iters` reads via `db.query`, timing each; `mk` builds the request for iteration `i`.
fn run_reads(db: &Db, iters: usize, mk: impl Fn(usize) -> Value) -> Vec<f64> {
    let mut lat = Vec::with_capacity(iters);
    for i in 0..iters {
        let req = mk(i);
        let t = Instant::now();
        db.query(&req).expect("read must succeed");
        lat.push(t.elapsed().as_secs_f64() * 1e3);
    }
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    lat
}

fn line(name: &str, lat: &[f64]) {
    println!(
        "  {name:<20} p50={:.3}ms  p99={:.3}ms  p999={:.3}ms  max={:.3}ms",
        pct(lat, 0.50),
        pct(lat, 0.99),
        pct(lat, 0.999),
        lat[lat.len() - 1],
    );
}

fn main() {
    let dir = std::env::temp_dir().join(format!("stroma_rw_p99_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = dir.join("db");
    Db::init(&db).unwrap();
    let db = Arc::new(Db::open(&db).unwrap());

    // --- build the warm graph: N Docs, 2 refs/doc, one embedding per doc ---
    let build = Instant::now();
    let mut ing = String::from(
        "{\"type_def\":{\"name\":\"Doc\"}}\n\
         {\"pred_def\":{\"name\":\"refs\",\"cardinality\":\"many\",\"domain\":\"Doc\",\"range\":\"Doc\"}}\n",
    );
    for i in 0..N {
        ing.push_str(&format!(
            "{{\"node\":{{\"id\":{i},\"type\":\"Doc\",\"label\":0}}}}\n"
        ));
    }
    for i in 0..N {
        ing.push_str(&format!(
            "{{\"fact\":{{\"subject\":{i},\"predicate\":\"refs\",\"object\":{{\"node\":{}}}}}}}\n",
            (i + 1) % N
        ));
        ing.push_str(&format!(
            "{{\"fact\":{{\"subject\":{i},\"predicate\":\"refs\",\"object\":{{\"node\":{}}}}}}}\n",
            (i + 7) % N
        ));
    }
    db.ingest_str(&ing).unwrap();

    let mut seed = 0xC0FF_EE00_1234_5678u64;
    let mut emb = String::new();
    for i in 0..N {
        let v = gen_vec(&mut seed);
        emb.push_str(&format!("{{\"node\":{i},\"vector\":[{}]}}\n", vec_json(&v)));
    }
    db.embed_str(&emb).unwrap();

    let queries: Vec<Vec<f32>> = (0..NQ).map(|_| gen_vec(&mut seed)).collect();
    let build_s = build.elapsed().as_secs_f64();

    let search_q =
        |i: usize| json!({ "op": "search", "type": "Doc", "vector": queries[i % NQ], "k": 10 });
    let expand_q = |i: usize| json!({ "op": "expand", "subject": (i as u64 * 2_654_435_761) % N, "predicate": "refs" });

    println!("=== #112 PR2 rw_p99: read latency, writer idle vs hammering ===");
    println!(
        "build       : {build_s:.1}s  ({N} docs, {} refs, {N} embeddings, dim {DIM})",
        2 * N
    );

    // --- regime 1: writer idle ---
    let s_idle = run_reads(&db, SEARCH_ITERS, search_q);
    let e_idle = run_reads(&db, EXPAND_ITERS, expand_q);

    // --- regime 2: a background thread does continuous durable ingest while we read ---
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicUsize::new(0));
    let writer = {
        let db = db.clone();
        let stop = stop.clone();
        let writes = writes.clone();
        std::thread::spawn(move || {
            let mut id = N;
            while !stop.load(Ordering::Relaxed) {
                let jsonl = format!(
                    "{{\"node\":{{\"id\":{id},\"type\":\"Doc\",\"label\":0}}}}\n\
                     {{\"fact\":{{\"subject\":{id},\"predicate\":\"refs\",\"object\":{{\"node\":{}}}}}}}\n",
                    id % N
                );
                db.ingest_str(&jsonl).expect("writer ingest must succeed");
                writes.fetch_add(1, Ordering::Relaxed);
                id += 1;
            }
        })
    };
    let s_busy = run_reads(&db, SEARCH_ITERS, search_q);
    let e_busy = run_reads(&db, EXPAND_ITERS, expand_q);
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    println!("\nwriter IDLE:");
    line("search", &s_idle);
    line("expand", &e_idle);
    println!(
        "\nwriter HAMMERING ({} durable ingests landed during the read window):",
        writes.load(Ordering::Relaxed)
    );
    line("search", &s_busy);
    line("expand", &e_busy);

    let flat = pct(&s_busy, 0.99) < pct(&s_idle, 0.99) * 3.0 + 0.5;
    println!(
        "\nread p99 flat under writes: {}",
        if flat { "PASS" } else { "CHECK" }
    );

    let _ = std::fs::remove_dir_all(dir);
}
