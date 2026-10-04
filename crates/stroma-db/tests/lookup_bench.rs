//! As-of `lookup` latency over a keyed graph with histories — a micro benchmark, not a pass/fail
//! gate (wall-clock numbers vary by machine). Run with:
//!   cargo test -p stromadb-store --release --test lookup_bench -- --ignored --nocapture

use std::fmt::Write as _;
use std::time::Instant;

use serde_json::{Value, json};
use stromadb_store::Db;

const ISSUES: u64 = 6_000;
const KEY_VERSIONS: u64 = 3;
const OTHER_PREDICATES: u64 = 4;
const LOOKUPS: u64 = 5_300;

fn key(i: u64, v: u64) -> String {
    format!("P{v}-{i}")
}

#[test]
#[ignore = "benchmark: run explicitly with --ignored --nocapture"]
fn asof_lookup_latency() {
    let dir = std::env::temp_dir().join(format!("stroma_lookup_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = Db::open_or_init(&dir).unwrap();
    let mut seed = String::from(concat!(
        "{\"type_def\":{\"name\":\"Issue\"}}\n",
        "{\"pred_def\":{\"name\":\"issue-key\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range_value\":\"text\"}}\n",
    ));
    for p in 0..OTHER_PREDICATES {
        writeln!(
            seed,
            "{{\"pred_def\":{{\"name\":\"attr{p}\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range_value\":\"int\"}}}}"
        )
        .unwrap();
    }
    for i in 1..=ISSUES {
        writeln!(seed, "{{\"node\":{{\"id\":{i},\"type\":\"Issue\"}}}}").unwrap();
    }
    db.ingest_str(&seed).unwrap();
    let mut facts = String::new();
    for i in 1..=ISSUES {
        // each issue was re-keyed KEY_VERSIONS - 1 times, at t = 100, 200, …
        for v in 0..KEY_VERSIONS {
            writeln!(
                facts,
                "{{\"fact\":{{\"subject\":{i},\"predicate\":\"issue-key\",\"object\":{{\"text\":\"{}\"}},\"valid_from\":{}}}}}",
                key(i, v),
                v * 100
            )
            .unwrap();
        }
        for p in 0..OTHER_PREDICATES {
            for v in 0..2 {
                writeln!(
                    facts,
                    "{{\"fact\":{{\"subject\":{i},\"predicate\":\"attr{p}\",\"object\":{{\"int\":{}}},\"valid_from\":{}}}}}",
                    i * 10 + v,
                    v * 100
                )
                .unwrap();
            }
        }
    }
    db.ingest_str(&facts).unwrap();

    // a key held between t=100 and t=200, read at t=150
    let probe = |n: u64| -> (String, u64) {
        let i = (n * 7919) % ISSUES + 1;
        (key(i, 1), i)
    };
    let t = Instant::now();
    for n in 0..LOOKUPS {
        let (k, want) = probe(n);
        let r = db
            .query(&json!({"op":"lookup","predicate":"issue-key","value":k,"valid_at":150}))
            .unwrap();
        assert_eq!(r["nodes"][0]["id"], json!(want), "{r}");
    }
    let single = t.elapsed();
    println!(
        "single as-of lookups: {LOOKUPS} calls in {single:?} ({:?}/call) over {} one keys",
        single / LOOKUPS as u32,
        ISSUES * (1 + OTHER_PREDICATES)
    );

    let t = Instant::now();
    for n in 0..LOOKUPS {
        let i = (n * 7919) % ISSUES + 1;
        let r = db
            .query(&json!({"op":"lookup","predicate":"issue-key","value":key(i, KEY_VERSIONS - 1)}))
            .unwrap();
        assert_eq!(r["nodes"][0]["id"], json!(i), "{r}");
    }
    let current = t.elapsed();
    println!(
        "single current lookups: {LOOKUPS} calls in {current:?} ({:?}/call)",
        current / LOOKUPS as u32
    );

    let queries: Vec<Value> = (0..LOOKUPS)
        .map(|n| json!({"value": probe(n).0, "valid_at": 150}))
        .collect();
    let t = Instant::now();
    let mut answered = 0;
    for chunk in queries.chunks(1_000) {
        let r = db
            .query(&json!({"op":"lookup","predicate":"issue-key","queries":chunk}))
            .unwrap();
        answered += r["results"].as_array().unwrap().len();
    }
    let batched = t.elapsed();
    assert_eq!(answered as u64, LOOKUPS);
    println!("batched as-of lookups: {LOOKUPS} queries in {batched:?}");
    let _ = std::fs::remove_dir_all(&dir);
}
