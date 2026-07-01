//! Durability SLO probe on the *real* engine (not a standalone spike): drive the A1 representative
//! point (~5M facts) through `Engine::open` → `write_batch` → `sync` (group commit), then cold-restart
//! and measure recovery. Checks the locked DONE SLO: 0 data loss + cold-start replay < 10s @5M.
//!
//! Run: `cargo run --release --example durability_slo -p stroma-core`

use std::time::Instant;
use stroma_core::{Engine, ObjKey, WriteKind};

const FACTS: u64 = 5_000_000;
const SUBJECTS: u64 = 625_000; // avg degree 8 (A1)
const BATCH: usize = 50_000; // ETL chunk = one append+sync (group commit)

fn edge(i: u64) -> (u32, WriteKind) {
    (
        0,
        WriteKind::AddMany {
            subject: i % SUBJECTS,
            predicate: 100 + (i % 8) as u32,
            object: ObjKey::Node(i),
        },
    )
}

fn main() {
    let path = std::env::temp_dir().join("stroma_durability_slo.log");
    let _ = std::fs::remove_file(&path);

    // --- write + fsync path (group commit per chunk) ---
    let t_write = Instant::now();
    {
        let mut e = Engine::open(&path, FACTS as usize + BATCH).expect("open");
        let mut i = 0u64;
        while i < FACTS {
            let n = BATCH.min((FACTS - i) as usize);
            let chunk: Vec<_> = (i..i + n as u64).map(edge).collect();
            e.write_batch(chunk).expect("append");
            e.sync().expect("fsync"); // durability commit point
            i += n as u64;
        }
        assert_eq!(e.durable_head(), FACTS);
    }
    let write_s = t_write.elapsed().as_secs_f64();
    let wal_bytes = std::fs::metadata(&path).unwrap().len();

    // --- cold-start recovery (RTO): recover WAL + rebuild fold ---
    let t_rto = Instant::now();
    let e = Engine::open(&path, FACTS as usize + BATCH).expect("reopen");
    let rto_s = t_rto.elapsed().as_secs_f64();

    let bytes_per_rec = wal_bytes as f64 / FACTS as f64;
    let logical = 20.0; // subject(8)+predicate(4)+object(8)
    println!("=== durability SLO — real engine @ A1 representative ({FACTS} facts) ===");
    println!(
        "write+fsync : {write_s:.2}s  ({:.2}M facts/s, {} chunks of {BATCH})",
        FACTS as f64 / write_s / 1e6,
        FACTS as usize / BATCH
    );
    println!(
        "WAL size    : {:.1} MB  ({bytes_per_rec:.1} B/record, app-WAF ~{:.2}x vs {logical:.0}B logical)",
        wal_bytes as f64 / 1e6,
        bytes_per_rec / logical
    );
    println!(
        "RTO (cold)  : {rto_s:.2}s   [SLO < 10s]  → {}",
        if rto_s < 10.0 { "PASS" } else { "FAIL" }
    );
    println!(
        "recovered   : durable_head={} (0 data loss)",
        e.durable_head()
    );
    assert_eq!(
        e.durable_head(),
        FACTS,
        "recovered count must equal committed"
    );
    let _ = std::fs::remove_file(&path);
}
