//! Changelog growth baseline — the measurement behind the snapshot+truncate compaction decision:
//! at what volume does an unbounded log actually hurt? Drives an *event-lane shaped* workload
//! (high per-key churn, unlike `durability_slo`'s uniform edge inserts) through the real engine at
//! several volumes and reports, per volume:
//!
//!   - WAL size on disk (the unbounded-growth cost)
//!   - write+fsync throughput (unchanged by compaction — context)
//!   - cold-start RTO, split into frame decode (`wal::recover`) vs full open (decode + fold +
//!     observe) — compaction can only remove the decode+apply share of the *truncated prefix*;
//!     the fold itself must keep superseded rows (as-of reads are part of the contract), so a
//!     loaded snapshot is not smaller than the replayed fold, just cheaper to reach.
//!
//! The mix per 10 records: 5 SetOne supersessions over a bounded key space (status churn — deep
//! per-key history), 3 AddMany grants with event-time valid_from, 1 CloseMany revocation, 1 edge
//! property. Subjects average ~64 records each.
//!
//! Run: `cargo run --release --example changelog_growth -p stromadb-core [-- N[,N...]]`
//! (volumes in records; default `1000000,5000000`)

use std::time::Instant;
use stromadb_core::{Engine, ObjKey, WriteKind, wal};

const BATCH: usize = 50_000; // ETL chunk = one append+sync (group commit)

/// Deterministic event-lane record `i` of `n`: bounded subject space (avg ~64 records/subject),
/// event-time `valid_from = i` so histories are ordered the way a poll lane delivers them.
fn record(i: u64, n: u64) -> (u32, WriteKind) {
    let subjects = (n / 64).max(1);
    let subject = i % subjects;
    let at = i as i64;
    let kind = match i % 10 {
        // status-like supersession: the same (subject, predicate) rewritten over and over
        0..=4 => WriteKind::SetOne {
            subject,
            predicate: (i % 4) as u32,
            object: ObjKey::Node(1000 + (i % 7)),
            valid_from: at,
            valid_to: None,
        },
        // grant-like Many adds, one of which the `9` arm later closes
        5..=7 => WriteKind::AddMany {
            subject,
            predicate: 100 + (i % 3) as u32,
            object: ObjKey::Node(2000 + (i % 50)),
            valid_from: at,
            valid_to: None,
        },
        8 => WriteKind::SetEdgeProp {
            subject,
            predicate: 100,
            object: ObjKey::Node(2000 + (i % 50)),
            key: "role".into(),
            value: ObjKey::Int((i % 3) as i64),
        },
        // close an element the same subject added earlier (i-4 lands in the 5..=7 arm)
        _ => WriteKind::CloseMany {
            subject,
            predicate: 100 + ((i - 4) % 3) as u32,
            object: ObjKey::Node(2000 + ((i - 4) % 50)),
            valid_from: at,
        },
    };
    (0, kind)
}

fn run(n: u64) {
    let path = std::env::temp_dir().join(format!("stroma_changelog_growth_{n}.log"));
    let _ = std::fs::remove_file(&path);

    let t_write = Instant::now();
    {
        let mut e = Engine::open(&path, n as usize + BATCH).expect("open");
        let mut i = 0u64;
        while i < n {
            let len = BATCH.min((n - i) as usize);
            let chunk: Vec<_> = (i..i + len as u64).map(|j| record(j, n)).collect();
            e.write_batch(chunk).expect("append");
            e.sync().expect("fsync");
            i += len as u64;
        }
        assert_eq!(e.durable_head(), n);
    }
    let write_s = t_write.elapsed().as_secs_f64();
    let wal_bytes = std::fs::metadata(&path).unwrap().len();

    // decode share of the RTO: framing + checksum + payload decode, no fold
    let t_decode = Instant::now();
    let recovered = wal::recover(&path).expect("recover");
    let decode_s = t_decode.elapsed().as_secs_f64();
    assert_eq!(recovered.len() as u64, n);
    drop(recovered);

    // full cold open: decode + fold apply + observe
    let t_rto = Instant::now();
    let e = Engine::open(&path, n as usize + BATCH).expect("reopen");
    let rto_s = t_rto.elapsed().as_secs_f64();
    assert_eq!(e.durable_head(), n, "0 data loss");

    println!(
        "{:>10} records | WAL {:>8.1} MB ({:.1} B/rec) | write+fsync {:>6.2}s ({:.2}M rec/s) | RTO {:>6.2}s (decode {:.2}s / fold+observe {:.2}s) {}",
        n,
        wal_bytes as f64 / 1e6,
        wal_bytes as f64 / n as f64,
        write_s,
        n as f64 / write_s / 1e6,
        rto_s,
        decode_s,
        rto_s - decode_s,
        if rto_s < 10.0 {
            "[<10s SLO ok]"
        } else {
            "[>10s SLO MISS]"
        }
    );
    let _ = std::fs::remove_file(&path);
}

fn main() {
    let volumes: Vec<u64> = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "1000000,5000000".into())
        .split(',')
        .map(|s| s.trim().parse().expect("volume must be an integer"))
        .collect();
    println!(
        "=== changelog growth baseline — event-lane mix (5 SetOne / 3 AddMany / 1 prop / 1 CloseMany per 10) ==="
    );
    for n in volumes {
        run(n);
    }
}
