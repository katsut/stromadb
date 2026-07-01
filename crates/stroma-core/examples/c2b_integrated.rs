//! #20 C2b: the integration seam under realistic load — durable changelog Engine (write + fsync) +
//! real IVF-PQ reads (type-aware hybrid via the query-IR) + Live Query (IVM) maintenance, all on one
//! coordinated-omission open-loop timeline. Re-measures the C2b tail with the REAL backends (not the
//! in-memory/exact stand-ins the Phase-0 spike used) and checks 0 data loss on cold reopen.
//!
//! Each epoch performs a durable write chunk (append + fsync) + incremental embeddings + materialize +
//! snapshot + live-query re-eval (the "stall"), then serves a batch of hybrid reads. Read latency is
//! charged against a fixed arrival schedule, so stalls that delay reads show up in the tail (no
//! coordinated omission). Run: `cargo run --release --example c2b_integrated -p stroma-core`

use std::time::Instant;
use stroma_core::catalog::{Cardinality, Catalog, Range, RelProps};
use stroma_core::changelog::WriteKind;
use stroma_core::engine::Engine;
use stroma_core::fold::ObjKey;
use stroma_core::ir::{Pipeline, Principal, Source, Transform, run};
use stroma_core::ivf::IvfPq;
use stroma_core::live::LiveQueries;
use stroma_core::query;
use stroma_core::version::{ReadMode, VersionVector};

const N: usize = 100_000; // set higher (e.g. 500_000) for the A1 representative-scale re-measure // initial docs
const DIM: usize = 768;
const M: usize = 96;
const TRAIN: usize = 20_000;
const NC: usize = 1500;
const NOISE: f32 = 0.35;
const K: usize = 10; // IR read path uses its own operating-point defaults (nprobe=8, R=256)

const EPOCHS: usize = 40;
const READS_PER_EPOCH: usize = 500;
const WRITE_EDGES: usize = 200; // durable edges appended per epoch
const NEW_DOCS: usize = 20; // embeddings added per epoch
const LIVE_QUERIES: usize = 30; // A1: 30 concurrent live queries

fn splitmix(s: &mut u64) -> f32 {
    *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *s;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    ((z ^ (z >> 31)) as f32 / u64::MAX as f32) * 2.0 - 1.0
}

fn centers() -> Vec<Vec<f32>> {
    let mut s = 0xC0FF_EE00_1234_5678u64;
    (0..NC)
        .map(|_| (0..DIM).map(|_| splitmix(&mut s)).collect())
        .collect()
}

fn gen_vecs(n: usize, seed: u64, ctr: &[Vec<f32>]) -> Vec<Vec<f32>> {
    let mut s = seed;
    (0..n)
        .map(|_| {
            let c = &ctr[(splitmix(&mut s).abs() * NC as f32) as usize % NC];
            (0..DIM).map(|i| c[i] + splitmix(&mut s) * NOISE).collect()
        })
        .collect()
}

fn pct(sorted: &[f64], q: f64) -> f64 {
    sorted[((sorted.len() as f64 * q) as usize).min(sorted.len() - 1)]
}

fn main() {
    let ctr = centers();
    let data = gen_vecs(N, 42, &ctr);

    // catalog: every node is a Doc; `refs` is a Doc→Doc relation; authz label = node % 4.
    let mut cat = Catalog::new();
    let doc = cat.register_type("Doc");
    let refs = cat.register_predicate(
        "refs",
        Cardinality::Many,
        RelProps::default(),
        doc,
        Range::Type(doc),
    );
    for i in 0..N as u64 {
        cat.set_node_type(i, doc);
        cat.set_node_label(i, (i % 4) as u8);
    }

    // real vector backend (IVF-PQ), parallel build — nlist scales with N (#30: avoids cell imbalance)
    let mut ivf = IvfPq::new(DIM, IvfPq::suggested_nlist(N), M);
    let t = Instant::now();
    ivf.train(&data[..TRAIN]);
    ivf.add_batch(
        data.iter()
            .enumerate()
            .map(|(i, v)| (i as u64, i as u64, v.clone(), (i % 4) as u32))
            .collect(),
    );
    let build_s = t.elapsed().as_secs_f64();

    // durable engine, seed 2 refs/doc
    let path = std::env::temp_dir().join("stroma_c2b.wal");
    let _ = std::fs::remove_file(&path);
    let mut eng = Engine::open(&path, 8_000_000).unwrap();
    for chunk_start in (0..N).step_by(25_000) {
        let end = (chunk_start + 25_000).min(N);
        let chunk: Vec<(u32, WriteKind)> = (chunk_start..end)
            .flat_map(|i| {
                let i = i as u64;
                [
                    (
                        0u32,
                        WriteKind::AddMany {
                            subject: i,
                            predicate: refs,
                            object: ObjKey::Node((i + 1) % N as u64),
                        },
                    ),
                    (
                        0u32,
                        WriteKind::AddMany {
                            subject: i,
                            predicate: refs,
                            object: ObjKey::Node((i + 7) % N as u64),
                        },
                    ),
                ]
            })
            .collect();
        eng.write_batch(chunk).unwrap();
        eng.sync().unwrap();
    }
    eng.materialize();
    let mut snap = eng.snapshot();

    // 30 live queries: 1-hop expand of hot subjects
    let mut live = LiveQueries::new(LIVE_QUERIES);
    for j in 0..LIVE_QUERIES as u64 {
        let subj = j * 137 % N as u64;
        live.register(&snap, move |s| query::expand(s, subj, refs))
            .unwrap();
    }

    let principal = Principal {
        allowed_labels: 0b0111,
    }; // deny label 3 (~25% filtered)
    let queries = gen_vecs(256, 7, &ctr);
    let pipeline = |q: Vec<f32>| Pipeline {
        source: Source::TypeAnn {
            q,
            target_type: doc,
            k: K,
        },
        transforms: vec![Transform::Expand { predicate: refs }],
        max_nodes: 100,
        mode: ReadMode::Fresh,
    };

    println!("=== #20 C2b integrated open-loop (durable Engine + IVF-PQ + IVM) ===");
    println!(
        "build       : {build_s:.1}s  ({N} docs, {} seed edges, {LIVE_QUERIES} live queries)",
        2 * N
    );

    // Integrated read service latency (real ANN hybrid over the real durable snapshot), interleaved
    // with durable write chunks + IVM maintenance each epoch. We report intrinsic service latency +
    // sustained single-thread throughput; the per-epoch stall breakdown shows what limits scaling.
    let mut lat = Vec::with_capacity(EPOCHS * READS_PER_EPOCH);
    let (mut t_fsync, mut t_mat, mut t_snap, mut t_live) = (0.0f64, 0.0, 0.0, 0.0);
    let mut read_wall = 0.0f64;
    let mut live_diffs = 0usize;
    let mut next_doc = N as u64;
    let mut read_idx = 0usize;

    for epoch in 0..EPOCHS {
        // --- durable write + IVM stall (on the timeline) ---
        let edges: Vec<(u32, WriteKind)> = (0..WRITE_EDGES)
            .map(|k| {
                let s = ((epoch * WRITE_EDGES + k) as u64 * 2_654_435_761) % N as u64;
                let o = (s + 1 + k as u64) % N as u64;
                (
                    0u32,
                    WriteKind::AddMany {
                        subject: s,
                        predicate: refs,
                        object: ObjKey::Node(o),
                    },
                )
            })
            .collect();
        eng.write_batch(edges).unwrap();
        let a = Instant::now();
        eng.sync().unwrap(); // durable fsync
        t_fsync += a.elapsed().as_secs_f64() * 1e3;

        // incremental embeddings (async-arrival stand-in), seqno-stamped
        for d in 0..NEW_DOCS {
            let emb = &data[(next_doc as usize + d) % N];
            ivf.add(next_doc, next_doc, emb, (next_doc % 4) as u32);
            next_doc += 1;
        }
        let a = Instant::now();
        eng.materialize();
        t_mat += a.elapsed().as_secs_f64() * 1e3;
        let a = Instant::now();
        snap = eng.snapshot();
        t_snap += a.elapsed().as_secs_f64() * 1e3;
        let a = Instant::now();
        for (_, d) in live.on_change(&snap) {
            live_diffs += d.added.len() + d.removed.len();
        }
        t_live += a.elapsed().as_secs_f64() * 1e3;

        let vv = VersionVector::new(eng.durable_head(), ivf.len() as u64);

        // --- reads (real ANN hybrid) against this epoch's snapshot ---
        for _ in 0..READS_PER_EPOCH {
            let q = queries[read_idx % queries.len()].clone();
            let start = Instant::now();
            let _ = run(&snap, &cat, &ivf, &pipeline(q), &principal, vv);
            let s = start.elapsed().as_secs_f64();
            read_wall += s;
            lat.push(s * 1e3);
            read_idx += 1;
        }
    }

    // --- read latency breakdown (isolate ANN vs authz+type filter vs +expand/IR overhead) ---
    let vv = VersionVector::new(eng.durable_head(), ivf.len() as u64);
    let keep_cat = |n: u64| {
        cat.node_label(n).is_none_or(|l| principal.can_see_label(l))
            && cat.node_type(n) == Some(doc)
    };
    let mut b_ann = Vec::new();
    let mut b_filt = Vec::new();
    let mut b_full = Vec::new();
    for q in queries.iter().cycle().take(1000) {
        let t = Instant::now();
        let _ = ivf.search_rerank(q, K, 8, 256, None, |_| true, |_| true); // pure ANN
        b_ann.push(t.elapsed().as_secs_f64() * 1e3);
        let t = Instant::now();
        let _ = ivf.search_rerank(q, K, 8, 256, None, |_| true, keep_cat); // + authz/type filter
        b_filt.push(t.elapsed().as_secs_f64() * 1e3);
        let t = Instant::now();
        let _ = run(&snap, &cat, &ivf, &pipeline(q.to_vec()), &principal, vv); // + expand + IR
        b_full.push(t.elapsed().as_secs_f64() * 1e3);
    }
    for v in [&mut b_ann, &mut b_filt, &mut b_full] {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    }
    println!(
        "breakdown p99: pure-ANN {:.3}ms | +filter {:.3}ms | +expand/IR (full) {:.3}ms",
        pct(&b_ann, 0.99),
        pct(&b_filt, 0.99),
        pct(&b_full, 0.99)
    );

    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let total_writes = 2 * N + EPOCHS * WRITE_EDGES;
    println!(
        "read service: {} hybrid reads — p50={:.3}ms p99={:.3}ms p999={:.3}ms max={:.3}ms ({:.0} rps/thread)",
        lat.len(),
        pct(&lat, 0.50),
        pct(&lat, 0.99),
        pct(&lat, 0.999),
        lat[lat.len() - 1],
        lat.len() as f64 / read_wall
    );
    println!(
        "epoch stall : fsync {:.1}ms + materialize {:.1}ms + snapshot {:.1}ms + live {:.1}ms (total over {EPOCHS} epochs)",
        t_fsync, t_mat, t_snap, t_live
    );
    println!(
        "durable     : {} writes, {EPOCHS} fsyncs, durable_head={}",
        total_writes,
        eng.durable_head()
    );
    println!("live query  : {live_diffs} diffs delivered across {LIVE_QUERIES} queries");
    println!(
        "read p99 <2ms: {} (integrated: real ANN + expand over durable snapshot)",
        if pct(&lat, 0.99) < 2.0 {
            "PASS"
        } else {
            "FAIL"
        }
    );

    // --- 0 data loss: cold reopen recovers every synced write ---
    let durable_before = eng.durable_head();
    drop(eng);
    let reopened = Engine::open(&path, 8_000_000).unwrap();
    println!(
        "cold reopen : recovered durable_head={} (before={}) → {}",
        reopened.durable_head(),
        durable_before,
        if reopened.durable_head() == durable_before {
            "0 data loss ✅"
        } else {
            "DATA LOSS ✗"
        }
    );
    let _ = std::fs::remove_file(&path);
}
