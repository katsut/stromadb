//! Hybrid-search SLO probe on the real IVF-PQ index at ~A1 representative scale.
//! Checks the locked DONE SLO hybrid-search leg: filtered recall@10 ≥ 0.9 @ type-selectivity 50%,
//! and authz-on warm hybrid p99 < 2ms. Also reports PQ compression (the A1 RAM load-bearing claim)
//! and recall-completeness with a bounded tail (H2).
//!
//! Run: `cargo run --release --example ann_slo -p stromadb-core`

use std::time::Instant;
use stromadb_core::ivf::IvfPq;
use stromadb_core::vector::sqdist;

#[path = "util/mod.rs"]
mod util;
use util::{centers, gen_vecs, percentile};

const N: usize = 200_000;
const DIM: usize = 768;
const M: usize = 96; // PQ subquantizers → 96 B/vec code
const NLIST: usize = 512;
const TRAIN_SAMPLE: usize = 20_000;
const NC: usize = 2000; // latent cluster centers (shared by data + queries)
const NOISE: f32 = 0.35; // within-cluster spread
const RERANK_R: usize = 100; // PQ candidates re-ranked by exact distance

fn exact_filtered_topk(
    data: &[Vec<f32>],
    q: &[f32],
    k: usize,
    is_type: &dyn Fn(usize) -> bool,
) -> Vec<u64> {
    let mut d: Vec<(f32, u64)> = data
        .iter()
        .enumerate()
        .filter(|(i, _)| is_type(*i))
        .map(|(i, v)| (sqdist(q, v), i as u64))
        .collect();
    d.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    d.into_iter().take(k).map(|(_, n)| n).collect()
}

fn main() {
    let k = 10;
    let is_type = |i: usize| i.is_multiple_of(2); // target type = 50% selectivity

    println!("=== hybrid-search SLO — real IVF-PQ ({N} vec × {DIM}d, type-sel 50%) ===");
    let t = Instant::now();
    let ctr = centers(NC, DIM);
    let data = gen_vecs(N, 42, &ctr, NOISE);
    println!("gen         : {:.1}s", t.elapsed().as_secs_f64());

    let t = Instant::now();
    let mut idx = IvfPq::new(DIM, NLIST, M);
    idx.train(&data[..TRAIN_SAMPLE]);
    idx.add_batch(
        data.iter()
            .enumerate()
            .map(|(i, v)| (i as u64, i as u64, v.clone(), (i % 2) as u32)) // label = type here
            .collect(),
    );
    println!(
        "build       : {:.1}s  (train {TRAIN_SAMPLE}, add_batch {N}, nlist={})",
        t.elapsed().as_secs_f64(),
        idx.nlist()
    );

    println!(
        "footprint   : hot codes {:.0} MB ({M} B/vec) + cold raw {:.0} MB ({:.0}× compression, raw→SSD-able)",
        idx.code_bytes() as f64 / 1e6,
        idx.raw_bytes() as f64 / 1e6,
        idx.raw_bytes() as f64 / idx.code_bytes() as f64
    );

    // --- filtered recall@10 vs nprobe (type filter, authz allow-all) ---
    let queries = gen_vecs(200, 7, &ctr, NOISE);
    let truth: Vec<Vec<u64>> = queries
        .iter()
        .map(|q| exact_filtered_topk(&data, q, k, &is_type))
        .collect();
    let recall_pq = |nprobe: usize| -> f64 {
        let mut sum = 0.0;
        for (q, tr) in queries.iter().zip(&truth) {
            let got = idx.search(q, k, nprobe, None, |_| true, |n| n.is_multiple_of(2));
            sum += got.iter().filter(|(n, _)| tr.contains(n)).count() as f64 / k as f64;
        }
        sum / queries.len() as f64
    };
    let recall_rerank = |nprobe: usize| -> f64 {
        let mut sum = 0.0;
        for (q, tr) in queries.iter().zip(&truth) {
            let got = idx.search_rerank(
                q,
                k,
                nprobe,
                RERANK_R,
                None,
                |_| true,
                |n| n.is_multiple_of(2),
            );
            sum += got.iter().filter(|(n, _)| tr.contains(n)).count() as f64 / k as f64;
        }
        sum / queries.len() as f64
    };
    println!("filtered recall@10  [pure-PQ | +rerank(R={RERANK_R})]:");
    let mut chosen = 0usize;
    for &np in &[1usize, 4, 8, 16, 32] {
        let rp = recall_pq(np);
        let rr = recall_rerank(np);
        let mark = if rr >= 0.9 { " ← ≥0.9 ✅" } else { "" };
        println!("  nprobe={np:<3} pure={rp:.3}  rerank={rr:.3}{mark}");
        if rr >= 0.9 && chosen == 0 {
            chosen = np;
        }
    }

    // --- warm p99 with authz + type filters active + rerank, at the chosen nprobe ---
    let np = if chosen == 0 { 32 } else { chosen };
    let warm = gen_vecs(3000, 123, &ctr, NOISE);
    let mut lat: Vec<f64> = Vec::with_capacity(warm.len());
    for q in &warm {
        let t = Instant::now();
        let _ = idx.search_rerank(
            q,
            k,
            np,
            RERANK_R,
            None,
            |l| l == 0,
            |n| n.is_multiple_of(2),
        ); // authz ON + type
        lat.push(t.elapsed().as_secs_f64() * 1e3); // ms
    }
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |q: f64| percentile(&lat, q);
    println!(
        "warm hybrid (authz ON, nprobe={np}, +rerank): p50={:.3}ms p99={:.3}ms max={:.3}ms  [SLO p99<2ms] → {}",
        p(0.50),
        p(0.99),
        lat[lat.len() - 1],
        if p(0.99) < 2.0 { "PASS" } else { "FAIL" }
    );

    // --- recall-completeness (H2): probed ∪ bounded brute-force tail ---
    let mut comp = 0.0;
    let mut trunc = 0;
    for (q, tr) in queries.iter().zip(&truth) {
        let (got, _, truncated) =
            idx.search_complete(q, k, 4, None, |_| true, |n| n.is_multiple_of(2), N);
        if truncated {
            trunc += 1;
        }
        let hit = got.iter().filter(|(n, _)| tr.contains(n)).count();
        comp += hit as f64 / k as f64;
    }
    println!(
        "recall-complete (nprobe=4 ∪ full tail): {:.3}  (truncated {trunc}/{})",
        comp / queries.len() as f64,
        queries.len()
    );
}
