//! #23: nprobe × (recall, p99) operating-point curve on *harder* data (heavily overlapping clusters ≈
//! a continuous manifold), where nprobe actually drives recall — unlike the well-separated synthetic
//! where nprobe=1 already suffices. Finds the minimum nprobe reaching recall@10 ≥ 0.9 and its warm p99,
//! deciding the operating point (recall≥0.9 AND p99<2ms).
//!
//! Run: `cargo run --release --example ann_nprobe_curve -p stromadb-core`

use std::time::Instant;
use stromadb_core::ivf::IvfPq;
use stromadb_core::vector::sqdist;

#[path = "util/mod.rs"]
mod util;
use util::{centers, gen_vecs, percentile};

const N: usize = 100_000;
const DIM: usize = 768;
const M: usize = 96;
const NLIST: usize = 256;
const TRAIN: usize = 20_000;
const NC: usize = 150; // few centers + large noise ⇒ clusters merge ⇒ neighbours spread across cells
const NOISE: f32 = 0.9;
const R: usize = 100;
const K: usize = 10;

fn exact_type0_topk(data: &[Vec<f32>], q: &[f32]) -> Vec<u64> {
    let mut d: Vec<(f32, u64)> = data
        .iter()
        .enumerate()
        .filter(|(i, _)| i.is_multiple_of(2))
        .map(|(i, v)| (sqdist(q, v), i as u64))
        .collect();
    d.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    d.into_iter().take(K).map(|(_, n)| n).collect()
}

fn main() {
    let ctr = centers(NC, DIM);
    let data = gen_vecs(N, 42, &ctr, NOISE);
    let mut idx = IvfPq::new(DIM, NLIST, M);
    idx.train(&data[..TRAIN]);
    idx.add_batch(
        data.iter()
            .enumerate()
            .map(|(i, v)| (i as u64, i as u64, v.clone(), (i % 2) as u32))
            .collect(),
    );
    println!(
        "=== #23 nprobe operating-point (harder data: NC={NC}, noise={NOISE}, {N}×{DIM}d, type-sel 50%) ==="
    );

    let queries = gen_vecs(200, 7, &ctr, NOISE);
    let truth: Vec<Vec<u64>> = queries.iter().map(|q| exact_type0_topk(&data, q)).collect();
    let warm = gen_vecs(3000, 123, &ctr, NOISE);
    let authz = |l: u32| l == 0;
    let keep = |n: u64| n.is_multiple_of(2);

    println!("  nprobe  recall@10   p50      p99      [recall≥0.9 & p99<2ms]");
    let mut op = None;
    for &np in &[1usize, 2, 4, 8, 16, 32, 64] {
        let mut rsum = 0.0;
        for (q, tr) in queries.iter().zip(&truth) {
            let got = idx.search_rerank(q, K, np, R, None, authz, keep);
            rsum += got.iter().filter(|(n, _)| tr.contains(n)).count() as f64 / K as f64;
        }
        let recall = rsum / queries.len() as f64;

        let mut lat: Vec<f64> = Vec::with_capacity(warm.len());
        for q in &warm {
            let t = Instant::now();
            let _ = idx.search_rerank(q, K, np, R, None, authz, keep);
            lat.push(t.elapsed().as_secs_f64() * 1e3);
        }
        lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p = |qq: f64| percentile(&lat, qq);
        let ok = recall >= 0.9 && p(0.99) < 2.0;
        if ok && op.is_none() {
            op = Some(np);
        }
        println!(
            "  {np:<6}  {recall:.3}       {:.3}ms  {:.3}ms  {}",
            p(0.50),
            p(0.99),
            if ok { "← operating point ✅" } else { "" }
        );
    }
    match op {
        Some(np) => {
            println!("operating point: nprobe={np} (recall@10≥0.9 AND authz-on warm p99<2ms)")
        }
        None => {
            println!("no nprobe meets both @ R={R} — recall may be candidate-limited; sweeping R…")
        }
    }

    // recall is plateauing below 0.9 → is it R-limited (non-residual PQ ranks true NN past top-R)?
    // Sweep rerank depth at a cheap nprobe. R only adds rerank reads (cheap, per #19) — not scan cost.
    println!("\n  nprobe=8, sweep R:   recall@10   p50      p99");
    for &r in &[100usize, 200, 400, 800, 1600] {
        let mut rsum = 0.0;
        for (q, tr) in queries.iter().zip(&truth) {
            let got = idx.search_rerank(q, K, 8, r, None, authz, keep);
            rsum += got.iter().filter(|(n, _)| tr.contains(n)).count() as f64 / K as f64;
        }
        let recall = rsum / queries.len() as f64;
        let mut lat: Vec<f64> = Vec::with_capacity(warm.len());
        for q in &warm {
            let t = Instant::now();
            let _ = idx.search_rerank(q, K, 8, r, None, authz, keep);
            lat.push(t.elapsed().as_secs_f64() * 1e3);
        }
        lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p = |qq: f64| percentile(&lat, qq);
        let mark = if recall >= 0.9 && p(0.99) < 2.0 {
            "← recall≥0.9 & p99<2ms ✅"
        } else {
            ""
        };
        println!(
            "  R={r:<5}             {recall:.3}       {:.3}ms  {:.3}ms  {mark}",
            p(0.50),
            p(0.99)
        );
    }
}
