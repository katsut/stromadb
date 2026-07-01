//! #23: nprobe × (recall, p99) operating-point curve on *harder* data (heavily overlapping clusters ≈
//! a continuous manifold), where nprobe actually drives recall — unlike the well-separated synthetic
//! where nprobe=1 already suffices. Finds the minimum nprobe reaching recall@10 ≥ 0.9 and its warm p99,
//! deciding the differentiation operating point (recall≥0.9 AND p99<2ms).
//!
//! Run: `cargo run --release --example ann_nprobe_curve -p stroma-core`

use std::time::Instant;
use stroma_core::ivf::IvfPq;
use stroma_core::vector::sqdist;

const N: usize = 100_000;
const DIM: usize = 768;
const M: usize = 96;
const NLIST: usize = 256;
const TRAIN: usize = 20_000;
const NC: usize = 150; // few centers + large noise ⇒ clusters merge ⇒ neighbours spread across cells
const NOISE: f32 = 0.9;
const R: usize = 100;
const K: usize = 10;

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
    let ctr = centers();
    let data = gen_vecs(N, 42, &ctr);
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

    let queries = gen_vecs(200, 7, &ctr);
    let truth: Vec<Vec<u64>> = queries.iter().map(|q| exact_type0_topk(&data, q)).collect();
    let warm = gen_vecs(3000, 123, &ctr);
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
        let p = |qq: f64| lat[((lat.len() as f64 * qq) as usize).min(lat.len() - 1)];
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
        let p = |qq: f64| lat[((lat.len() as f64 * qq) as usize).min(lat.len() - 1)];
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
