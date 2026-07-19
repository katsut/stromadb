//! Shared bench utilities for the stroma-core examples: the deterministic PRNG, the clustered
//! vector generators every ANN/integration bench feeds on, and the percentile read. One copy —
//! examples are separate crate roots, so each includes this via `#[path = "util/mod.rs"] mod util;`
//! (a bare `examples/util.rs` would itself be auto-discovered as an example target and fail to
//! build without a `main`).

#![allow(dead_code)] // each example uses its own subset

/// One splitmix64 step scaled to [-1, 1) — the benches' deterministic float source.
pub fn splitmix(s: &mut u64) -> f32 {
    *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *s;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    ((z ^ (z >> 31)) as f32 / u64::MAX as f32) * 2.0 - 1.0
}

/// `nc` latent cluster centers of `dim` dims with a FIXED seed, so data and queries generated from
/// the same centers share one manifold (realistic embeddings: near-neighbours live in the same
/// cluster → PQ residuals stay small).
pub fn centers(nc: usize, dim: usize) -> Vec<Vec<f32>> {
    let mut s = 0xC0FF_EE00_1234_5678u64;
    (0..nc)
        .map(|_| (0..dim).map(|_| splitmix(&mut s)).collect())
        .collect()
}

/// Each vector = a random cluster center + `noise`-scaled jitter; `seed` picks clusters + noise.
pub fn gen_vecs(n: usize, seed: u64, ctr: &[Vec<f32>], noise: f32) -> Vec<Vec<f32>> {
    let nc = ctr.len();
    let mut s = seed;
    (0..n)
        .map(|_| {
            let c = &ctr[(splitmix(&mut s).abs() * nc as f32) as usize % nc];
            c.iter().map(|&x| x + splitmix(&mut s) * noise).collect()
        })
        .collect()
}

/// The `q`-quantile of an ASCENDING-sorted sample.
pub fn percentile(sorted: &[f64], q: f64) -> f64 {
    sorted[((sorted.len() as f64 * q) as usize).min(sorted.len() - 1)]
}
