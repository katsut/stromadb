use super::build::Rng;
use super::*;
use crate::vector::sqdist;
use std::collections::BTreeSet;

// clustered vectors (shared centers) so near-neighbours are well-defined, like real embeddings
fn clustered(n: usize, seed: u64, ctr: &[Vec<f32>], dim: usize) -> Vec<Vec<f32>> {
    let mut rng = Rng::new(seed);
    (0..n)
        .map(|_| {
            let c = &ctr[rng.below(ctr.len())];
            (0..dim)
                .map(|i| c[i] + (rng.next_u64() as f32 / u64::MAX as f32 - 0.5) * 0.7)
                .collect()
        })
        .collect()
}

fn centers(nc: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = Rng::new(seed);
    (0..nc)
        .map(|_| {
            (0..dim)
                .map(|_| (rng.next_u64() as f32 / u64::MAX as f32) * 2.0 - 1.0)
                .collect()
        })
        .collect()
}

fn exact_topk(data: &[Vec<f32>], q: &[f32], k: usize) -> BTreeSet<NodeId> {
    let mut d: Vec<(f32, NodeId)> = data
        .iter()
        .enumerate()
        .map(|(i, v)| (sqdist(q, v), i as NodeId))
        .collect();
    d.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    d.into_iter().take(k).map(|(_, n)| n).collect()
}

#[test]
fn rerank_recovers_recall_that_pure_pq_loses() {
    let dim = 64;
    let ctr = centers(200, dim, 5);
    let data = clustered(6000, dim as u64, &ctr, dim);
    let mut idx = IvfPq::new(dim, 64, 16);
    idx.train(&data);
    for (i, v) in data.iter().enumerate() {
        idx.add(i as NodeId, i as u64, v, 0);
    }
    let queries = clustered(40, 999, &ctr, dim);
    let k = 10;
    let mut pq = 0.0;
    let mut rr = 0.0;
    for q in &queries {
        let truth = exact_topk(&data, q, k);
        let a: BTreeSet<NodeId> = idx
            .search(q, k, 8, None, |_| true, |_| true)
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        let b: BTreeSet<NodeId> = idx
            .search_rerank(q, k, 8, 100, None, |_| true, |_| true)
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        pq += a.intersection(&truth).count() as f64 / k as f64;
        rr += b.intersection(&truth).count() as f64 / k as f64;
    }
    let (pq, rr) = (pq / queries.len() as f64, rr / queries.len() as f64);
    assert!(rr > pq, "rerank must beat pure PQ (pq={pq}, rerank={rr})");
    assert!(rr >= 0.9, "rerank recall@10 must reach the SLO (got {rr})");
}

#[test]
fn add_batch_matches_serial_add() {
    let dim = 32;
    let ctr = centers(80, dim, 9);
    let data = clustered(2000, 3, &ctr, dim);
    let mut a = IvfPq::new(dim, 32, 8);
    a.train(&data);
    let mut b = IvfPq::new(dim, 32, 8);
    b.train(&data);
    for (i, v) in data.iter().enumerate() {
        a.add(i as NodeId, i as u64, v, (i % 3) as u32);
    }
    b.add_batch(
        data.iter()
            .enumerate()
            .map(|(i, v)| (i as NodeId, i as u64, v.clone(), (i % 3) as u32))
            .collect(),
    );
    // identical index → identical search results
    let q = &data[7];
    assert_eq!(
        a.search_rerank(q, 10, 8, 100, None, |_| true, |_| true),
        b.search_rerank(q, 10, 8, 100, None, |_| true, |_| true)
    );
    assert_eq!(a.len(), b.len());
}

#[test]
fn two_level_coarse_preserves_recall() {
    // nlist >= 512 activates the 2-level coarse quantizer; recall must stay high vs exact.
    let dim = 32;
    let ctr = centers(400, dim, 7);
    let data = clustered(20_000, 2, &ctr, dim);
    let mut idx = IvfPq::new(dim, 512, 8);
    idx.train(&data);
    assert!(
        !idx.super_coarse.is_empty(),
        "2-level path should be active at nlist=512"
    );
    for (i, v) in data.iter().enumerate() {
        idx.add(i as NodeId, i as u64, v, 0);
    }
    let queries = clustered(40, 111, &ctr, dim);
    let k = 10;
    let mut rr = 0.0;
    for q in &queries {
        let truth = exact_topk(&data, q, k);
        let got: BTreeSet<NodeId> = idx
            .search_rerank(q, k, 16, 200, None, |_| true, |_| true)
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        rr += got.intersection(&truth).count() as f64 / k as f64;
    }
    let rr = rr / queries.len() as f64;
    assert!(
        rr >= 0.9,
        "2-level coarse recall@10 must stay ≥0.9 (got {rr})"
    );
}

#[test]
fn authz_label_scopes_out_unauthorized() {
    let dim = 16;
    let ctr = centers(20, dim, 2);
    let data = clustered(500, 7, &ctr, dim);
    let mut idx = IvfPq::new(dim, 16, 4);
    idx.train(&data);
    for (i, v) in data.iter().enumerate() {
        idx.add(i as NodeId, i as u64, v, (i % 2) as u32); // label 0 / 1
    }
    let res = idx.search_rerank(&data[0], 20, 16, 50, None, |l| l == 0, |_| true);
    assert!(
        res.iter().all(|(n, _)| n % 2 == 0),
        "unauthorized label leaked"
    );
    assert!(!res.is_empty());
}

#[test]
fn watermark_scopes_indexed_prefix() {
    let dim = 16;
    let ctr = centers(20, dim, 3);
    let data = clustered(300, 11, &ctr, dim);
    let mut idx = IvfPq::new(dim, 16, 4);
    idx.train(&data);
    for (i, v) in data.iter().enumerate() {
        idx.add(i as NodeId, i as u64, v, 0);
    }
    let res = idx.search(&data[0], 50, 16, Some(100), |_| true, |_| true);
    assert!(
        res.iter().all(|(n, _)| *n < 100),
        "watermark tail leaked into strict read"
    );
}

#[test]
fn complete_tail_covers_unprobed_cells() {
    let dim = 32;
    let ctr = centers(100, dim, 4);
    let data = clustered(3000, 1, &ctr, dim);
    let mut idx = IvfPq::new(dim, 64, 8);
    idx.train(&data);
    for (i, v) in data.iter().enumerate() {
        idx.add(i as NodeId, i as u64, v, 0);
    }
    // nprobe=1 may miss; full-budget completeness scores every matching posting (no truncation)
    let (_, _, truncated) = idx.search_complete(&data[0], 10, 1, None, |_| true, |_| true, 3000);
    assert!(!truncated, "full budget must not truncate");
    // a tiny budget forces truncation (the bounded knob)
    let (_, _, truncated_small) = idx.search_complete(&data[0], 10, 1, None, |_| true, |_| true, 1);
    assert!(truncated_small, "tiny budget must bound the tail");
}
