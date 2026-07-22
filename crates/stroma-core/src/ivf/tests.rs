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
fn fit_tracks_distribution_drift() {
    let dim = 32;
    let ctr = centers(50, dim, 21);
    let mut idx = IvfPq::new(dim, 64, 8);
    idx.train(&clustered(3000, 100, &ctr, dim));
    // fresh index, no adds: no evidence of drift
    assert!((idx.fit().ratio - 1.0).abs() < 1e-6);
    // same distribution → ratio stays near 1 (out-of-sample, so slightly above)
    for (i, v) in clustered(2000, 200, &ctr, dim).iter().enumerate() {
        idx.add(i as NodeId, i as u64, v, 0);
    }
    let same = idx.fit();
    assert_eq!(same.adds, 2000);
    assert!(
        same.ratio < 1.3,
        "same-distribution ratio must stay ~1 (got {})",
        same.ratio
    );
    // shifted distribution → ratio blows past any reasonable threshold
    let far: Vec<Vec<f32>> = ctr
        .iter()
        .map(|c| c.iter().map(|x| x + 8.0).collect())
        .collect();
    let mut drifted = idx.fresh_like();
    for (i, v) in clustered(2000, 300, &far, dim).iter().enumerate() {
        drifted.add(i as NodeId, i as u64, v, 0);
    }
    assert!(
        drifted.fit().ratio > 2.0,
        "shifted-distribution ratio must rise (got {})",
        drifted.fit().ratio
    );
}

#[test]
fn fresh_like_reuses_quantizers_and_matches_search() {
    let dim = 32;
    let ctr = centers(80, dim, 9);
    let data = clustered(2000, 3, &ctr, dim);
    let mut a = IvfPq::new(dim, 32, 8);
    a.train(&data);
    let mut b = a.fresh_like();
    for (i, v) in data.iter().enumerate() {
        a.add(i as NodeId, i as u64, v, (i % 3) as u32);
    }
    b.add_batch(
        data.iter()
            .enumerate()
            .map(|(i, v)| (i as NodeId, i as u64, v.clone(), (i % 3) as u32))
            .collect(),
    );
    // same quantizers + same content → identical cells, codes, and results
    let q = &data[7];
    assert_eq!(
        a.search_rerank(q, 10, 8, 100, None, |_| true, |_| true),
        b.search_rerank(q, 10, 8, 100, None, |_| true, |_| true)
    );
    assert_eq!(a.len(), b.len());
    assert!((a.fit().ratio - b.fit().ratio).abs() < 1e-6);
}

#[test]
fn retraining_on_the_shifted_corpus_restores_recall() {
    // Quantizers trained on distribution A while the corpus is entirely B (= A shifted): PQ-only
    // recall degrades because cells concentrate and codebooks cover the wrong range. Retraining on
    // the actual corpus restores it — the payoff the fit ratio is there to trigger.
    let dim = 32;
    let ctr_a = centers(60, dim, 13);
    let ctr_b: Vec<Vec<f32>> = ctr_a
        .iter()
        .map(|c| c.iter().map(|x| x + 6.0).collect())
        .collect();
    let corpus = clustered(4000, 2, &ctr_b, dim);
    let queries = clustered(30, 5, &ctr_b, dim);
    let k = 10;

    let mut misfit = IvfPq::new(dim, 64, 8);
    misfit.train(&clustered(3000, 1, &ctr_a, dim));
    let mut retrained = IvfPq::new(dim, 64, 8);
    retrained.train(&corpus);
    for (i, v) in corpus.iter().enumerate() {
        misfit.add(i as NodeId, i as u64, v, 0);
        retrained.add(i as NodeId, i as u64, v, 0);
    }
    assert!(
        misfit.fit().ratio > 2.0,
        "misfit must be visible in the ratio"
    );
    assert!(retrained.fit().ratio < 1.3, "retrained fit must be healthy");

    let recall = |idx: &IvfPq| {
        let mut r = 0.0;
        for q in &queries {
            let truth = exact_topk(&corpus, q, k);
            let got: BTreeSet<NodeId> = idx
                .search(q, k, 4, None, |_| true, |_| true)
                .into_iter()
                .map(|(n, _)| n)
                .collect();
            r += got.intersection(&truth).count() as f64 / k as f64;
        }
        r / queries.len() as f64
    };
    let (bad, good) = (recall(&misfit), recall(&retrained));
    assert!(
        good - bad >= 0.1,
        "retraining must materially beat the misfit index (misfit={bad}, retrained={good})"
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
