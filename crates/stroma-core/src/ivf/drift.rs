//! Quantizer-fit tracking — the observable behind drift-triggered retraining.
//!
//! The coarse quantizer and PQ codebooks are fitted once, to a training sample; every vector added
//! afterwards is squeezed into that partition whether it fits or not. The fit numbers make the
//! squeeze visible: [`IvfPq::train`] records the mean coarse-assignment distance over its sample
//! (the *trained* baseline), and every `add` accumulates the same distance for the live corpus —
//! a number the build path already computes and used to throw away. When the live/trained ratio
//! grows, the cells and codebooks no longer describe the data being searched, and recall decays
//! silently; callers watch [`IvfPq::fit`] and rebuild via [`IvfPq::fresh_like`] (cheap, reuses the
//! quantizers) or a fresh train (full retrain) when the ratio crosses their threshold.

use super::IvfPq;

/// Snapshot of quantizer fit: the training-time baseline vs the live corpus.
#[derive(Debug, Clone, Copy)]
pub struct FitReport {
    /// Mean coarse-assignment sqdist over the training sample (the baseline).
    pub trained: f32,
    /// Mean coarse-assignment sqdist over everything added since training.
    pub live: f32,
    /// `live / trained` — ~1.0 means the quantizers still describe the corpus; growth means drift.
    pub ratio: f32,
    /// Vectors added since training (the evidence behind `live`).
    pub adds: u64,
}

impl IvfPq {
    /// Current quantizer fit. With no adds yet there is no evidence of drift, so `live` echoes the
    /// baseline and the ratio is 1.0. A degenerate zero baseline (single-point sample) reports
    /// infinite ratio as soon as any add lands off the centroid — retraining a trivial index is free.
    pub fn fit(&self) -> FitReport {
        let trained = self.trained_fit;
        let live = if self.live_fit_n == 0 {
            trained
        } else {
            (self.live_fit_sum / self.live_fit_n as f64) as f32
        };
        let ratio = if trained > 0.0 {
            live / trained
        } else if live > 0.0 {
            f32::INFINITY
        } else {
            1.0
        };
        FitReport {
            trained,
            live,
            ratio,
            adds: self.live_fit_n,
        }
    }

    /// An empty index that reuses this one's trained quantizers (coarse cells, 2-level routing, PQ
    /// codebooks) and its fit baseline — the cheap rebuild path: re-adding content skips k-means
    /// entirely. The baseline carries over on purpose: drift is measured against the last *train*,
    /// not the last rebuild. Panics if this index is untrained.
    pub fn fresh_like(&self) -> IvfPq {
        assert!(!self.coarse.is_empty(), "index not trained");
        let mut idx = IvfPq::new(self.dim, self.nlist, self.m);
        idx.coarse = self.coarse.clone();
        idx.super_coarse = self.super_coarse.clone();
        idx.super_members = self.super_members.clone();
        idx.codebooks = self.codebooks.clone();
        idx.pq_ksub = self.pq_ksub;
        idx.lists = (0..self.nlist).map(|_| super::Cell::default()).collect();
        idx.trained_fit = self.trained_fit;
        idx
    }
}
