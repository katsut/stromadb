//! Training and ingestion: the deterministic PRNG, k-means, and the train / encode / add paths.

use super::{Cell, IvfPq, KSUB};
use crate::fact::NodeId;
use crate::vector::sqdist;

/// Deterministic splitmix64 PRNG — reproducible training without a `rand` dependency.
pub(super) struct Rng(u64);

impl Rng {
    pub(super) fn new(seed: u64) -> Self {
        Rng(seed)
    }
    pub(super) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub(super) fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Map `f` over `items` across available CPUs (scoped threads — no external dep), order-preserving.
/// Falls back to serial for small inputs. Used for the embarrassingly-parallel build hot paths
/// (k-means assignment, per-vector assign+encode).
pub(super) fn par_map<T, U, F>(items: &[T], f: F) -> Vec<U>
where
    T: Sync,
    U: Send,
    F: Fn(&T) -> U + Sync,
{
    let n = items.len();
    let threads = std::thread::available_parallelism()
        .map(|x| x.get())
        .unwrap_or(1)
        .min(n)
        .max(1);
    if threads == 1 || n < 2048 {
        return items.iter().map(&f).collect();
    }
    let chunk = n.div_ceil(threads);
    std::thread::scope(|s| {
        let handles: Vec<_> = items
            .chunks(chunk)
            .map(|ch| s.spawn(|| ch.iter().map(&f).collect::<Vec<U>>()))
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    })
}

pub(super) fn nearest_centroid(x: &[f32], centroids: &[Vec<f32>]) -> (usize, f32) {
    let mut best = 0usize;
    let mut best_d = f32::INFINITY;
    for (i, c) in centroids.iter().enumerate() {
        let d = sqdist(x, c);
        if d < best_d {
            best_d = d;
            best = i;
        }
    }
    (best, best_d)
}

/// Lloyd's k-means. Returns exactly `min(k, points.len())` centroids (never empty). Deterministic
/// given `rng`. Empty clusters are re-seeded to a random point so `k` is always honoured.
pub(super) fn kmeans(points: &[&[f32]], k: usize, iters: usize, rng: &mut Rng) -> Vec<Vec<f32>> {
    let n = points.len();
    let d = points[0].len();
    let k = k.min(n).max(1);
    let mut centroids: Vec<Vec<f32>> = (0..k).map(|_| points[rng.below(n)].to_vec()).collect();

    for _ in 0..iters {
        // assignment is the O(k·d) hot loop — parallelize it; accumulation is cheap and stays serial
        let assigns = par_map(points, |&p| nearest_centroid(p, &centroids).0);
        let mut sums = vec![vec![0f32; d]; k];
        let mut counts = vec![0usize; k];
        for (p, &c) in points.iter().zip(&assigns) {
            for i in 0..d {
                sums[c][i] += p[i];
            }
            counts[c] += 1;
        }
        for c in 0..k {
            if counts[c] > 0 {
                for i in 0..d {
                    centroids[c][i] = sums[c][i] / counts[c] as f32;
                }
            } else {
                centroids[c] = points[rng.below(n)].to_vec(); // re-seed dead cluster
            }
        }
    }
    centroids
}

impl IvfPq {
    /// Train the coarse quantizer and PQ codebooks from a representative sample. Must be called once
    /// before [`IvfPq::add`]. `nlist` may shrink if the sample is smaller than requested.
    pub fn train(&mut self, sample: &[Vec<f32>]) {
        assert!(!sample.is_empty(), "cannot train on empty sample");
        let refs: Vec<&[f32]> = sample.iter().map(|v| v.as_slice()).collect();
        let mut rng = Rng::new(0xA5A5_1234_DEAD_BEEF);

        self.coarse = kmeans(&refs, self.nlist, 12, &mut rng);
        self.nlist = self.coarse.len();
        self.lists = (0..self.nlist).map(|_| Cell::default()).collect();

        // 2-level coarse quantizer (#32): for large nlist, cluster the coarse centroids into ~√nlist
        // super-centroids so probe_cells routes via the supers (sub-linear) instead of scanning all
        // nlist centroids (which was ~1ms at nlist≈2828 — the 0.5M read-p99 blocker). Small nlist keeps
        // the plain linear scan (cheaper than two levels).
        self.super_coarse = Vec::new();
        self.super_members = Vec::new();
        if self.nlist >= 512 {
            let s = (self.nlist as f64).sqrt() as usize;
            let crefs: Vec<&[f32]> = self.coarse.iter().map(|c| c.as_slice()).collect();
            let supers = kmeans(&crefs, s, 10, &mut rng);
            let mut members = vec![Vec::new(); supers.len()];
            for (ci, c) in self.coarse.iter().enumerate() {
                let (si, _) = nearest_centroid(c, &supers);
                members[si].push(ci as u32);
            }
            self.super_coarse = supers;
            self.super_members = members;
        }

        // Fit baseline: the mean coarse-assignment distance over the training sample. Every later
        // add accumulates the same number for the live corpus; the ratio is the drift observable.
        let coarse = &self.coarse;
        let dists = par_map(&refs, |&p| nearest_centroid(p, coarse).1);
        self.trained_fit =
            (dists.iter().map(|&d| d as f64).sum::<f64>() / dists.len() as f64) as f32;
        self.live_fit_sum = 0.0;
        self.live_fit_n = 0;

        // PQ codebooks are trained on RAW sub-vectors (non-residual). Because re-rank restores exact
        // recall, PQ only has to rank candidates, so we trade a little candidate precision for a huge
        // query-time win: the ADC table becomes cell-independent (computed once per query, not per
        // probed cell), so candidate-gen cost stops scaling with nprobe.
        self.pq_ksub = KSUB.min(refs.len()).max(1);
        self.codebooks.clear();
        for j in 0..self.m {
            let subs: Vec<&[f32]> = refs
                .iter()
                .map(|r| &r[j * self.dsub..(j + 1) * self.dsub])
                .collect();
            let cents = kmeans(&subs, self.pq_ksub, 12, &mut rng);
            let mut flat = vec![0f32; self.pq_ksub * self.dsub];
            for (c, cen) in cents.iter().enumerate() {
                flat[c * self.dsub..(c + 1) * self.dsub].copy_from_slice(cen);
            }
            self.codebooks.push(flat);
        }
    }

    /// Encode a (raw, non-residual) vector into `m` PQ code bytes (allocation-free inner loop).
    fn encode(&self, x: &[f32]) -> Vec<u8> {
        let mut code = vec![0u8; self.m];
        for (j, slot) in code.iter_mut().enumerate() {
            let off = j * self.dsub;
            let mut best = 0usize;
            let mut best_d = f32::INFINITY;
            for c in 0..self.pq_ksub {
                let cb = self.sub_codebook(j, c);
                let mut d = 0f32;
                for i in 0..self.dsub {
                    let r = x[off + i] - cb[i];
                    d += r * r;
                }
                if d < best_d {
                    best_d = d;
                    best = c;
                }
            }
            *slot = best as u8;
        }
        code
    }

    /// Add a received embedding, tagged with the changelog `seqno` it became available at and its
    /// authz `label`. Panics on dimension mismatch or if the index is untrained.
    pub fn add(&mut self, node: NodeId, seqno: u64, embedding: &[f32], label: u32) {
        assert_eq!(embedding.len(), self.dim, "embedding dimension mismatch");
        assert!(!self.coarse.is_empty(), "index not trained");
        let (cell, dist) = nearest_centroid(embedding, &self.coarse);
        self.live_fit_sum += dist as f64;
        self.live_fit_n += 1;
        let code = self.encode(embedding);
        self.lists[cell].push(node, seqno, label, &code);
        self.row_of.insert(node, (self.raw.len() / self.dim) as u32);
        self.raw.extend_from_slice(embedding);
        self.ntotal += 1;
    }

    /// Bulk-add embeddings. The per-vector assign + PQ-encode (the build hot path) runs in parallel
    /// across CPUs; insertion into the inverted lists is serial. Equivalent to calling [`IvfPq::add`]
    /// for each item in order. Panics on dimension mismatch or if the index is untrained.
    pub fn add_batch(&mut self, items: Vec<(NodeId, u64, Vec<f32>, u32)>) {
        assert!(!self.coarse.is_empty(), "index not trained");
        let computed: Vec<(usize, f32, Vec<u8>)> = {
            let this = &*self; // immutable borrow for the parallel phase
            par_map(&items, |(_, _, emb, _)| {
                assert_eq!(emb.len(), this.dim, "embedding dimension mismatch");
                let (cell, dist) = nearest_centroid(emb, &this.coarse);
                (cell, dist, this.encode(emb))
            })
        };
        for ((node, seqno, emb, label), (cell, dist, code)) in items.into_iter().zip(computed) {
            self.live_fit_sum += dist as f64;
            self.live_fit_n += 1;
            self.lists[cell].push(node, seqno, label, &code);
            self.row_of.insert(node, (self.raw.len() / self.dim) as u32);
            self.raw.extend_from_slice(&emb);
            self.ntotal += 1;
        }
    }
}
