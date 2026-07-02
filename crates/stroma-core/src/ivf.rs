//! Real IVF-PQ approximate nearest-neighbour index with exact re-ranking — the production vector
//! backend that replaces the exact stand-in (`VectorIndex`) behind the same scoped contract.
//!
//! - **IVF** (inverted file): a coarse quantizer partitions vectors into `nlist` cells; a query probes
//!   only its `nprobe` nearest cells. The approximation knob — more probes → higher recall, more work.
//! - **PQ** (product quantization, **non-residual**): each raw vector is split into `m` subvectors,
//!   each encoded to one byte against a 256-entry codebook (asymmetric distance / ADC). A 768-dim f32
//!   vector (3072 B) becomes `m` bytes — the compression that fits the A1 vector footprint in RAM (raw
//!   1.5 GB → ~48 MB @ m=96). This is the **hot** tier. Non-residual (vs classic IVFADC residual) makes
//!   the ADC table cell-independent → computed once per query, so candidate-gen cost stops scaling with
//!   `nprobe` (the p99 driver, #19). Codes are stored struct-of-arrays per cell for cache locality.
//! - **Exact re-rank**: PQ ranking alone caps recall@10 below 0.9 in high dim (quantization error
//!   swamps fine ranking; non-residual caps lower still). [`IvfPq::search_rerank`] generates top-
//!   `rerank_r` candidates with PQ (cheap), then re-scores just those with the **raw** vectors —
//!   recall@10 → ~1.0. Re-rank depth `rerank_r` is the cheap recall lever (raw reads ~0.3 ms, #19), so
//!   the operating point is a moderate `nprobe` + generous `rerank_r` (measured: nprobe=8, R=256 →
//!   recall ~1.0, warm p99 <1 ms even on overlapping-cluster data). Raw is the **cold** tier (in-RAM
//!   here; mmap/SSD-backed later — A1 sizing).
//!
//! Three orthogonal scoping axes ride on top, matching the frozen contracts:
//! - **seqno watermark** (H3 / version vector): `max_seqno` reads the indexed prefix (strict) or all.
//! - **authz** (H4): postings carry an authz label; unauthorized postings are skipped *before* scoring
//!   (structurally scoped — no distance is computed for them, so no timing/completeness leak).
//! - **recall completeness** (H2): [`IvfPq::search_complete`] merges the probed result with a bounded
//!   brute-force over matching postings in *unprobed* cells (`ANN(probed) ∪ brute-force(unprobed)`).

use crate::fact::NodeId;
use crate::vector::sqdist;
use std::collections::HashMap;

const KSUB: usize = 256; // PQ centroids per subquantizer (fits one byte)
const COARSE_FANOUT: usize = 8; // 2-level probe gathers ~FANOUT×nprobe candidate cells, then picks nprobe

/// Deterministic splitmix64 PRNG — reproducible training without a `rand` dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Map `f` over `items` across available CPUs (scoped threads — no external dep), order-preserving.
/// Falls back to serial for small inputs. Used for the embarrassingly-parallel build hot paths
/// (k-means assignment, per-vector assign+encode).
fn par_map<T, U, F>(items: &[T], f: F) -> Vec<U>
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

fn nearest_centroid(x: &[f32], centroids: &[Vec<f32>]) -> (usize, f32) {
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
fn kmeans(points: &[&[f32]], k: usize, iters: usize, rng: &mut Rng) -> Vec<Vec<f32>> {
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

/// One inverted list, struct-of-arrays for cache-friendly scanning: posting `i` is
/// `codes[i*m..(i+1)*m]` + `nodes[i]`/`seqnos[i]`/`labels[i]`. Contiguous codes keep the ADC hot loop
/// off the heap-pointer chase a `Vec<Vec<u8>>` would cause.
#[derive(Default)]
struct Cell {
    codes: Vec<u8>, // flat, m bytes per posting
    nodes: Vec<NodeId>,
    seqnos: Vec<u64>,
    labels: Vec<u32>,
}

impl Cell {
    fn push(&mut self, node: NodeId, seqno: u64, label: u32, code: &[u8]) {
        self.codes.extend_from_slice(code);
        self.nodes.push(node);
        self.seqnos.push(seqno);
        self.labels.push(label);
    }
}

/// Trained IVF-PQ index. Build with [`IvfPq::train`] on a sample, then [`IvfPq::add`] every vector.
pub struct IvfPq {
    dim: usize,
    m: usize,    // number of subquantizers
    dsub: usize, // dim / m
    nlist: usize,
    coarse: Vec<Vec<f32>>,        // nlist × dim
    super_coarse: Vec<Vec<f32>>, // 2-level coarse quantizer: ~√nlist super-centroids (empty if small)
    super_members: Vec<Vec<u32>>, // per super-centroid: the coarse cell indices assigned to it
    codebooks: Vec<Vec<f32>>,    // m × (pq_ksub × dsub), flattened
    pq_ksub: usize,              // centroids per subquantizer (≤ KSUB)
    lists: Vec<Cell>,            // nlist inverted lists (hot: PQ codes, SoA)
    raw: Vec<f32>,               // cold tier: raw vectors, flat (ntotal × dim), for exact re-rank
    row_of: HashMap<NodeId, u32>,
    ntotal: usize,
}

impl IvfPq {
    /// A reasonable `nlist` for `n` vectors (~4·√n, clamped). Cells this size keep a query's probed
    /// postings bounded — cell imbalance from too-small `nlist` was the integrated-read p99 driver
    /// (#30: at 100K, nlist 256→1024 cut read p99 3.1→1.6ms). Callers should scale `nlist` with `n`.
    pub fn suggested_nlist(n: usize) -> usize {
        ((4.0 * (n as f64).sqrt()) as usize).clamp(16, 65_536)
    }

    /// Create an untrained index. `dim` must be divisible by `m`.
    pub fn new(dim: usize, nlist: usize, m: usize) -> Self {
        assert!(m > 0 && dim.is_multiple_of(m), "dim must be divisible by m");
        IvfPq {
            dim,
            m,
            dsub: dim / m,
            nlist: nlist.max(1),
            coarse: Vec::new(),
            super_coarse: Vec::new(),
            super_members: Vec::new(),
            codebooks: Vec::new(),
            pq_ksub: 0,
            lists: Vec::new(),
            raw: Vec::new(),
            row_of: HashMap::new(),
            ntotal: 0,
        }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }
    pub fn len(&self) -> usize {
        self.ntotal
    }
    pub fn is_empty(&self) -> bool {
        self.ntotal == 0
    }
    pub fn nlist(&self) -> usize {
        self.nlist
    }
    /// Bytes held by PQ codes — the hot compressed footprint (`ntotal × m`).
    pub fn code_bytes(&self) -> usize {
        self.ntotal * self.m
    }
    /// Bytes held by raw vectors — the cold re-rank tier (`ntotal × dim × 4`; mmap/SSD-able).
    pub fn raw_bytes(&self) -> usize {
        self.raw.len() * 4
    }

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

    #[inline]
    fn sub_codebook(&self, j: usize, c: usize) -> &[f32] {
        &self.codebooks[j][c * self.dsub..(c + 1) * self.dsub]
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
        let (cell, _) = nearest_centroid(embedding, &self.coarse);
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
        let computed: Vec<(usize, Vec<u8>)> = {
            let this = &*self; // immutable borrow for the parallel phase
            par_map(&items, |(_, _, emb, _)| {
                assert_eq!(emb.len(), this.dim, "embedding dimension mismatch");
                let (cell, _) = nearest_centroid(emb, &this.coarse);
                (cell, this.encode(emb))
            })
        };
        for ((node, seqno, emb, label), (cell, code)) in items.into_iter().zip(computed) {
            self.lists[cell].push(node, seqno, label, &code);
            self.row_of.insert(node, (self.raw.len() / self.dim) as u32);
            self.raw.extend_from_slice(&emb);
            self.ntotal += 1;
        }
    }

    /// Raw vector for a node (the cold re-rank tier).
    fn raw_of(&self, node: NodeId) -> &[f32] {
        let row = self.row_of[&node] as usize;
        &self.raw[row * self.dim..(row + 1) * self.dim]
    }

    /// Flat ADC table for query `q`: `m × pq_ksub` sub-distances, `score(code) = Σ_j table[j*ksub+code[j]]`.
    /// Cell-independent (non-residual PQ), so it is computed **once per query** and reused across every
    /// probed cell — candidate-gen cost no longer scales with `nprobe`.
    fn adc_table(&self, q: &[f32]) -> Vec<f32> {
        let mut table = vec![0f32; self.m * self.pq_ksub];
        for j in 0..self.m {
            let off = j * self.dsub;
            for c in 0..self.pq_ksub {
                let cb = self.sub_codebook(j, c);
                let mut d = 0f32;
                for i in 0..self.dsub {
                    let r = q[off + i] - cb[i];
                    d += r * r;
                }
                table[j * self.pq_ksub + c] = d;
            }
        }
        table
    }

    /// ADC score = Σ_j table[j·ksub + code_j]. The loop is gather-bound (a table lookup per code
    /// byte); NEON has no gather, so the win is instruction-level parallelism — four independent
    /// accumulators break the add dependency chain and let loads pipeline, and `chunks_exact`
    /// removes per-element bounds checks.
    #[inline]
    fn adc_score(&self, table: &[f32], code: &[u8]) -> f32 {
        let ksub = self.pq_ksub;
        let (mut s0, mut s1, mut s2, mut s3) = (0f32, 0f32, 0f32, 0f32);
        let mut off = 0usize;
        let mut it = code.chunks_exact(4);
        for ch in &mut it {
            s0 += table[off + ch[0] as usize];
            s1 += table[off + ksub + ch[1] as usize];
            s2 += table[off + 2 * ksub + ch[2] as usize];
            s3 += table[off + 3 * ksub + ch[3] as usize];
            off += 4 * ksub;
        }
        for &c in it.remainder() {
            s0 += table[off + c as usize];
            off += ksub;
        }
        (s0 + s1) + (s2 + s3)
    }

    /// The `nprobe` cells nearest to `q`, closest first. With the 2-level coarse quantizer (#32) this
    /// routes via the super-centroids — gather the coarse cells under the nearest supers until there
    /// are enough candidates, then pick the `nprobe` nearest among *those* — so the per-query coarse
    /// work is ~√nlist + a bounded candidate set instead of a full O(nlist) scan. Exact re-rank absorbs
    /// the small routing approximation. Falls back to a linear scan for small nlist.
    fn probe_cells(&self, q: &[f32], nprobe: usize) -> Vec<usize> {
        let np = nprobe.max(1);
        if self.super_coarse.is_empty() {
            let mut cd: Vec<(f32, usize)> = self
                .coarse
                .iter()
                .enumerate()
                .map(|(i, c)| (sqdist(q, c), i))
                .collect();
            cd.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            return cd.into_iter().take(np).map(|(_, i)| i).collect();
        }
        // level 1: rank super-centroids by distance to q
        let mut sd: Vec<(f32, usize)> = self
            .super_coarse
            .iter()
            .enumerate()
            .map(|(i, c)| (sqdist(q, c), i))
            .collect();
        sd.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        // level 2: gather coarse cells from the nearest supers until we have a generous candidate pool
        let target = (np * COARSE_FANOUT).min(self.nlist);
        let mut cand: Vec<usize> = Vec::with_capacity(target + 64);
        for (_, si) in sd {
            for &ci in &self.super_members[si] {
                cand.push(ci as usize);
            }
            if cand.len() >= target {
                break;
            }
        }
        // pick the np nearest coarse cells among the gathered candidates
        let mut cd: Vec<(f32, usize)> = cand
            .into_iter()
            .map(|ci| (sqdist(q, &self.coarse[ci]), ci))
            .collect();
        cd.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        cd.into_iter().take(np).map(|(_, i)| i).collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_cell(
        &self,
        table: &[f32],
        cell: usize,
        max_seqno: Option<u64>,
        allowed_label: &impl Fn(u32) -> bool,
        keep: &impl Fn(NodeId) -> bool,
        out: &mut Vec<(f32, NodeId)>,
        scored: &mut usize,
    ) {
        let c = &self.lists[cell];
        let rows = c
            .codes
            .chunks_exact(self.m)
            .zip(&c.nodes)
            .zip(&c.seqnos)
            .zip(&c.labels);
        for (((code, &node), &seqno), &label) in rows {
            if max_seqno.is_some_and(|w| seqno >= w) {
                continue; // strict watermark: indexed prefix only
            }
            if !allowed_label(label) {
                continue; // authz: never score unauthorized postings
            }
            if !keep(node) {
                continue; // graph type / arbitrary filter
            }
            out.push((self.adc_score(table, code), node));
            *scored += 1;
        }
    }

    fn topk(mut scored: Vec<(f32, NodeId)>, k: usize) -> Vec<(NodeId, f32)> {
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));
        scored.into_iter().take(k).map(|(d, n)| (n, d)).collect()
    }

    /// Approximate top-k by PQ (ADC) over the `nprobe` nearest cells — the "probed" view. Recall rises
    /// with `nprobe`. Filters: `max_seqno` (watermark), `allowed_label` (authz), `keep` (type). PQ-only
    /// ranking; for high recall use [`IvfPq::search_rerank`].
    pub fn search(
        &self,
        q: &[f32],
        k: usize,
        nprobe: usize,
        max_seqno: Option<u64>,
        allowed_label: impl Fn(u32) -> bool,
        keep: impl Fn(NodeId) -> bool,
    ) -> Vec<(NodeId, f32)> {
        let table = self.adc_table(q); // once per query — reused across all probed cells
        let mut out = Vec::new();
        let mut scored = 0;
        for cell in self.probe_cells(q, nprobe) {
            self.scan_cell(
                &table,
                cell,
                max_seqno,
                &allowed_label,
                &keep,
                &mut out,
                &mut scored,
            );
        }
        Self::topk(out, k)
    }

    /// High-recall top-k: generate `rerank_r` PQ candidates (cheap, hot codes), then re-score just
    /// those by *exact* distance over the raw (cold) tier. Returns exact distances. This is the
    /// production read path — PQ error no longer bounds recall.
    #[allow(clippy::too_many_arguments)]
    pub fn search_rerank(
        &self,
        q: &[f32],
        k: usize,
        nprobe: usize,
        rerank_r: usize,
        max_seqno: Option<u64>,
        allowed_label: impl Fn(u32) -> bool,
        keep: impl Fn(NodeId) -> bool,
    ) -> Vec<(NodeId, f32)> {
        let cand = self.search(q, rerank_r.max(k), nprobe, max_seqno, allowed_label, keep);
        let exact: Vec<(f32, NodeId)> = cand
            .into_iter()
            .map(|(n, _)| (sqdist(q, self.raw_of(n)), n))
            .collect();
        Self::topk(exact, k)
    }

    /// Recall-complete top-k (H2): the probed result unioned with a bounded brute-force over matching
    /// postings in *unprobed* cells. `tail_budget` caps how many extra postings may be scored — if the
    /// matching tail fits the budget, coverage is complete; otherwise the tail is bounded (the knob).
    /// Returns `(results, tail_scored, tail_truncated)` so callers can see whether completeness held.
    /// (Ranking is still PQ; compose with re-rank for exact ordering of the union.)
    #[allow(clippy::too_many_arguments)]
    pub fn search_complete(
        &self,
        q: &[f32],
        k: usize,
        nprobe: usize,
        max_seqno: Option<u64>,
        allowed_label: impl Fn(u32) -> bool,
        keep: impl Fn(NodeId) -> bool,
        tail_budget: usize,
    ) -> (Vec<(NodeId, f32)>, usize, bool) {
        let table = self.adc_table(q); // once per query
        let probed: std::collections::HashSet<usize> =
            self.probe_cells(q, nprobe).into_iter().collect();
        let mut out = Vec::new();
        let mut scored = 0;
        for &cell in &probed {
            self.scan_cell(
                &table,
                cell,
                max_seqno,
                &allowed_label,
                &keep,
                &mut out,
                &mut scored,
            );
        }
        let mut tail_scored = 0;
        let mut truncated = false;
        for cell in 0..self.nlist {
            if probed.contains(&cell) {
                continue;
            }
            let before = out.len();
            let mut cell_scored = 0;
            self.scan_cell(
                &table,
                cell,
                max_seqno,
                &allowed_label,
                &keep,
                &mut out,
                &mut cell_scored,
            );
            tail_scored += cell_scored;
            if tail_scored > tail_budget {
                out.truncate(before); // drop the overflowing cell wholesale — bounded, not partial
                truncated = true;
                break;
            }
        }
        (Self::topk(out, k), tail_scored, truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let (_, _, truncated) =
            idx.search_complete(&data[0], 10, 1, None, |_| true, |_| true, 3000);
        assert!(!truncated, "full budget must not truncate");
        // a tiny budget forces truncation (the bounded knob)
        let (_, _, truncated_small) =
            idx.search_complete(&data[0], 10, 1, None, |_| true, |_| true, 1);
        assert!(truncated_small, "tiny budget must bound the tail");
    }
}
