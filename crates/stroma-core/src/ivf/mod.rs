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
//!
//! Module layout: `build` (training + ingestion) and `search` (ADC scoring + probe paths).

mod build;
mod search;
#[cfg(test)]
mod tests;

use crate::fact::NodeId;
use std::collections::HashMap;

const KSUB: usize = 256; // PQ centroids per subquantizer (fits one byte)
const COARSE_FANOUT: usize = 8; // 2-level probe gathers ~FANOUT×nprobe candidate cells, then picks nprobe

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

    #[inline]
    fn sub_codebook(&self, j: usize, c: usize) -> &[f32] {
        &self.codebooks[j][c * self.dsub..(c + 1) * self.dsub]
    }

    /// Raw vector for a node (the cold re-rank tier).
    fn raw_of(&self, node: NodeId) -> &[f32] {
        let row = self.row_of[&node] as usize;
        &self.raw[row * self.dim..(row + 1) * self.dim]
    }
}
