//! Vector index: pre-computed embeddings + nearest-neighbour search.
//!
//! Embeddings are **received** (the engine never embeds — no-LLM substrate). Each carries the
//! changelog `seqno` at which it became available; the index `watermark` is how far it has been
//! indexed. ANN (the production quantized index, IVF-PQ/DiskANN) only "sees" the indexed prefix
//! `seqno < watermark`; the un-indexed tail is brute-forced in fresh reads (closes split-brain).
//! This MVP uses exact distance (recall-complete) as the ANN stand-in behind the same scoped contract.

use std::collections::HashMap;

use crate::fact::NodeId;

/// Squared Euclidean distance (monotonic with Euclidean; cheaper).
pub fn sqdist(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

struct Entry {
    seqno: u64,
    emb: Vec<f32>,
}

/// A versioned store of received embeddings keyed by node.
pub struct VectorIndex {
    dim: usize,
    version: u64,
    watermark: u64,
    embeddings: HashMap<NodeId, Entry>,
}

impl VectorIndex {
    pub fn new(dim: usize) -> Self {
        VectorIndex {
            dim,
            version: 0,
            watermark: 0,
            embeddings: HashMap::new(),
        }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Index version. A model/dimension change starts a new version (mixed versions are rejected).
    pub fn version(&self) -> u64 {
        self.version
    }

    /// How far the index has caught up (entries with `seqno < watermark` are "indexed").
    pub fn watermark(&self) -> u64 {
        self.watermark
    }

    /// Advance the indexing watermark (monotonic).
    pub fn advance_watermark(&mut self, to: u64) {
        self.watermark = self.watermark.max(to);
    }

    pub fn len(&self) -> usize {
        self.embeddings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.embeddings.is_empty()
    }

    /// Insert (or replace) a node's embedding, tagged with the changelog seqno it became available at.
    /// Panics on dimension mismatch (caller contract).
    pub fn insert(&mut self, node: NodeId, seqno: u64, embedding: Vec<f32>) {
        assert_eq!(embedding.len(), self.dim, "embedding dimension mismatch");
        self.embeddings.insert(
            node,
            Entry {
                seqno,
                emb: embedding,
            },
        );
    }

    pub fn get(&self, node: NodeId) -> Option<&[f32]> {
        self.embeddings.get(&node).map(|e| e.emb.as_slice())
    }

    /// k nearest over all embeddings (exact = recall-complete; the "fresh" view).
    pub fn nearest(&self, q: &[f32], k: usize) -> Vec<(NodeId, f32)> {
        self.nearest_scoped(q, k, None, |_| true)
    }

    /// k nearest over all embeddings where `keep(node)` holds.
    pub fn nearest_filtered(
        &self,
        q: &[f32],
        k: usize,
        keep: impl Fn(NodeId) -> bool,
    ) -> Vec<(NodeId, f32)> {
        self.nearest_scoped(q, k, None, keep)
    }

    /// k nearest among entries with `seqno < max_seqno` (if `Some`) — the indexed-prefix view — or
    /// all entries (`None`) — the fresh view (indexed ∪ brute-force tail). `keep` post-filters
    /// (e.g. by ontology type). Deterministic tie-break by NodeId.
    pub fn nearest_scoped(
        &self,
        q: &[f32],
        k: usize,
        max_seqno: Option<u64>,
        keep: impl Fn(NodeId) -> bool,
    ) -> Vec<(NodeId, f32)> {
        let mut scored: Vec<(f32, NodeId)> = self
            .embeddings
            .iter()
            .filter(|(_, e)| max_seqno.is_none_or(|w| e.seqno < w))
            .filter(|(n, _)| keep(**n))
            .map(|(n, e)| (sqdist(q, &e.emb), *n))
            .collect();
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));
        scored.into_iter().take(k).map(|(d, n)| (n, d)).collect()
    }
}
