//! Vector index: pre-computed embeddings + nearest-neighbour search.
//!
//! Embeddings are **received** (the engine never embeds — no-LLM substrate). This MVP uses exact
//! distance (recall-complete) as a stand-in for the production quantized ANN index (IVF-PQ/DiskANN),
//! which slots in behind the same `nearest` contract. Versioning/watermark and the brute-force tail
//! that closes index/structure split-brain are wired in Epic 4 (cross-store snapshot).

use std::collections::HashMap;

use crate::fact::NodeId;

/// Squared Euclidean distance (monotonic with Euclidean; cheaper).
pub fn sqdist(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// A versioned store of received embeddings keyed by node.
pub struct VectorIndex {
    dim: usize,
    version: u64,
    embeddings: HashMap<NodeId, Vec<f32>>,
}

impl VectorIndex {
    pub fn new(dim: usize) -> Self {
        VectorIndex {
            dim,
            version: 0,
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

    pub fn len(&self) -> usize {
        self.embeddings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.embeddings.is_empty()
    }

    /// Insert (or replace) a node's embedding. Panics on dimension mismatch (caller contract).
    pub fn insert(&mut self, node: NodeId, embedding: Vec<f32>) {
        assert_eq!(embedding.len(), self.dim, "embedding dimension mismatch");
        self.embeddings.insert(node, embedding);
    }

    pub fn get(&self, node: NodeId) -> Option<&[f32]> {
        self.embeddings.get(&node).map(Vec::as_slice)
    }

    /// k nearest nodes to `q` by squared distance (deterministic tie-break by NodeId). Exact —
    /// the recall-complete stand-in for ANN.
    pub fn nearest(&self, q: &[f32], k: usize) -> Vec<(NodeId, f32)> {
        self.nearest_filtered(q, k, |_| true)
    }

    /// k nearest nodes for which `keep(node)` holds — the post-filter hook used by type-aware search.
    pub fn nearest_filtered(
        &self,
        q: &[f32],
        k: usize,
        keep: impl Fn(NodeId) -> bool,
    ) -> Vec<(NodeId, f32)> {
        let mut scored: Vec<(f32, NodeId)> = self
            .embeddings
            .iter()
            .filter(|(n, _)| keep(**n))
            .map(|(n, e)| (sqdist(q, e), *n))
            .collect();
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));
        scored.into_iter().take(k).map(|(d, n)| (n, d)).collect()
    }
}
