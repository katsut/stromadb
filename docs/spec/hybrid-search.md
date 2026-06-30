# Spec — Vector index & type-aware hybrid search

> Contract for `stroma-core::vector` and `stroma-core::hybrid`. Companion HOW:
> `../architecture/hybrid-search.md`. Status: implemented (Epic 3). The differentiator (CAP-3).

## VectorIndex

Received (pre-computed) embeddings keyed by node — the engine never embeds.

| method | contract |
|---|---|
| `new(dim)` / `dim()` | fixed embedding dimension |
| `version()` | index version; a model/dim change starts a new version (mixed versions rejected) |
| `insert(node, embedding)` | store/replace; dimension must match |
| `get(node)` | the node's embedding |
| `nearest(q, k)` | k nearest by squared distance, tie-broken by NodeId (deterministic) |
| `nearest_filtered(q, k, keep)` | k nearest among nodes where `keep(node)` holds |

This MVP uses **exact distance** (recall-complete) as the stand-in for the production quantized ANN
index (IVF-PQ/DiskANN), which slots in behind `nearest`/`nearest_filtered`.

## Search

| fn | meaning |
|---|---|
| `plain_ann(index, q, k)` | type-blind top-k (baseline) |
| `type_aware(index, catalog, q, target_type, k)` | top-k restricted to `target_type` via the catalog |

**Quality contract (CAP-3):** type-aware returns recall ≥ plain ANN with **type-violation rate ≈ 0**
when types are interleaved in embedding space (the disjoint-type mis-fusion case). Validated
statistically in Phase 0 (`poc-quality-hybrid`: Δrecall +0.54, type-violation 80%→0%); a constructed
unit bench asserts it here.

## Out of scope (later)
- Quantized ANN index (IVF-PQ/DiskANN); versioned-index parallel-run/swap + watermark → Epic 4.
- **Recall completeness** = ANN(indexed) ∪ brute-force(un-indexed tail), closing index/structure
  split-brain → Epic 4 (needs the version vector).
- filtered-ANN recall degradation at high type-selectivity (continuing concern; brute-force fallback).
- authz as an additional post-filter → Epic 6.
