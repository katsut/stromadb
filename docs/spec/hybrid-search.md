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

## Frozen contract decisions (v1, from injection spikes)
- **Recall-completeness (H2).** With a quantized/approximate ANN, a selective type filter drops
  recall — relevant type-T items sit in cells the query never probed (spike: recall 0.31 @ type
  selectivity 0.02). The type-ANN contract therefore promises
  `result = ANN(probed) ∪ brute-force(unprobed type-T)` within a **bounded tail budget**.
  This RECALL tail (un-probed type-T) is a *different set* from the version vector's WATERMARK tail
  (un-indexed, H3); a fresh read must cover **both**.
- **authz = scoped index (H4).** authz is an **index-partitioning** decision — a per-authz-class /
  label scoped sub-index the principal searches — **NOT** a post-filter over one shared index. A
  shared index + post-filter leaks the unauthorized-near count via timing (scan depth grows ~linearly)
  and top-k completeness (collapses to 0); a scoped index is flat on both. Epic 6 wires authz on this
  basis.

## Out of scope (later / implementation)
- The quantized ANN index (IVF-PQ/DiskANN) itself; versioned-index parallel-run/swap.
- The bounded-tail budget's concrete value (tune on the real machine).
