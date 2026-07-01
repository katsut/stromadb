# Spec — Vector Index (IVF-PQ)

> Contract for `stroma-core::ivf` (production backend) and `stroma-core::vector` (exact stand-in /
> reference). Companion HOW: `../architecture/vector-index.md`. Consumed by type-aware hybrid
> (`spec/hybrid-search.md`). Status: real IVF-PQ + exact re-rank implemented and measured against the
> DONE SLO; wiring into the query-IR read path is the next step.

## Role

The received-embedding store + approximate nearest-neighbour search. Embeddings are **received**
(the engine never embeds — no-LLM substrate); each carries the changelog `seqno` it became available
at and an authz `label`. Search returns `(node, distance)` top-k under three orthogonal scopes.

## Two tiers

| tier | holds | size @ A1 rep (0.5M×768) | residence |
|---|---|---|---|
| **hot** | PQ codes (`m` bytes/vec) + IVF cell lists + centroids | ~48 MB @ m=96 (**32× vs raw**) | RAM |
| **cold** | raw f32 vectors, for exact re-rank of candidates only | ~1.5 GB | RAM now; mmap/SSD-able |

Candidate generation touches only the hot tier; the cold tier is read for the `rerank_r` candidates
of a query, not scanned.

## API (`IvfPq`)

- `new(dim, nlist, m)` — untrained; `dim % m == 0`.
- `train(sample)` — trains the coarse quantizer (`nlist` cells) and PQ codebooks from a representative
  sample. Once, before `add`.
- `add(node, seqno, embedding, label)` — assign to nearest cell, PQ-encode the residual, store the raw
  vector in the cold tier.
- `search(q, k, nprobe, max_seqno, allowed_label, keep) -> [(node, dist)]` — approximate top-k by PQ
  (ADC) over the `nprobe` nearest cells. Recall rises with `nprobe`; ranking is PQ (lossy).
- `search_rerank(q, k, nprobe, rerank_r, max_seqno, allowed_label, keep) -> [(node, exact_dist)]` —
  **production read path**: `rerank_r` PQ candidates re-scored by exact distance over the cold tier.
  Recall no longer bounded by PQ error.
- `search_complete(q, k, nprobe, max_seqno, allowed_label, keep, tail_budget) -> (results, tail_scored, truncated)`
  — probed ∪ bounded brute-force over matching postings in *unprobed* cells (H2 recall completeness).

## Scoping axes (all applied before/at scoring)

- **watermark (H3)**: `max_seqno = Some(w)` reads the indexed prefix (`seqno < w`, strict); `None`
  reads all (fresh). This is the vector axis of the version vector.
- **authz (H4)**: `allowed_label` is checked *before* a posting is scored — unauthorized vectors are
  never distance-computed, so neither timing nor completeness leaks their existence (scoped, not
  shared-index + post-filter).
- **type/predicate (`keep`)**: ontology-type or arbitrary node filter.

## Invariants
- A posting failing `max_seqno`, `allowed_label`, or `keep` is never scored and never returned.
- `search_rerank` distances are exact (`sqdist` over raw); ordering is exact over the candidate set.
- `search_complete` with `tail_budget ≥ matching postings` scans every match (coverage complete,
  `truncated=false`); a smaller budget bounds the tail (`truncated=true`) — silent partial coverage
  never happens.
- Deterministic tie-break by NodeId; deterministic training (fixed-seed k-means).

## Measured (DONE SLO differentiation leg — `examples/ann_slo.rs`, 200K×768, type-sel 50%)
- filtered recall@10: pure-PQ ~0.38 → **+rerank(R=100) ~0.99–1.0** (SLO ≥ 0.9 ✅).
- warm hybrid **p99 = 0.78 ms** with authz ON + type filter + rerank (SLO < 2 ms ✅); p50 0.41 ms.
- compression **32×** (hot codes vs raw).

> ⚠ **Measurement condition**: the p99 above was measured with the **raw (cold) tier in RAM**. Moving
> raw to mmap/SSD makes re-rank a random read of `rerank_r` vectors per query, which changes p99 — so
> "raw = cold **and** p99 < 2 ms" is **not yet jointly validated**. That re-measurement is the decisive
> step of the integration leg. If it exceeds 2 ms, the staged fallback is OPQ → smaller `rerank_r` →
> a thin hot re-rank set. The recall/compression numbers are condition-independent.

## Query-IR integration (done)
`IvfPq` and the exact `VectorIndex` both implement `ir::AnnBackend`, so `ir::run` is generic over the
backend. The `TypeAnn` source calls `ann_search(q, k, scope, keep)` — `keep` carries authz+type and is
applied before scoring; `scope` is the watermark. The IVF-PQ path is tested for id-set equivalence
against the exact oracle through a full pipeline (`ir::tests::ivfpq_backend_matches_exact_through_ir`).
IR probe/re-rank defaults (`IR_NPROBE`, `IR_RERANK_R`) are provisional pending tuning (#23/#26).

## Out of scope (later)
- Cold tier on mmap/SSD (DiskANN-style) instead of RAM (#19); OPQ rotation (#26); SIMD/parallel build (#22).
- Async embedding pipeline stamping `seqno` on arrival (Vesicle responsibility, H3).
