# Architecture — Vector Index (IVF-PQ)

> Design and rationale for `stroma-core::ivf`. Companion WHAT: `../spec/vector-index.md`.
> Overview: `../ARCHITECTURE.md`. Status: real IVF-PQ + exact re-rank, measured against the DONE SLO.

## Why IVF-PQ + re-rank (not one or the other)

The A1 envelope has 0.5M–5M vectors × 768 dim. Raw f32 is 1.5–15 GB — too big for the hot RAM budget
that the single-node thesis depends on. Two techniques, each solving half the problem:

- **PQ** compresses each vector to `m` bytes (32× @ m=96), so the *searchable* structure fits RAM.
  But PQ is lossy: in 768-dim the reconstruction error swamps the fine distances between the true
  top-10 and their runners-up, so **PQ-only recall@10 caps around 0.4–0.5** — measured, not assumed.
- **IVF** routes a query to its `nprobe` nearest cells so it scans a fraction of the corpus, not all
  of it. This is the latency/coverage knob, orthogonal to PQ's accuracy.

Neither alone meets `recall@10 ≥ 0.9`. The production shape is **IVF-PQ for cheap candidate generation,
then exact re-rank**: pull `rerank_r` (≫ k) candidates from the hot codes, then re-score just those by
exact distance over the raw vectors. Re-rank fixes recall (0.38 → ~1.0, measured) while touching raw
for only `rerank_r` vectors per query — so raw can live in the **cold** tier (mmap/SSD), and the hot
RAM footprint stays the PQ codes (~48 MB @ A1 rep). This is the DiskANN/IVFADC-refine pattern.

## Residual encoding and ADC

PQ encodes the *residual* `x − centroid(cell)`, not `x` — residuals are small and same-scaled across
cells, so one shared codebook set encodes all cells well. Search builds per-cell asymmetric distance
(ADC) tables: for probed cell `c`, `table[j][code] = ‖ (q−centroid_c)_j − codebook[j][code] ‖²`, and a
posting's approximate distance is `Σ_j table[j][code_j]` — one table lookup + add per subquantizer, no
per-vector float math. Tables are flat (`m × pq_ksub`) to avoid per-query allocation on the hot path.

## The three scopes are structural, not post-hoc

`max_seqno` (watermark), `allowed_label` (authz), and `keep` (type) are checked **inside the cell scan,
before ADC scoring**. This matters most for authz (H4): the party-review spike showed a shared index +
post-filter leaks unauthorized density through timing and top-k completeness. Here an unauthorized
posting is skipped before any distance is computed — it costs nothing and is indistinguishable from
absent. That is the "scoped sub-index" guarantee realized without physically separate indexes: the
label predicate partitions the scan.

## Recall completeness vs re-rank — two different tails

- **`search_complete`** answers "did I *see* every matching vector?" (H2 coverage): probed cells ∪ a
  bounded brute-force of matching postings in unprobed cells. Its ranking is still PQ.
- **`search_rerank`** answers "is my *ordering* exact?" (recall quality): exact re-score of the
  candidate set.

They compose — a complete-and-exact read is `search_complete` feeding its union into an exact re-rank —
but they are separate knobs (coverage budget vs rerank depth) because they trade off different costs.

## Determinism

Training uses a fixed-seed splitmix64 PRNG for k-means init and dead-cluster re-seeding, so an index
built from the same sample is byte-identical run to run — required for reproducible tests and for the
version vector's meaning to be stable.

## Boundaries / forward references
- Query-IR `TopK` still calls the exact stand-in (`vector::VectorIndex`); swapping it to
  `IvfPq::search_rerank` (with the principal's label scope) is the next integration.
- Cold tier is an in-RAM `Vec<f32>` today; moving it to mmap/SSD is transparent to the API.
- OPQ (learned rotation before PQ) and SIMD/parallel training would raise recall / cut build time; not
  needed to meet the current SLO.
- Async embedding arrival + `seqno` stamping is upstream (Vesicle, H3 / version vector).
