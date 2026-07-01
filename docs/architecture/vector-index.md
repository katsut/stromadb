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

## Non-residual PQ + a once-per-query ADC table (the p99 lever)

Classic IVFADC encodes the *residual* `x − centroid(cell)`, which makes the ADC table depend on the
cell — so a query rebuilds the table for every probed cell, and candidate-gen cost scales with
`nprobe`. Measurement (`examples/ann_ssd_p99`) showed that per-cell table rebuild — not the raw tier —
is the p99 driver: at `nprobe=16` candidate-gen alone was ~4 ms.

Because **exact re-rank restores recall** (below), PQ only has to *rank candidates*, not reconstruct
precisely. So we encode the **raw** sub-vectors (non-residual). The ADC table is then
cell-independent: `table[j][code] = ‖ q_j − codebook[j][code] ‖²`, computed **once per query** and
reused across every probed cell. A posting's distance is `Σ_j table[j][code_j]` — one lookup + add per
subquantizer, no per-vector float math and no per-cell rebuild. This dropped warm p99 ~37% (4.0 → 2.5
ms at `nprobe=16`) with recall unchanged. The table is flat (`m × pq_ksub`) to avoid hot-path allocation.
The SoA cell layout (contiguous PQ codes per cell) landed for cache locality; remaining p99 headroom is
SIMD ADC and OPQ (not needed for the SLO).

## Parallel build

Assignment (nearest coarse cell) and PQ encoding are the O(nlist·d) / O(m·ksub·dsub) per-vector build
hot paths. They are embarrassingly parallel and read-only over the trained quantizer, so `train`'s
k-means assignment step and `add_batch` fan out across CPUs via `std::thread::scope` (no external
dependency, no `unsafe`); list insertion stays serial. This cut the 200K×768 build ~112 s → ~10 s
(~11×), making the A1 representative point (0.5M) a ~25 s build — feasible for the integration bench.
`add_batch` is order-equivalent to a sequence of `add` calls (tested).

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

## Tuning: `nlist` must scale with N (integrated read p99)

The integrated C2b measurement (durable Engine + IVF-PQ + IVM, real ANN read) found the read-path p99
driver was **not** the authz/type filter but the ANN scan itself: with `nlist` too small for the corpus,
coarse cells are large and imbalanced, so a query's probed postings blow up in the tail. At 100K vectors,
`nlist` 256→1024 cut integrated read p99 from 3.1ms to 1.6ms (<2ms SLO). `IvfPq::suggested_nlist(n)`
(~4·√n) encodes this; callers should size `nlist` to the corpus. A faster catalog (FxHash on
`node_type`/`node_label`, hit per candidate by the filter) shaved a further ~1ms and removes a SipHash
cost from the hot path, but the dominant lever is `nlist`.

## Boundaries / forward references
- Query-IR `TopK` still calls the exact stand-in (`vector::VectorIndex`); swapping it to
  `IvfPq::search_rerank` (with the principal's label scope) is the next integration.
- Cold tier is an in-RAM `Vec<f32>` today; moving it to mmap/SSD is transparent to the API.
- OPQ (learned rotation before PQ) and SIMD/parallel training would raise recall / cut build time; not
  needed to meet the current SLO.
- Async embedding arrival + `seqno` stamping is upstream (Vesicle, H3 / version vector).
