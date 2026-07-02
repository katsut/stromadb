# Architecture — Type-aware hybrid search

> Design for `stroma-core::vector` + `stroma-core::hybrid`. Companion WHAT:
> `../spec/hybrid-search.md`. Overview: `../ARCHITECTURE.md` §6. Status: implemented (Epic 3).

## The failure mode it fixes

Embeddings cluster by *semantics*; graph types cut *across* those clusters. So a pure vector
neighbourhood mixes types — a "Python" skill, a doc about Python, and a person named for it all sit
near each other. Plain ANN returns that mix; for a query that wants type T, the wrong-type neighbours
are noise (and silent: nothing flags them). This disjoint-type mis-fusion is the concrete thing CAP-3
targets, and the Phase 0 quality spike (`poc-quality-hybrid`) showed it is large and real
(plain ANN ~80% type-violations; type-aware ~0).

## The fix: filter ANN candidates by type

Type-aware search is ANN + an graph-type filter applied to the candidates (via the catalog's
node→type map). Because the filter is exact, type-violations are **0 by construction**; recall rises
because relevant type-T items are no longer evicted from top-k by closer wrong-type distractors. The
"symbolic" half (types) makes the "neuro" half (vectors) correct — cheaply, deterministically, no ML
on the hot path.

## Exact now, quantized later

The index is exact distance today (recall-complete), which lets us build and test the *quality*
contract without a heavy native ANN dependency. The production quantized index (IVF-PQ/DiskANN, RAM-
cheap) slots in behind the same `nearest`/`nearest_filtered` API; only recall (not the contract)
changes, and the recall gap is closed by the brute-force tail (next).

## What Epic 4 adds

Two things this story defers because they need the cross-store version vector:
- **versioning/watermark** — the index is a derived store lagging the changelog.
- **recall completeness** — `ANN(indexed) ∪ brute-force(un-indexed-but-embedded tail)`, which closes
  the index/structure split-brain (a structurally-present, freshly-embedded node not yet indexed must
  still be findable). `nearest_filtered` is the hook the brute-force tail and the authz post-filter
  both plug into.

## Embeddings are received
The DB does not embed (no internal model). Vectors are produced caller/Vesicle-side and inserted; model/dim
changes run a new versioned index in parallel and switch over (mixed versions rejected by version).
