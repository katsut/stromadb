# Architecture — Version vector & cross-store consistency

> Design for `stroma-core::version` + the version-vector path of `hybrid`. Companion WHAT:
> `../spec/version-vector.md`. Overview: `../ARCHITECTURE.md` §7. Status: implemented (Epic 4). R-3.

## The deepest risk (R-3)

A read spans the authoritative changelog (structure) and a derived store that lags (the vector
index). If the index is behind, a freshly-created-and-embedded node is *structurally present* but
*not yet indexed* — pure ANN silently drops it (index/structure split-brain). This was the deepest
Phase 0 risk; `poc-crossstore-snapshot` validated the fix before we built it.

## Version vector exposes the skew

`(changelog_seqno, vector_watermark)` is sampled as one consistent cut, with the invariant
`vector_watermark <= changelog_seqno` (the derived store never claims more than the authority has — no
dangling). The skew is not hidden; it is a value the read carries.

## strict vs fresh

- **strict** reads the vector at its watermark — everything is consistent at one version, the newest
  tail excluded. For audit / reproducibility.
- **fresh** reads the indexed prefix and **brute-forces the un-indexed tail** `[watermark, head)`, so
  a structurally-present embedded node is never missed. `strict ⊆ fresh` always. This is the
  recall-completeness `ANN(indexed) ∪ brute-force(tail)` the spec calls for, and the reason the
  exact stand-in can be swapped for a lagging quantized ANN without losing matches.

## Why a tail at all

With the exact stand-in there is no real split-brain — so the index models a watermark explicitly to
reproduce the lagging-ANN condition (entries `seqno >= watermark` are "un-indexed"). That keeps the
strict/fresh contract honest now and correct when the quantized index lands: only the prefix becomes
approximate; the bounded tail stays exact.

## Boundaries / forward references
- Sampling the cut under *live concurrent* ingest (so a reader never observes a torn vector where
  `vector_watermark > changelog_seqno`) — the consistent-cut discipline; `poc-crossstore-snapshot`
  showed non-atomic sampling dangles. Wired with the concurrent engine.
- `skew > B` → backpressure / sync-index (ties index lag to read freshness, like read-merge's n_max).
- Lance/MVCC axes; per-store watermarks coordinated in one vector.
