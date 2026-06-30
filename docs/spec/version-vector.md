# Spec — Version vector & cross-store reads

> Contract for `stroma-core::version` and the version-vector path of `stroma-core::hybrid`.
> Companion HOW: `../architecture/version-vector.md`. Status: implemented (Epic 4). R-3.

## VersionVector

`VersionVector { changelog_seqno, vector_watermark }` — MVP axes: the changelog seqno (authority)
and the vector-index watermark (derived). Invariant: `vector_watermark <= changelog_seqno`.

| method | meaning |
|---|---|
| `new(changelog_seqno, vector_watermark)` | construct (debug-asserts the invariant) |
| `dominates(other)` | componentwise `>=` |
| `comparable(other)` | one dominates the other (else concurrent) |
| `skew()` | `changelog_seqno - vector_watermark` (un-indexed tail length) |

## Read modes

`ReadMode::Strict` — resolve the vector axis at `vector_watermark` only (fully consistent, newest
tail excluded; audit/repro). `ReadMode::Fresh` — indexed prefix ∪ brute-force tail (agent default).

## Vector index watermark

`VectorIndex` tags each embedding with the changelog seqno it became available at, and tracks a
`watermark` (`advance_watermark`). `nearest_scoped(q, k, max_seqno, keep)`:
- `Some(w)` → only entries with `seqno < w` (the indexed prefix);
- `None` → all entries (indexed ∪ tail).

## Hybrid `search(index, catalog, q, target_type, k, vv, mode)`

- **Strict** → `nearest_scoped(.., Some(vv.vector_watermark), type-filter)`.
- **Fresh** → `nearest_scoped(.., None, type-filter)` = indexed prefix ∪ brute-force tail.

**Guarantee:** `strict ⊆ fresh`, and fresh is recall-complete — a structurally-present, embedded
node the index has not yet caught up to is still found (split-brain closed). Type-violations remain 0.

## Out of scope (later)
- Lance (cold) and MVCC txid axes of the version vector.
- Sampling the cut under live concurrent ingest (the consistent-cut invariant; torn sampling dangles
  — proven in Phase 0 `poc-crossstore-snapshot`) → wired with the concurrent engine.
- `skew > B` backpressure / sync-materialize of the vector index.
