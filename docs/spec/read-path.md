# Spec — Read path (read-merge + point/expand)

> Contract for `stroma-core::engine` (read-merge) and `stroma-core::query` (read primitives).
> Companion HOW: `../architecture/read-path.md`. Status: implemented (Epic 2).

## Engine — write-append / read-merge (CAP-1, CAP-4)

`Engine::new(n_max)` bounds the un-merged backlog.

| method | contract |
|---|---|
| `write(source, WriteKind) -> Result<u64, Backpressure>` | append to the changelog; `Backpressure` when the un-merged backlog == `n_max` |
| `materialize()` | fold the tail `[watermark, head)` into the base; advance the watermark; relieve backpressure |
| `snapshot() -> Snapshot` | read-merge: `base ∪ tail`, observed canonically |
| `unmerged() -> usize` | tail length (`<= n_max`) |

**Guarantee:** a read-merged `snapshot()` equals the `snapshot()` after `materialize()` — merging on
read never changes the result, only when the work is done. Partial updates are appended, not
rewritten; the un-merged tail is bounded by `n_max`.

## Query primitives over a `Snapshot` (CAP-2)

| fn | returns |
|---|---|
| `point_one(snap, subject, predicate)` | current functional value `Option<ObjKey>` (None if absent/closed) |
| `point_many(snap, subject, predicate)` | present element set `BTreeSet<ObjKey>` |
| `expand(snap, subject, predicate)` | 1-hop neighbor node ids (One value + Many set, node-valued only) |
| `expand_set(snap, subjects, predicate)` | 1-hop from a frontier set |
| `two_hop(snap, subject, p1, p2)` | `subject -p1-> X -p2-> Y`, the `Y` frontier |

All are pure functions of the snapshot; deterministic.

## Out of scope (later)
- Physical co-location (CSR adjacency, degree-aware hub spill) — optimization, same contracts (CAP-2).
- Time-scoped reads (`now/as-of/ever/overlap`) — the `temporal` operator (Epic 6 IR; uses
  `one_history`).
- Type-aware hybrid (vector) reads → Epic 3; authz injection → Epic 6; Live Query → Epic 5
  (reuse these same operators).
