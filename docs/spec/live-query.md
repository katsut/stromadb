# Spec — Live Query (IVM)

> Contract for `stroma-core::live`. Companion HOW: `../architecture/live-query.md`.
> Status: recompute-and-diff MVP (Epic 5) + keyed-incremental maintenance for the completeness-rule
> class (`incremental::Maintained`, O(touched), verified equal to full recompute); a general
> differential-dataflow backend is deferred. CAP-5.

## Model

A live query is any `Fn(&Snapshot) -> BTreeSet<NodeId>` — the monotonic / bounded-diff class
(filter / expand / equi-join / windowed aggregate), expressed with the *same* operators as one-shot
reads (CAP-10). On each engine change the registry re-evaluates and pushes only the delta.

| type | meaning |
|---|---|
| `Diff { added, removed }` | node ids that entered / left the result |
| `QueryId` | handle for a registered live query |
| `AtCapacity` | registration refused (live-query cap reached) |

## LiveQueries

| method | contract |
|---|---|
| `new(max)` | bounded registry (`max` live queries) |
| `register(snapshot, eval) -> Result<(QueryId, Diff), AtCapacity>` | register; initial result returned as an all-`added` diff |
| `deregister(id)` | stop a live query |
| `on_change(snapshot) -> Vec<(QueryId, Diff)>` | re-evaluate all; return only non-empty diffs |

**Guarantees**
- `on_change` with no change returns no diffs.
- A diff reflects exactly the set difference vs the previous result (add and remove).
- Registration beyond `max` returns `AtCapacity` (count is bounded — a cost north star).

## Out of scope (later)
- **Efficient IVM backend** — differential-dataflow incremental arrangements over rkyv zero-copy
  facts (validated in Phase 0 `poc-rkyv-ivm`), replacing recompute-and-diff behind this contract.
- Non-monotonic recursion (connected components / shortest path) → separate SLO / best-effort.
- Push transport (subscriber delivery); per-query IVM memory cap.
