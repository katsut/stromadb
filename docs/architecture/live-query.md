# Architecture — Live Query (IVM)

> Design for `stroma-core::live`. Companion WHAT: `../spec/live-query.md`. Overview:
> `../ARCHITECTURE.md` §5. Status: recompute-and-diff MVP (Epic 5). CAP-5.

## What it is for

Agents register decision queries once and want to be told *what changed*, not to re-poll. A live
query maintains a result and emits diffs (added/removed) as the graph changes — the reactive half of
the query model.

## Same operators, one algebra (CAP-10)

A live query is just a query function over the snapshot — the exact `expand` / `point` / filter
operators used for one-shot reads. There is no separate "live query language": one-shot and live are
the same algebra, differing only in whether the result is returned once or maintained. This is the
API-level expression of the single-algebra bet (whose efficient execution B4 validated).

## Recompute-and-diff now, differential-dataflow later

This MVP re-evaluates each live query on change and diffs against the last result — correct and
simple, but O(result) per change. The hot-path-efficient backend is differential-dataflow: maintain
incremental arrangements so a change touches only affected keys. Phase 0's `poc-rkyv-ivm` proved the
hard part — dd's mutable arrangements can operate over rkyv *immutable* zero-copy facts without a
borrow conflict — so the backend can swap in behind `register`/`on_change` with no contract change,
exactly like the changelog's LSM backend and the vector index's quantized ANN.

## Bounded by design

Live-query count is capped (`max`); the efficient backend will also cap per-query IVM memory. This
is the cost north star: reactivity is opt-in and bounded, one-shot local reads are the cheap default.

## Scope

Only the monotonic / bounded-diff class (filter, equi-join, windowed aggregate, expand) is a
first-class live query; non-monotonic recursion (connected components, shortest path) is separate
SLO / best-effort. Push transport (delivering diffs to subscribers) is above this module.
