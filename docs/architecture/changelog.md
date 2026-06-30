# Architecture — Changelog

> Design and rationale for `stroma-core::changelog`. Companion WHAT: `../spec/changelog.md`.
> Overview: `../ARCHITECTURE.md` §4, §7. Status: in-memory (Epic 1, Story 1.3).

## Version authority (single source of truth)

One store is authoritative for *version*: the append-only changelog. Everything else — the fold's
materialized state, the vector index, Lance cold storage — is *derived* and carries a watermark
saying how far it has caught up. Centralizing version in one monotonic seqno is what makes
cross-store consistency tractable (the seqno is the `changelog_seqno` axis of the version vector,
A4 / Epic 4) and what lets derived stores be rebuilt at will.

## Deterministic replay

The changelog assigns each record a seqno and folds it as `OrderKey{tx: seqno, source, seq: seqno}`.
Because the seqno is globally monotonic and dense, replay is a pure function of the log — the same
log always reconstructs the same `Snapshot`. This is the backbone of audit and recovery: state is
never the authority, the log is; state is just a fast cache of `replay()`.

## Backpressure, not silent stall

A derived store that falls behind must not let the writer melt the system. The changelog bounds the
in-flight (appended-but-not-materialized) backlog and returns explicit `Backpressure` when full, so
the producer can slow down or shed load — never a silent OOM/stall (CAP-1). `mark_materialized`
relieves it as derived stores catch up. The bound is the knob that ties write rate to read freshness.

## In-memory now, durable behind the same contract

This story implements the *semantics* (append, seqno authority, replay, watermark, backpressure) in
memory so the rest of the engine can build on a stable contract. The durable backend — LSM
(RocksDB/Speedb) for the append path, rkyv zero-copy records, O_DIRECT, WAL fsync, snapshot/PITR —
slots in behind the same `append`/`replay`/watermark API in a later story. Nothing above the
changelog needs to change when durability lands.

## Relationship to the fold

The changelog *owns* order-key assignment; the fold *consumes* ops and converges. Keeping assignment
in one place (the authority) is what guarantees the uniqueness the fold's LWW tie-break requires
(`spec/fold.md`). The two compose as: ingest → `append` (assigns seqno) → `replay_into(fold)`.

## Boundaries / forward references
- Durability/persistence backend → later story (trait-boundaried).
- `RemoveMany.observed` resolution (which tags a retraction removes) → ingest layer (needs current
  fold state).
- Watermark coordination across multiple derived stores → version vector (Epic 4).
