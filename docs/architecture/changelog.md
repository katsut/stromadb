# Architecture — Changelog

> Design and rationale for `stroma-core::changelog`. Companion WHAT: `../spec/changelog.md`.
> Overview: `../ARCHITECTURE.md` §4, §7. Status: in-memory semantics + durable file-WAL backend
> (Epic 1 + durability build); LSM is a later swap.

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

## Durability: framed WAL + group-commit fsync

Durability slots in behind the same `append`/`replay`/watermark API — additively, so nothing above
the changelog changed when it landed. `Changelog::new` stays the pure in-memory mode; `Changelog::open`
is the durable mode.

**Group commit, not per-append fsync.** `append` does exactly what it always did (push a record to
the in-memory log; assign a seqno) — no I/O, so its signature stays `Result<u64, Backpressure>` with
no `io::Error`. Durability is a separate, explicit step: `sync` frames every record in
`[durable_head, head)` and issues one `write_all` + `fsync` (`File::sync_all`). The caller batches the
commit boundary — an ETL chunk is `append_batch` then one `sync` — so N appends cost one fsync, not N.
A record is durable iff a `sync` returned `Ok` after it; a crash loses only the un-synced tail. This is
the standard WAL group-commit shape and is why the write path hits ~7M facts/s with an fsync per chunk
(measured, `examples/durability_slo.rs`).

**Frame format & recovery.** Each record is written as `[payload_len u32][crc32 u32][payload]`
(`wal.rs`). On `open`, `wal::recover` reads frames in order and **stops at the first torn frame** —
a short read (crash mid-write), a checksum mismatch, or an undecodable payload. Everything before it
is recovered intact; the torn tail is dropped. This makes crash recovery prefix-exact without a
separate "commit marker": the checksum *is* the marker. A corrupt length field is bounded by a
16 MiB per-record cap so a garbage tail can't trigger a huge allocation. `crc` is FNV-1a — a
torn-write detector, not a cryptographic MAC (the WAL is trusted local storage).

**Cold start = RTO.** `Engine::open` calls `Changelog::open` (recover the log) then `replay_into`
(fold the whole recovered log back into the base state). That fold *is* the recovery-time objective.
Measured on the real engine at the A1 representative point (5M facts): recovery **0.81s** — well
under the 10s DONE SLO — with 0 data loss. Application-level framing overhead is ~34 B/edge-record
(~1.7× the 20 B logical payload); device-level write amplification is a property of the LSM backend
and is measured when that lands.

**LSM later, same contract.** The eventual LSM backend (RocksDB/Speedb append path, rkyv zero-copy
records, O_DIRECT, compaction, snapshot/PITR) replaces the file WAL behind this same
open/sync/replay/watermark API. The frame codec and recovery discipline carry over; only the storage
engine underneath changes.

## Relationship to the fold

The changelog *owns* order-key assignment; the fold *consumes* ops and converges. Keeping assignment
in one place (the authority) is what guarantees the uniqueness the fold's LWW tie-break requires
(`spec/fold.md`). The two compose as: ingest → `append` (assigns seqno) → `replay_into(fold)`.

## Boundaries / forward references
- LSM storage engine (device-level WAF, compaction, PITR) → later swap behind the same contract.
- `RemoveMany.observed` resolution (which tags a retraction removes) → ingest layer (needs current
  fold state).
- Watermark coordination across multiple derived stores → version vector (Epic 4).
