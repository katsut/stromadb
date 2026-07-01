# Spec — Changelog

> Contract for `stroma-core::changelog`. Companion HOW: `../architecture/changelog.md`.
> Status: in-memory semantics + **durable file-WAL backend** implemented (Epic 1 + durability build);
> LSM backend is a later swap behind the same contract.

## Role

The append-only, version-authoritative write log (source of truth). Every write is appended and
assigned a monotonic `seqno` (the version authority). Derived stores chase its watermark; replaying
it reproduces state exactly.

## Write API

`append(source: FieldId, kind: WriteKind) -> Result<u64, Backpressure>` — appends a write, returns
its `seqno`, or `Backpressure` if the in-flight backlog is full.

`WriteKind` (a diff *without* its order key; the changelog assigns it):
| variant | meaning |
|---|---|
| `SetOne { subject, predicate, object, valid_from }` | functional value assert |
| `CloseOne { subject, predicate, valid_from }` | functional close/delete |
| `AddMany { subject, predicate, object }` | OR-Set add |
| `RemoveMany { subject, predicate, observed }` | observed-remove (tags resolved by ingest) |
| `HardDelete { subject, predicate, cardinality }` | compliance purge |

## seqno → OrderKey

A record at `seqno` with `source` folds as `OrderKey { tx: seqno, source, seq: seqno }`. Because
seqno is globally monotonic, the order key is total — replay reproduces the exact same fold state.

## Backpressure (CAP-1)

`new(max_unmaterialized)` bounds the appended-but-not-materialized backlog. When
`unmaterialized() >= max_unmaterialized`, `append` returns `Backpressure { unmaterialized, limit }`
instead of stalling silently. A derived store calls `mark_materialized(up_to)` to advance the
watermark and relieve it.

## Replay

- `replay() -> Fold` — fold the whole log in seqno order (deterministic).
- `replay_into(&mut Fold)` — fold into an existing fold (catch-up).
- `head() -> u64` — next seqno (== length).

## Durability (file-WAL backend)

Durability is opt-in and lives behind the *same* append/replay/watermark contract — `new` is the
pure in-memory mode; `open` is the durable mode.

- `open(path, max_unmaterialized) -> io::Result<Changelog>` — open a durable changelog backed by the
  framed WAL at `path`. Recovers the committed prefix on cold start (a torn tail from a crash
  mid-append is dropped). Recovered records count as already-durable and already-materialized, so a
  fresh open neither re-fsyncs nor backpressures on them. A missing file starts empty.
- `sync() -> io::Result<()>` — **durability commit point**: frame the `[durable_head, head)` tail and
  `fsync` it (group commit; the caller picks the boundary, typically per ETL chunk). No-op in
  in-memory mode.
- `durable_head() -> u64` — seqno up to which records are guaranteed durable (== `head()` right after
  a successful `sync`; `0` in in-memory mode).

Durability guarantee: **a record is durable iff `sync` returned `Ok` after it was appended.** A crash
loses only the un-synced tail (writes after the last `sync`), never a synced prefix. `Engine::open` /
`Engine::sync` expose the same at the engine level and rebuild the fold on cold start (the RTO path).

On-disk frame: `[payload_len u32 LE][crc32 u32 LE][payload]`; `crc` is FNV-1a (torn-write detector,
not a MAC). See `../architecture/changelog.md` for recovery semantics.

## Invariants
- `seqno` is monotonic and dense from 0; it never changes once assigned.
- `replay()` is pure: same log → same `Snapshot`.
- Recovery is prefix-exact: `open` recovers exactly the records that were `sync`-committed, in order.

## Out of scope (later)
- **LSM backend**: RocksDB/Speedb append path, rkyv zero-copy records, O_DIRECT, compaction,
  snapshot/PITR — slots in behind this same contract (device-level WAF measured there).
- Deriving `RemoveMany.observed` / `CloseOne` from high-level retractions → ingest layer.
- The changelog seqno is the `changelog_seqno` axis of the cross-store version vector (A4 / Epic 4).
