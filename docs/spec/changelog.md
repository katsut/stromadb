# Spec — Changelog

> Contract for `stroma-core::changelog`. Companion HOW: `../architecture/changelog.md`.
> Status: in-memory semantics implemented (Epic 1, Story 1.3); durable backend deferred.

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

## Invariants
- `seqno` is monotonic and dense from 0; it never changes once assigned.
- `replay()` is pure: same log → same `Snapshot`.

## Out of scope (later)
- **Durability**: LSM (RocksDB/Speedb), rkyv zero-copy, O_DIRECT, WAL fsync, snapshot/PITR — slots
  in behind this same append/replay/watermark contract.
- Deriving `RemoveMany.observed` / `CloseOne` from high-level retractions → ingest layer.
- The changelog seqno is the `changelog_seqno` axis of the cross-store version vector (A4 / Epic 4).
