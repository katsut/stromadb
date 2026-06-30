# Spec — Fold (stream diffs → state)

> Contract for `stroma-core::fold`. Companion HOW: `../architecture/fold.md`.
> Status: implemented (Epic 1, Story 1.2). Algebra validated in Phase 0 (`poc-fold-determinism`).

## Inputs — diffs (`Op`)

Diffs are folded per `(subject, predicate)` key. The predicate's cardinality (from the catalog)
determines which ops are valid for a key.

| op | target | meaning |
|---|---|---|
| `SetOne { subject, predicate, object, valid_from, ok }` | cardinality-One | assert the functional value |
| `CloseOne { subject, predicate, valid_from, ok }` | cardinality-One | close/delete (supersede with no successor) |
| `AddMany { subject, predicate, object, ok }` | cardinality-Many | add an element (its `ok` is the OR-Set tag) |
| `RemoveMany { subject, predicate, observed }` | cardinality-Many | observed-remove: tombstone the given tags |
| `HardDelete { subject, predicate, ok, cardinality }` | either | compliance purge floor |

`Op::assert_from(catalog, fact, seq)` builds the assert op for a `Fact`, routed by cardinality
(`One → SetOne`, `Many → AddMany`); returns `None` for an unregistered predicate.

## OrderKey — total order

`OrderKey = (tx: u64, source: FieldId, seq: u64)`, compared lexicographically.

**Requirement:** the write engine MUST assign a globally-unique `(source, seq)` per op, so two
distinct competing writes never share an `OrderKey`. Otherwise the LWW winner is ambiguous and the
fold is non-deterministic.

## Semantics

- **cardinality-One → LWW-Register with history.** All versions are kept (grow-only map keyed by
  `OrderKey`). Current value = the version with the max `OrderKey` (above the hard-delete floor);
  `object = None` means closed. `CloseOne` is just a `None` version competing in the LWW
  (delete → closes the valid-time interval; history retained).
- **cardinality-Many → OR-Set.** Per-element add tags + observed-remove tombstones. An element is
  present iff it has a tag that is not tombstoned and is above the floor.
- **HardDelete → max-register floor.** Everything with `OrderKey <= floor` is purged; re-assertion
  above the floor survives (GDPR: forget the past, allow new facts).

## Observation

`Fold::observe() -> Snapshot`:
- `one[(s,p)]` = current functional value (`Option<ObjKey>`), absent if fully purged/never set.
- `one_history[(s,p)]` = version rows above the floor, sorted.
- `many[(s,p)]` = present element set, absent if empty.

`ObjKey` is the orderable/hashable object identity (`Node | Int | Float(bits) | Text | Bool`).

## Guarantees (proven by `tests/fold_determinism.rs`, 2000 cases each)

Each key state is a join-semilattice (commutative + associative + idempotent merge), so:
- **P1 permutation invariance** — any arrival order → same snapshot.
- **P2 multi-source invariance** — fold per source then `merge` = single fold.
- **P3 idempotence** — at-least-once redelivery changes nothing.
- **P4 GC invariance** — `gc()` (drop `<= floor`) preserves `observe()` and stays convergent.

## Out of scope (other components)
`OrderKey`/`transaction_time` assignment and durability → changelog (Story 1.3). Deriving
`RemoveMany`/`CloseOne` from retraction diffs → ingest layer. On-disk format → changelog.
