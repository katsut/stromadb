# Architecture — Fold

> Design and rationale for `stroma-core::fold`. Companion WHAT: `../spec/fold.md`.
> Overview: `../ARCHITECTURE.md` §2. Status: implemented (Epic 1, Story 1.2).

## Why a join-semilattice

The fold's one hard requirement is **deterministic convergence**: ingest is out-of-order and
multi-source, delivery is at-least-once, and audit/replay must reproduce the exact same state. We get
this for free — not by careful ordering, but structurally — by making each `(subject, predicate)`
state a join-semilattice whose merge is commutative + associative + idempotent. Convergence under any
order / partition / redelivery then follows from the CRDT convergence theorem. Phase 0's
`poc-fold-determinism` spike refuted non-determinism across thousands of cases before we committed to
this; Story 1.2 ports that validated algebra onto the engine types.

## Cardinality picks the lattice

The catalog says *what* a predicate is; the fold says *how* it converges:
- **One → LWW-Register (with history).** A grow-only map of versions keyed by the total-order
  `OrderKey`; the current value is the max. Keeping all versions (not just the winner) makes history
  a queryable byproduct and makes the merge a plain map-union. Delete is a `None` version competing
  in the same LWW — supersession and deletion are the same mechanism.
- **Many → OR-Set.** Add-tags + observed-remove tombstones. This is what lets a concurrent add
  survive a remove that didn't observe it — the correct behaviour for multi-source accumulation.

## Total-order tie-break

LWW needs a total order so "last write" is unambiguous. `OrderKey = (tx, source, seq)` provides it;
the load-bearing invariant is that the write engine makes `(source, seq)` globally unique. This is
the one place determinism can break (two distinct writes sharing a key), so it's a contract on the
producer, documented in the spec.

## Hard-delete as a floor

Compliance deletion is different from supersession: it must *forget the past* yet allow *new* facts.
Modelling it as a max-register floor (purge `<= floor`, keep `> floor`) keeps it inside the lattice
(a max is a join), so it composes with everything else and stays order-independent. GC drops state
below the floor and is provably observation-preserving.

## One algebra

This fold is the *write* side of StromaDB's intended single read/IVM algebra. Live Queries are
maintained incrementally over the same state the fold produces — recompute-and-diff generally, and
keyed-incremental for completeness/rule queries (`incremental::Maintained`). A full
differential-dataflow backend (validated in Phase 0 `poc-rkyv-ivm`) is the roadmap target that slots
in behind the same contract.

## Boundaries / forward references
- `OrderKey` assignment, durability, the authoritative changelog → Story 1.3.
- Turning retraction diffs into `CloseOne`/`RemoveMany` (which tags to observe) → ingest layer.
- `ObjKey` exists because the raw `Object` carries an `f64`; the fold needs a total order, so floats
  are keyed by bits. Equality of `ObjKey` is the fold's notion of object identity.
