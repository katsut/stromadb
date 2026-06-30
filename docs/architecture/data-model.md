# Architecture — Data model & ontology catalog

> Design and rationale for `stroma-core`'s Fact model + catalog. Companion WHAT: `../spec/data-model.md`.
> Overview: `../ARCHITECTURE.md` §1, §4, §8. Status: implemented (Epic 1, Story 1.1).

## Why Fact-centric

Everything — nodes, edges, attributes, history — is a projection of one unit, the Fact. A single
unit is what lets every capability (ingest fold, read-merge, hybrid search, IVM, authz) compose over
the same shape instead of each operating on a bespoke structure. Nodes/edges are *views*, not storage.

## Field-ID interning

Type and predicate names intern to small integer `FieldId`s. This separates the logical vocabulary
(human-readable names, evolvable) from the physical encoding (compact ids in facts/indexes), and
keeps the catalog cheap to carry in the hot cache. It is the basis for non-stop additive schema
evolution (CAP-6): a new predicate is a new interned id, not a migration.

## Minimal validation, not a reasoner

Validation at the ingest boundary checks only what is cheap and load-bearing: predicate existence and
domain/range types. It deliberately does **not** do ontology reasoning — that is relocated to the
caller (no-LLM substrate). Validation is also **open-world**: unknown node types pass; only *known*
mismatches fail. "What should exist but doesn't" is a separate, opt-in concern (absence detection,
CAP-12), not a validation error.

## Cardinality drives the fold

`Cardinality` lives on the predicate but is not enforced at single-fact validation — it is a
multi-fact property, so it is the **fold's** job (Story 1.2): `One` → supersede (LWW-Register),
`Many` → accumulate (OR-Set). Keeping cardinality in the catalog and enforcing it in the fold avoids
read-time surprises and keeps validation O(1) per fact.

## Bitemporal & provenance, from the start

`valid_time` (true-in-the-world) is first-class on every fact; `transaction_time` is owned by the
changelog (assigned at append) so producers cannot forge ordering. Provenance separates `Asserted`
(primary) from `Derived` (LLM/hypothesis) at the unit level, so the query layer can default to
primary and prevent hallucination self-reinforcement without a special path.

## Boundaries / forward references

- `transaction_time` assignment, durability, version authority → **changelog** (Story 1.3).
- cardinality enforcement & convergence → **fold** (Story 1.2; algebra validated in Phase 0
  `poc-fold-determinism`).
- node→type storage is currently an in-memory map for validation; it will be backed by the symbolic
  core (co-located type/adjacency) in the read path (Epic 2).

## Module shape

`crates/stroma-core/src/fact.rs` (model) + `catalog.rs` (interner, defs, validation). No external
dependencies — the model is pure and deterministic.
