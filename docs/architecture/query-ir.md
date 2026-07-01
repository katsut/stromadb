# Architecture — Composable operator IR

> Design for `stroma-core::ir`. Companion WHAT: `../spec/query-ir.md`. Overview:
> `../ARCHITECTURE.md` §5, §9. Status: implemented (Epic 6).

## One algebra, one-shot (CAP-10)

A query is a pipeline of composable operators over a traverser (id set + scores + as_of). It is
submitted once and evaluated server-side in one shot — the agent composes small primitives instead of
issuing a query language. The same operators back Live Query (Epic 5), so one-shot and reactive reads
are the *same algebra*, differing only in whether the result is returned once or maintained.

## authz at the head = source scoping (H4)

The injection spike (`poc-authz-index-leak`) showed a shared index + post-authz filter leaks the
unauthorized-near count (timing + top-k completeness). So authz is not a post-filter — it is threaded
into each **source** as a scope: `TypeAnn` searches only authorized nodes (`nearest_scoped` with the
authz predicate), and expands only cross authorized edges. A principal never scores or ranks against
data it can't see, so there is no inference channel. The engine injects this at the head; a pipeline
cannot opt out.

## The result is a contract

Every traverser is (a) bounded by `max_nodes` — the token budget, enforced at every step so a
sub-graph explosion can't blow the caller's context — and (b) stamped with the version vector
(`as_of`), so the result is self-describing: the caller knows exactly which cross-store cut it
reflects and can re-pin or audit against it. `mode` (strict/fresh) picks the vector-axis semantics.

## Thin planner, agent-side macro plan

The executor is deliberately thin: it scopes sources for authz and caps the traverser. Operator
fusion / predicate pushdown / cardinality reorder (post-authz) are the future micro-planner; the
*macro* plan (which questions, in what order) stays with the agent — the DB supplies fast primitives,
the intelligence is caller-side.

## Forward references
- Operators: `temporal` (reads `one_history` / valid-time), `filter` (fact-attribute predicates),
  `score-rank` with agent weights (deterministic fusion; judgment stays agent-side).
- openCypher subset → IR lowering (optional front-end, later).
- decision-provenance audit hook (as_of + referenced fact-id set).
- CAP-12 absence-detection / CAP-11 staleness as IR operators (stretch).
