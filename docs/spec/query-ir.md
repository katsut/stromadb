# Spec — Composable operator IR (tool surface)

> Contract for `stroma-core::ir`. Companion HOW: `../architecture/query-ir.md`. Status: implemented
> (Epic 6). The agent-facing tool surface: submit a pipeline, get one-shot results (CAP-10) with authz
> and the result contract.

## Principal (authz)

`Principal { allowed_labels: u32 }` — an ABAC label bitmask. `can_see_label(l)` = bit l set.
Unlabeled nodes are public (visible to all). (Deny-by-default for unlabeled is future hardening.)

## Traverser

`Traverser { ids, scores, as_of: VersionVector }` — what flows between operators and is returned:
an id set, per-id score, and the version vector the read was pinned at.

## Pipeline

`Pipeline { source, transforms, max_nodes, mode }` (`mode` = strict/fresh).

Sources:
| source | meaning |
|---|---|
| `Point { subjects }` | identity lookup of specific nodes |
| `TypeAnn { q, target_type, k }` | type-aware hybrid: k nearest of `target_type`, **authz+type+version scoped** |

Transforms:
| transform | meaning |
|---|---|
| `Expand { predicate }` | 1-hop expand (structural), authz-filtered |
| `TopK { k }` | keep top-k by current score |

## Execution — `run(snapshot, catalog, vector, pipeline, principal, vv) -> Traverser`

- **authz at the head (H4):** the principal's authz predicate is threaded into the source as a
  *scoped* filter (`TypeAnn` only ever scores authorized nodes — no shared-index post-filter leak);
  expanded nodes are authz-filtered too. Callers cannot skip it.
- **result contract:** the traverser is bounded by `max_nodes` (token budget) at every step and
  stamped with `vv` (as_of). `mode` selects strict/fresh on the vector axis.
- **single algebra:** these are the same read operators one-shot and Live Query use.

## Out of scope (later)
- More operators: `temporal` (now/as-of/ever/overlap), `filter` (fact-attribute predicates),
  `score-rank` with agent-supplied weights.
- A real micro-planner (operator fusion / predicate pushdown / cardinality reorder; post-authz
  cardinality exposure).
- openCypher front-end → IR lowering.
- decision-provenance audit (as_of + referenced fact ids).
- CAP-11 collaborative-abstraction / CAP-12 absence-detection operators (Epic 6.3, stretch).
