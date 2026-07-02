# Spec — Data model & type catalog

> Contract for the `stroma-core` data model and catalog (crate `crates/stroma-core`).
> Companion HOW: `../architecture/data-model.md`. Status: implemented (Epic 1, Story 1.1).

## Fact

The single unit of the data model. Nodes and edges are *projections* of facts.

```
Fact = ⟨ subject, predicate, object, valid_time, transaction_time, provenance, confidence ⟩
```

| field | type | meaning |
|---|---|---|
| `subject` | `NodeId` (u64) | the entity the fact is about |
| `predicate` | `FieldId` (u32) | interned predicate id (see Catalog) |
| `object` | `Object` | `Node(NodeId)` (an edge) or `Value(Value)` (an attribute) |
| `valid_time` | `ValidTime` | `{ from: i64, to: Option<i64> }`; `to = None` ⇒ open (currently valid) |
| `transaction_time` | u64 | assigned by the changelog on append (0 until persisted) |
| `provenance` | `Provenance` | `{ kind: Asserted | Derived, source: FieldId }` |
| `confidence` | f32 | assertion confidence |

`Value = Int(i64) | Float(f64) | Text(String) | Bool(bool)`.

**Rules**
- Queries default to `Asserted`; `Derived` (LLM/hypothesis) is returned only on explicit request.
- `transaction_time` is owned by the changelog; producers leave it 0.

## Type catalog (declarative half)

Vocabulary + structural rules — in effect a *lightweight ontology* (entity types, predicates with
domain/range, cardinality, relation properties), deliberately **without** axioms or a reasoner.
Bounded by design (tens–hundreds of predicates).

- **Field-ID interning** — every type/predicate name interns to a stable `FieldId`. Names ↔ ids are
  1:1 and stable for the catalog's lifetime.
- **Type** — a registered entity type (`register_type(name) -> FieldId`).
- **Predicate** (`PredicateDef`):
  | field | meaning |
  |---|---|
  | `id` | interned predicate id |
  | `cardinality` | `One` (functional → supersede) or `Many` (→ accumulate). Drives the fold. |
  | `props` | `RelProps { symmetric, transitive, inverse: Option<FieldId> }` — expanded at query time, never pre-materialized |
  | `domain` | subject entity type (`FieldId`) |
  | `range` | `Type(FieldId)` (edge → entity type) or `Value(ValueType)` (attribute) |

## Minimal constraint validation (ingest boundary)

`Catalog::validate(&Fact) -> Result<(), ConstraintError>` enforces the minimum (not a reasoner):

1. predicate must be registered — else `UnknownPredicate`.
2. if the subject's type is known (`set_node_type`), it must equal `domain` — else `DomainMismatch`.
3. object must match `range`:
   - `Range::Type(t)` requires `Object::Node(n)`; if `n`'s type is known it must equal `t`
     (`RangeTypeMismatch`); a `Value` object is `ExpectedNodeObject`.
   - `Range::Value(vt)` requires `Object::Value(v)` whose value-type equals `vt`
     (`RangeValueMismatch`); a `Node` object is `ExpectedValueObject`.

**Out of scope here** (deferred to other components): cardinality enforcement (it is a multi-fact
property → the **fold**, Story 1.2), full schema reasoning (relocated to the caller), and dynamic
schema-evolution lease semantics (CAP-6).

## Invariants

- A `FieldId` never changes meaning once interned.
- Unknown subject/object node types are *permitted* at validation time (open-world); validation only
  fails on *known* mismatches. Absence is handled separately (CAP-12).
