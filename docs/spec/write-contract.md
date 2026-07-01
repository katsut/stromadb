# Spec — DB↔ETL write contract

> The narrow, frozen interface between the DB core and any ingest/ETL layer (e.g. Vesicle).
> Companion HOW: `../architecture/write-contract.md`. Status: implemented (write-contract; the one
> DB↔ETL coupling — "diff reflection" + chunking).

## Diff vocabulary (`WriteKind`)

A CDC change maps to one of: `SetOne` / `CloseOne` (cardinality-One), `AddMany` / `RemoveMany`
(cardinality-Many), `HardDelete`. Each append carries a `source` (FieldId) and is assigned a
monotonic `seqno` by the changelog.

## Chunk receiver (batch append)

`Changelog::append_batch(writes)` / `Engine::write_batch(writes)` append a chunk **atomically w.r.t.
backpressure** (all-or-nothing; returns seqnos). A source chunk becomes one append; chunk size is
the ETL's decision and trades off against the backlog bound (`n_max`).

## Diff reflection (retraction resolver)

Turning a high-level "remove this edge" into an OR-Set observed-remove needs the current tags. The DB
owns that resolution so ETL never touches OR-Set internals:

- `Engine::retract_edge(source, subject, predicate, object)` → resolves the live tags from the
  effective state, appends `RemoveMany { observed }`, returns the seqno (or `None` if absent).
- `Fold::live_tags(subject, predicate, object)` → the resolver primitive (live add-tags to observe).
- cardinality-One retraction is `CloseOne` (no tag resolution needed).

## Ownership split

| ETL (Vesicle) owns | DB owns |
|---|---|
| chunking strategy; CDC change → `WriteKind` mapping; which edges to retract | seqno assignment; tag resolution (`retract_edge`); batch atomicity; backpressure |

## Out of scope (Vesicle)
Real CDC connectors, chunk-sizing policy, backfill/cutover (LSN), entity resolution, embedding
production. The DB freezes only the *receiver shape*; ETL fills it in.
