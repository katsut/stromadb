# Architecture — DB↔ETL write contract

> Design for the DB↔ETL seam. Companion WHAT: `../spec/write-contract.md`. Overview:
> `../ARCHITECTURE.md` §2.

## Why this seam exists (and why it's the only one)

Schema *quality* (whether a good mapping was authored) does not change
the DB's *design*: the DB consumes a predicate catalog + a stream of facts regardless of who authored
them. The one place ETL realities actually meet the DB is the **write interface** — how source
changes are chunked, and how a change becomes a fold op ("diff reflection"). Freezing this contract
now (a narrow receiver) is what lets the DB core be finished independently of ETL and of B2, and
keeps a future Vesicle from forcing a DB write-API rewrite.

## Diff reflection: the DB owns tag resolution

An OR-Set retraction is an *observed-remove* — it must tombstone the tags currently present for the
element, which requires reading current state. If ETL had to do that, OR-Set internals would leak
into the ETL layer and every connector would reimplement it. So the DB exposes `retract_edge` (ETL
names the edge; the DB resolves the tags via `Fold::live_tags`) — the coupling is crushed to a single
high-level call. Supersession/close (`CloseOne`) needs no resolution.

## Chunking: ETL batches, the DB accepts atomic chunks

ETL decides how to chunk source rows (a CDC batch, a backfill page). The DB accepts a chunk as one
atomic append (`append_batch`), so a chunk either lands whole or is refused by backpressure — no
half-applied chunk. Chunk size ↔ the un-merged backlog bound (`n_max`) is the tuning knob linking ETL
throughput to read freshness; it's tuning, not a design change.

## What stays out
The DB freezes only the *shape* of the receiver (vocabulary, batch atomicity, retraction resolver).
Real CDC, chunk-sizing policy, backfill cutover (LSN), entity resolution, and embedding production
live in Vesicle. This is the two-part split holding: the DB is the stable core; ETL is the
programmable edge.
