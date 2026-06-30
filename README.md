# StromaDB

**StromaDB** is an open-source, Rust, **no-LLM neuro-symbolic knowledge-graph core** for AI agents:
it fuses **meaning (vectors) × structure (ontology/graph) × time (bitemporal)** so an agent can pull
semantically-correct decision context in low-ms — over a graph that is updated by a live stream.

It targets the **bounded scale of a single organization** (per-org graph is bounded), which is what
makes low-cost *and* high-performance achievable at once: the hot working set fits in memory, the
footprint is small, and idle tenants can scale to zero.

> Status: design complete, **Phase 0 validation done** (kill-switch spikes for the load-bearing
> technical bets). Pre-implementation. Source-available license (Elastic License v2, first candidate).

## Why

Real-time, agent-driven decisions need a graph that ingests a stream instantly and answers
**type-aware hybrid** queries cheaply. Existing options don't fit this shape:

- Vector DBs are **type-blind** — they return semantically near but structurally wrong results
  (a "Python" skill, doc, and person all look alike to pure ANN).
- Property graphs (Neo4j/…) are batch-oriented, not stream-native.
- `Postgres + pgvector` splits meaning from structure across separate I/O paths and contends on
  stream updates.

StromaDB is built for the AI-agent case: stream-native, neuro-symbolic, low-cost, bounded-scale.

## Core capabilities

- **Type-aware hybrid search** — ANN candidates filtered/reranked by ontology type, so disjoint-type
  mis-fusion is rejected (the differentiator).
- **Stream ingest, no write stalls** — append-only changelog; explicit backpressure under overload.
- **Composable operator query IR** — `point / type-ANN / expand / temporal / filter / score-rank`
  composed as a pipeline; **one algebra** evaluates both one-shot queries and incrementally-maintained
  Live Queries (IVM).
- **Bitemporal** — valid-time + transaction-time; `now / as-of / ever / overlap` time scopes.
- **No internal LLM** — a deterministic substrate; the LLM is always the caller. Agent-written
  summaries are stored with provenance, kept distinct from asserted facts.
- **Self-hostable single-node engine** under a source-available license.

See **[SPEC.md](SPEC.md)** for the capability/constraint contract and
**[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** for the design.

## Where Vesicle fits

StromaDB is the OSS core. **Vesicle** is the commercial managed layer (real-time source→graph
mapping, Zero-ETL CDC, managed multi-tenant, scale-to-zero) built on top of it.
