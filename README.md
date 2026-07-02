# StromaDB

**StromaDB** is an open-source, Rust **real-time GraphRAG engine optimized for LLMs**:
it fuses **meaning (vectors) × structure (typed graph) × time (bitemporal)** so an LLM can retrieve
relevant, structurally-correct context in low-ms — over a graph that is updated by a live stream.

It targets the **bounded scale of a single organization** (per-org graph is bounded), which is what
makes low-cost *and* high-performance achievable at once: the hot working set fits in memory, the
footprint is small, and idle tenants can scale to zero.

> Status: design complete, **Phase 0 validation done** (kill-switch spikes for the load-bearing
> technical bets). Pre-implementation. Source-available license (Elastic License v2, first candidate).

## Why

Real-time LLM retrieval needs a graph that ingests a stream instantly and answers
**type-aware hybrid** queries cheaply. Existing options don't fit this shape:

- Vector DBs are **type-blind** — they return semantically near but structurally wrong results
  (a "Python" skill, doc, and person all look alike to pure ANN).
- Property graphs (Neo4j/…) are batch-oriented, not stream-native.
- `Postgres + pgvector` splits meaning from structure across separate I/O paths and contends on
  stream updates.

StromaDB is built for LLM retrieval: stream-native, vector + typed-graph, low-cost, bounded-scale.

## Core capabilities

- **Type-aware hybrid search** — ANN candidates filtered/reranked by graph type, so disjoint-type
  mis-fusion is rejected.
- **Stream ingest, no write stalls** — append-only changelog; explicit backpressure under overload.
- **Composable operator query IR** — `point / type-ANN / expand / temporal / filter / score-rank`
  composed as a pipeline; **one algebra** evaluates both one-shot queries and incrementally-maintained
  Live Queries (IVM).
- **Bitemporal** — valid-time + transaction-time; `now / as-of / ever / overlap` time scopes.
- **No internal model** — a deterministic retrieval/query layer; the LLM is always the caller.
  Model-written summaries are stored with provenance, kept distinct from asserted facts.
- **Self-hostable single-node engine** under a source-available license.

See **[SPEC.md](SPEC.md)** for the capability/constraint contract,
**[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** for the design, and
**[docs/DECISIONS.md](docs/DECISIONS.md)** for *why* the engine is shaped this way — the decision trail
with the measurements that settled each call (and the known limitations / roadmap).

## Where Vesicle fits

StromaDB is the OSS core. **Vesicle** is the commercial managed layer (real-time source→graph
mapping, Zero-ETL CDC, managed multi-tenant, scale-to-zero) built on top of it.
