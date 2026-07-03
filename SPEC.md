# StromaDB — Spec

The capability / constraint / non-goal contract for the StromaDB core. Technical scope only.
Companion: `docs/ARCHITECTURE.md`.

## Capabilities

- **CAP-1 — High-frequency stream ingest, no write stalls.** Ingest org events/activity/knowledge
  into the graph in real time, append-only. Under sustained load no hard write stall occurs; overload
  returns explicit backpressure (never silent OOM/stop).
- **CAP-2 — Co-located typed core for low-I/O local traversal.** Type/attributes + adjacency
  skeleton are logically co-located so point lookups and 1–2 hop traversals resolve in few I/Os.
  Vectors live in a *separate* quantized index (nodes hold a pointer; see CAP-3).
- **CAP-3 — Type-aware hybrid search.** ANN candidates are filtered/reranked by
  graph type/constraints, returning only semantically-coherent results (disjoint-type mis-fusion
  rejected), cheaply and deterministically on the hot path. Recall completeness =
  ANN(indexed) ∪ brute-force(bounded un-indexed-but-embedded tail), closing index/structure split-brain.
- **CAP-4 — Write-append / read-merge.** Partial updates are appended without read-modify-write and
  merged zero-copy at read time; un-merged diffs are bounded (N ≤ N_max) with sync materialization on
  overflow.
- **CAP-5 — Reactive Live Query (IVM).** Registered decision queries are incrementally maintained and
  result diffs pushed to subscribers, for the monotonic/bounded-diff query class (filter/equi-join/
  windowed aggregate). Live Query count and IVM memory are capped.
- **CAP-6 — Dynamic schema evolution.** A Field-ID catalog separates logical/physical so types and
  properties can be added without downtime (additive changes). Meaning-changing changes require an
  explicit migration.
- **CAP-7 — Self-hostable OSS core.** Run a single-org engine (ingest → hybrid query → Live Query)
  on one node under a source-available license.
- **CAP-10 — Composable operator query model (one algebra).** Search/traversal is expressed as
  composable primitives an agent chains in a loop; the *same* pipeline runs as a one-shot evaluation
  or an incrementally-maintained Live Query.
- **CAP-11 — Collaborative abstraction layer.** Store caller-generated summaries/abstractions as graph
  data (provenance-stamped, distinct from asserted facts) and cheaply detect *structural staleness*
  when the source subgraph changes.
- **CAP-12 — Absence detection (expected-but-absent).** Against the declared schema and opt-in
  completeness profiles, detect and surface facts/relations that *should* exist but don't
  (negative knowledge) — deterministic, structural, post-authz. Scope: absence only.

## Constraints

- **Bounded target scale.** Per-org bounded graph (≈ millions–tens-of-millions of nodes, GB-class).
  The physical bound is an *envelope* (total facts × degree distribution × vector dim/count ×
  concurrent Live Queries × hot working-set) drawn against a per-tenant cost budget. Over-ceiling →
  degrade (latency) / shard / reject — never silently melt.
- **No internal model (deterministic engine).** No model inference inside the engine; the LLM is
  always the caller. The engine stores/serves/staleness-checks semantic summaries.
- **Physical.** Rust + rkyv zero-copy. Writes = LSM (RocksDB/Speedb; O_DIRECT). Materialization =
  Lance V2 columnar (cold tier). Co-location is *logical* (io_uring coalescing), physical is separate.
  Every feature states whether its state is volatile (IVM) or durable (changelog).
- **Node physical layout.** Typed core (type/hot props/adjacency skeleton) co-located and
  cache-resident; **vectors in a separate quantized index (IVF-PQ/DiskANN) + pointer**; high-degree
  hubs spill adjacency; fragment by community.
- **Query planning.** Macro plan is the agent's; the engine is a thin micro-planner (operator fusion,
  predicate pushdown, cardinality reorder). **Cardinality estimates are post-authz.**
- **Result contract.** Each result is within a token budget and stamped with an `as_of` **version
  vector** (changelog seqno, Lance ver, ANN index ver, MVCC txid) exposing cross-store skew. Two read
  modes: **strict** (all stores at min-watermark = fully consistent, audit/repro) and **fresh** (each
  store latest, vector-axis skew ≤ B, agent default). Sessions pin a snapshot, may re-pin (bounded
  staleness).
- **Access control is first-class.** Authz is injected at the head of every composed pipeline; agents
  query on behalf of an end-user principal (delegation, cross-principal leakage prevented). ABAC
  label-based; tenant namespace isolation is the outermost boundary. Derived/cached summaries inherit
  source labels.
- **Embeddings.** Pre-computed and received (the engine does not embed). Model/dim changes run a new
  versioned index in parallel; mixed versions are rejected by index version.
- **Operational completeness.** DR of the authoritative subset (changelog + type catalog +
  received embeddings) is in scope; derived stores rebuild on restore. Migration/backfill =
  snapshot(LSN) → map → bulk fold → CDC from that LSN. Observability = per-primitive SLO metrics +
  agent-pipeline traces + quality proxies; deterministic changelog replay for reproduction.

## Non-goals

- Giant-enterprise bespoke deployments; web-scale (billion-node) graphs.
- Real-time bidirectional inference on the hot path (batch only).
- A full schema reasoner (OWL/description-logic); only minimal type/constraint validation.
- Heavy multi-tenant isolation machinery; heavy cost-based query optimizers.
- General-purpose OLTP RDBMS / batch-analytics DWH replacement; multi-region distribution (v1).

## Success signal

A bounded-scale organization ingests its data via stream/CDC and its AI agents pull
meaning × structure × type fused decision context in low-ms, with type-aware hybrid search returning
semantically-coherent evidence even as the graph updates — at a fraction of large-enterprise cost,
self-hosted or managed.
