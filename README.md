# StromaDB

**StromaDB** is an open-source, Rust **real-time GraphRAG engine optimized for LLMs**:
it fuses **meaning (vectors) × structure (typed graph) × time (bitemporal)** so an LLM can retrieve
relevant, structurally-correct context in low-ms — over a graph that is updated by a live stream.

It targets the **bounded scale of a single organization** (per-org graph is bounded), which is what
makes low-cost *and* high-performance achievable at once: the hot working set fits in memory, the
footprint is small, and idle tenants can scale to zero.

> Status: **core engine implemented and measured** — durable changelog (framed WAL, group-commit
> fsync), IVF-PQ vector index with exact re-rank, typed hybrid reads, live queries, composable query
> IR. Pre-1.0: single-node, single-threaded serving; see [docs/DECISIONS.md](docs/DECISIONS.md) for
> known limitations and roadmap. Source-available (Elastic License v2).

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

## Performance (measured, reproducible)

Single node, single thread, in-process. Synthetic clustered 768-d vectors (bge-class distribution),
Apple M-series laptop. Every row reproduces with one command from `crates/stroma-core/examples/`.

| What | Result | Reproduce |
|---|---|---|
| Hybrid read — vector top-10 + type/label filter + 1-hop expand, **while durably writing** | **p50 0.86 ms / p99 1.84 ms** @ 0.5M docs | `--example c2b_integrated` |
| Write → query-visible (durable fsync + vector add + consistent view refresh) | **single-digit ms**; view refresh is O(changed keys), not O(state) | `--example c2b_integrated` |
| Filtered recall@10 @ 50% type selectivity (overlapping-cluster data, exact re-rank) | **~0.99–1.0** at ~1 ms warm p99 | `--example ann_nprobe_curve` |
| Cold-start recovery (RTO) | **0.81 s for 5M facts**; torn-write → **0 data loss** | `--example durability_slo` |
| Ingest (append + group-commit fsync) | **~7M facts/s** | `--example durability_slo` |
| Hot-tier memory | **96 B/vector** PQ codes (32× vs raw f32); the raw re-rank tier is cold/SSD-able | `--example ann_slo` |
| Integrated open-loop (writes + reads + live queries) | **0 data loss**, version-consistent reads | `--example c2b_integrated` |

Notes: numbers are from our runs on the hardware above — run the examples on yours. Tail latencies
(p99) are reported, not just medians. No vendor comparisons here; see
[docs/DECISIONS.md](docs/DECISIONS.md) for known limitations (single-threaded serving, file-WAL
compaction pending, cold-SSD re-rank caveat).

## Where Vesicle fits

StromaDB is the OSS core. **Vesicle** is the commercial managed layer (real-time source→graph
mapping, Zero-ETL CDC, managed multi-tenant, scale-to-zero) built on top of it.
