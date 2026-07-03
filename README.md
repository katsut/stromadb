# StromaDB

**StromaDB** is an open-source, Rust **real-time GraphRAG engine optimized for LLMs**:
it fuses **meaning (vectors) × structure (typed graph) × time (bitemporal)** so an LLM can retrieve
relevant, structurally-correct context in low-ms — over a graph that is updated by a live stream.

It targets the **bounded scale of a single organization** (per-org graph is bounded), which is what
makes low-cost *and* high-performance achievable at once: the hot working set fits in memory, the
footprint is small, and idle tenants can scale to zero.

> Status: **core engine implemented and measured** — durable changelog (framed WAL, group-commit
> fsync), IVF-PQ vector index with exact re-rank, typed hybrid reads, a composable query IR
> (point / type-ANN / expand / filter / top-k), incremental Live Query maintenance, and a `stroma`
> CLI. Pre-1.0: single-node, single-threaded serving; see [docs/DECISIONS.md](docs/DECISIONS.md) for
> known limitations and roadmap. A `stroma` CLI and a `stroma-serve` HTTP surface ship alongside the
> engine. Source-available (Elastic License v2).

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
- **Composable operator query IR** — `point / type-ANN / expand / filter / top-k` composed as a
  pipeline (filters cover type, current value, and **valid-time as-of** value). Standing queries are
  maintained incrementally: recompute-and-diff generally, plus **keyed-incremental** maintenance for
  completeness/rule queries (O(touched), verified equal to a full recompute). Unifying one-shot and
  Live evaluation under one algebra is the design direction; full differential-dataflow maintenance of
  arbitrary pipelines is on the roadmap.
- **Temporal reads** — facts carry valid-time; **valid-time as-of** point reads return the value in
  effect at a past instant, and transaction-time as-of is a version-vector pin (`strict` / `fresh`
  read modes). Full temporal query scopes (`ever` / `overlap`, and valid-time over multi-valued
  edges) are on the roadmap.
- **No internal model** — a deterministic retrieval/query layer; the LLM is always the caller.
  Model-written summaries are stored with provenance, kept distinct from asserted facts.
- **Self-hostable single-node engine** under a source-available license.

See **[SPEC.md](SPEC.md)** for the capability/constraint contract,
**[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** for the design, and
**[docs/DECISIONS.md](docs/DECISIONS.md)** for *why* the engine is shaped this way — the decision trail
with the measurements that settled each call (and the known limitations / roadmap).

## Quickstart (CLI)

```bash
cargo install --path crates/stroma-cli   # installs the `stroma` binary

stroma init --db ./mydb

cat > data.jsonl <<'EOF'
{"type_def":{"name":"Person"}}
{"type_def":{"name":"Project"}}
{"pred_def":{"name":"works-on","cardinality":"many","domain":"Person","range":"Project"}}
{"pred_def":{"name":"age","cardinality":"one","domain":"Person","range_value":"int"}}
{"node":{"id":1,"type":"Person"}}
{"node":{"id":2,"type":"Project"}}
{"fact":{"subject":1,"predicate":"works-on","object":{"node":2}}}
{"fact":{"subject":1,"predicate":"age","object":{"int":34}}}
EOF
stroma ingest data.jsonl --db ./mydb     # durable (fsync per chunk), typed, validated

echo '{"node":1,"vector":[1.0,0.0,0.0,0.0]}' > emb.jsonl
stroma embed emb.jsonl --db ./mydb       # embeddings are received, never computed

stroma query point 1 age --db ./mydb                     # {"one":{"int":34}}
stroma query expand 1 works-on --db ./mydb               # {"nodes":[2]}
echo '[1.0,0.0,0.0,0.0]' > q.json
stroma query search --type Person --k 5 --vector-file q.json --db ./mydb
stroma stats --db ./mydb
```

The database directory holds only the authoritative inputs (changelog WAL, schema/node
assignments, received embeddings); derived stores (the vector index) rebuild on open.

## Quickstart (Docker)

Run the HTTP surface with no local Rust toolchain — a fresh data volume is initialized on first run:

```bash
docker compose up            # builds the image, serves on localhost:7687 (persisted in a volume)
# or without compose:
docker build -t stromadb .
docker run -p 7687:7687 -v stroma-data:/data stromadb

curl -s localhost:7687/health
curl -s -X POST localhost:7687/ingest -d '{"type_def":{"name":"Person"}}'
```

The image ships `stroma-serve` (entrypoint), plus the `stroma` CLI and `stroma-mcp` binaries.

## Serve (HTTP)

`stroma-serve` exposes the same database over HTTP so an agent or service can query and ingest it
without embedding the engine — the intended surface for an LLM caller.

```bash
stroma-serve --db ./mydb --addr 127.0.0.1:7687   # worker pool: concurrent reads, exclusive writes

curl -s localhost:7687/health
curl -s -X POST localhost:7687/query  -d '{"op":"expand","subject":1,"predicate":"works-on"}'
curl -s -X POST localhost:7687/query  -d '{"op":"search","type":"Person","vector":[...],"k":10,"allowed_labels":7}'
# retrieve_context: assembled, date-stamped, current-value context ready for an LLM
curl -s -X POST localhost:7687/query  -d '{"op":"retrieve_context","type":"Doc","vector":[...],"content":"body","date":"created_at","k":8}'
curl -s -X POST localhost:7687/ingest -d '{"fact":{"subject":1,"predicate":"works-on","object":{"node":2}}}'
curl -s localhost:7687/stats
```

Settings come from flags or environment variables (flag > env > default) — see
**[docs/CONFIGURATION.md](docs/CONFIGURATION.md)** and `.env.example`.

Reads are authz-scoped (`allowed_labels` is the caller's ABAC bitmask) and stamped with an `as_of`
version vector. v1 handles requests sequentially (single-threaded engine, pre-1.0); concurrent reads
are on the roadmap.

## MCP (agent tools)

`stroma-mcp` speaks the Model Context Protocol over stdio, exposing the database as tools an LLM agent
can call directly — `point`, `expand`, `search` (authz-scoped hybrid), `stats`, `ingest`.

```bash
stroma-mcp --db ./mydb          # newline-delimited JSON-RPC 2.0 over stdin/stdout
```

Point an MCP client at that command; `tools/list` returns the schemas, `tools/call` runs a tool and
returns the JSON result as text content.

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
