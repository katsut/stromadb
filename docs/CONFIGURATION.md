# Configuration

StromaDB is configured with command-line flags and environment variables. Precedence, highest first:

**`--flag <value>`  →  `$ENV_VAR`  →  built-in default.**

There is **no JVM-style runtime tuning** — no heap size, GC, or JIT settings. Memory is managed by
the OS directly; the only knob that bounds resident memory is `STROMA_MAX_UNMERGED` (below).

## Settings

| Env var | Flag | Default | Applies to | Meaning |
|---|---|---|---|---|
| `STROMA_DB` | `--db <dir>` | `.` | cli, serve, mcp | Database directory. `stroma-serve`/`stroma-mcp` create it on first run if it is empty. |
| `STROMA_ADDR` | `--addr <host:port>` | `127.0.0.1:7687` | serve | HTTP bind address. Use `0.0.0.0:7687` to accept connections from outside the host (e.g. in Docker). Port `7687` is the graph-database convention. |
| `STROMA_MAX_UNMERGED` | `--max-unmerged <n>` | `8000000` | serve, mcp | Upper bound on the un-merged read-merge tail (appended-but-not-materialized writes). This is the backpressure threshold and the main resident-memory knob: **larger** = more RAM headroom before backpressure; **smaller** = backpressure sooner, less memory. Not persisted — it is a per-process property. |

`RUST_BACKTRACE=1` is honored by the Rust runtime for panic diagnostics.

## Using a `.env` file

The binaries read variables from the process environment; they do not auto-load `.env`. Copy
[`.env.example`](../.env.example) and either export it —

```bash
set -a; . ./.env; set +a
stroma-serve
```

— or, with Docker Compose, reference it via `env_file:` (or the `environment:` block already in
[`docker-compose.yml`](../docker-compose.yml)).

## Deployment shape (v1)

`stroma-serve` is single-threaded (one writer, requests handled sequentially) — the honest pre-1.0
shape. Concurrent reads, a thread-count setting, TLS, and structured logging are on the roadmap; none
are configurable yet because they are not built yet.
