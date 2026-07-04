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
| `STROMA_ADMIN_USER` | `--admin-user <name>` | `admin` | serve | Console login username. |
| `STROMA_ADMIN_PASSWORD` | `--admin-password <pw>` | `password` | serve | Console login password. **Change this before exposing the server** — while the default is in use, `stroma-serve` prints a startup warning. |
| `STROMA_API_TOKEN` | `--api-token <token>` | *(unset)* | serve | API token for programmatic clients. When set, requests carrying `Authorization: Bearer <token>` are authorized without the login/cookie flow. Unset = bearer auth disabled (cookie-only). |

`RUST_BACKTRACE=1` is honored by the Rust runtime for panic diagnostics.

## Console authentication

The `stroma-serve` HTTP surface is gated by a session login. On success the server sets an
`HttpOnly`, `SameSite=Strict` session cookie (12-hour expiry; sessions are in-memory and clear on
restart). Every endpoint requires a valid session **except** `GET /health` (for container probes)
and the login page / `POST /login`. Unauthenticated API calls receive `401`; unauthenticated page
loads are served the login page. `POST /logout` ends the session.

Credentials come from the settings above (default `admin` / `password`). There is no cookie
`Secure` flag yet, so put the server behind TLS (or keep it on localhost) if the network is
untrusted. The MCP stdio surface is local and is not affected by this login.

For **programmatic clients** (a service or agent, not a browser), set `STROMA_API_TOKEN` and send it
as `Authorization: Bearer <token>` — this authorizes `/query`, `/ingest`, and the other gated
endpoints without the login/cookie round-trip. The token is compared in constant time. Leave it unset
to keep bearer auth disabled (cookie-only). Put the server behind TLS when sending a token over an
untrusted network.

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

`stroma-serve` runs a worker pool sharing the database behind an `RwLock`: reads (`/query`, `/stats`,
`/health`) are concurrent; writes (`/ingest`, `/embed`) take the write lock and are exclusive. The
worker count defaults to the available parallelism (clamped to 2–32). Fully lock-free reads *during*
a write (over a pinned snapshot), a thread-count setting, TLS, and structured logging are on the
roadmap; none of those are configurable yet because they are not built yet.
