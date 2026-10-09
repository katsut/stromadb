# Configuration

StromaDB is configured with command-line flags and environment variables. Precedence, highest first:

**`--flag <value>`  →  `$ENV_VAR`  →  built-in default.**

The server runs as `stroma serve` (or `stroma up`, which also initializes a missing directory —
defaulting it to `./stroma-db`); the standalone `stroma-serve` binary ships too and takes the same
flags. There is **no JVM-style runtime tuning** — no heap size, GC, or JIT settings. Memory is
managed by the OS directly; the only knob that bounds resident memory is `STROMA_MAX_UNMERGED`
(below).

## Settings

| Env var | Flag | Default | Applies to | Meaning |
|---|---|---|---|---|
| `STROMA_DB` | `--db <dir>` | `.` | cli, serve, mcp | Database directory. `stroma-serve`/`stroma-mcp` create it on first run if it is empty. |
| `STROMA_ADDR` | `--addr <host:port>` | `127.0.0.1:7687` | serve | HTTP bind address. Use `0.0.0.0:7687` to accept connections from outside the host (e.g. in Docker). Port `7687` is the graph-database convention. |
| `STROMA_MAX_UNMERGED` | `--max-unmerged <n>` | `8000000` | serve, mcp | Upper bound on the un-merged read-merge tail (appended-but-not-materialized writes). This is the backpressure threshold and the main resident-memory knob: **larger** = more RAM headroom before backpressure; **smaller** = backpressure sooner, less memory. Not persisted — it is a per-process property. |
| `STROMA_ADMIN_USER` | `--admin-user <name>` | `admin` | serve | Console login username. |
| `STROMA_ADMIN_PASSWORD` | `--admin-password <pw>` | `password` | serve | Console login password. **Change this before exposing the server** — while the default is in use, `stroma-serve` prints a startup warning. |
| `STROMA_API_TOKEN` | `--api-token <token>` | *(unset)* | serve | Legacy single API token: one unnamed, unrestricted bearer. When set, requests carrying `Authorization: Bearer <token>` are authorized without the login/cookie flow. Prefer named tokens (below). |
| `STROMA_TOKENS` | `--tokens <file>` | *(unset)* | serve | **Named token registry** (JSON: `{"tokens":[{"name":"support-agent","token":"...","labels":15,"read_only":true}, …]}`). Each token carries a client identity — its name is stamped as provenance on un-sourced writes — plus an optional ABAC `labels` cap (intersected with every read's `allowed_labels`) and an optional `read_only` bit. No tokens configured at all = bearer auth disabled (cookie-only). |
| `STROMA_SSE_HEARTBEAT` | `--sse-heartbeat <secs>` | `15` | serve | Seconds of silence after which an idle `GET /events/stream` connection gets a comment line, so proxies keep it open. A whole number above 0; anything else is refused at startup. |
| — | `--demo` | `false` | serve | Boot with the bundled sample org graph (seeded only into an empty database) and print first-run queries plus an MCP connection snippet with a minted `demo-agent` token. With no `--db`/`$STROMA_DB`, the demo gets its own directory under the OS temp dir. |
| `STROMA_ALLOW_RESET` | `--allow-reset` | `false` | serve | Enable `POST /reset`, which **clears the entire database** (or, under `/ns/<name>/reset`, that namespace only). Off by default; intended for dev/demo/test. Set `STROMA_ALLOW_RESET=1` (or pass the flag). Still requires auth, and read-only tokens are always refused. The console's settings panel (⚙) exposes it as **Reset database**, the last item in its danger zone, with a typed `RESET` confirmation; without the flag the action is shown disabled with the server's hint. `GET /me` reports `allow_reset` and `read_only` for the caller. The namespaces page also offers a per-namespace **Reset…** action, shown only when reset is enabled and the credential is writable; it requires typing the namespace name, clears that namespace's data, and leaves the namespace registered. |

`RUST_BACKTRACE=1` is honored by the Rust runtime for panic diagnostics.

## Console authentication

The `stroma-serve` HTTP surface is gated by a session login. On success the server sets an
`HttpOnly`, `SameSite=Strict` session cookie (12-hour expiry; sessions are in-memory and clear on
restart). Every endpoint requires a valid session **except** `GET /health` (for container probes)
and the login page / `POST /login`. Unauthenticated API calls receive `401`; unauthenticated page
loads are served the login page. `POST /logout` ends the session.

State-changing requests (`POST`, `PUT`, `PATCH`, `DELETE`) authenticated by the session cookie
must also come from the server's own origin: the `Origin` header (or, when it is absent, the
`Referer`) must name the same `host[:port]` as the request's `Host` header, otherwise the server
answers `403`. A cookie-authenticated state change with neither header is refused too. The scheme
is not compared, so a TLS-terminating proxy works as long as it forwards the original `Host`.
`POST /login` refuses a present `Origin` that does not match. Browsers send `Origin` on these
requests automatically, so the console is unaffected. Bearer-token requests and `GET` requests
are not checked.

Credentials come from the settings above (default `admin` / `password`). There is no cookie
`Secure` flag yet, so put the server behind TLS (or keep it on localhost) if the network is
untrusted. The MCP stdio surface is local and is not affected by this login.

For **programmatic clients** (a service or agent, not a browser), register tokens and send one as
`Authorization: Bearer <token>` — this authorizes `/query`, `/ingest`, `/mcp`, and the other gated
endpoints without the login/cookie round-trip. Prefer the **named registry** (`--tokens`): each
client gets an identity (stamped as provenance on its un-sourced writes), an optional ABAC label
cap on reads (intersected per request — a client can narrow itself, never widen), and an optional
read-only bit (writes answer a clear 403). The legacy single `STROMA_API_TOKEN` remains an unnamed,
unrestricted entry. Tokens are compared in constant time; sessions (the console) are unrestricted.
Configure none of them to keep bearer auth disabled (cookie-only). Put the server behind TLS when
sending a token over an untrusted network.

`GET /me` tells any authenticated caller what it may do and what it is talking to. It returns
`user`, `read_only`, `allow_reset`, `reset_hint`, `auth` (`session`, `token` or `open` under
`--no-auth`), `token_name` and `labels` (the registry entry's name and label cap, `null` for
sessions and the legacy token), and the server facts `version`, `db_path`, `workers` and `mcp_url`
(the MCP endpoint on the bound address). The console's settings panel renders from it.

## Namespaces

One server can front several isolated databases, so an unrelated dataset (say, an event log you
want to explore) does not mix its types, predicates and node ids into the graph an app already
uses. The `--db` directory is the **`default`** namespace, exactly as before. A named namespace is
an ordinary database directory under it:

```
<db>/            default namespace (wal.log, schema.jsonl, …)
<db>/ns/ocel/    namespace "ocel" — a complete database directory of its own
<db>/ns/crm/     namespace "crm"
```

- **Routing.** A path `/ns/<name>/<rest>` is served by namespace `<name>` with `/<rest>` as the
  endpoint: `/query`, `/ingest`, `/embed`, `/events`, `/stats`, `/compact`, `/reset`, `/mcp`, and
  the console at `/ns/<name>/`. Every unprefixed path goes to `default`, so existing clients are
  unaffected and `/ns/default/...` is an alias. `/health`, `/login`, `/logout`, `/me` and
  `GET /namespaces` are global and never prefixed.
- **Names** match `[a-z0-9_-]{1,64}`; anything else answers `400`. `default` is reserved.
- **Creation.** A namespace is created only by `POST /namespaces` with `{"name":"<name>"}`: `201`
  on success, `400` for a malformed name, `409` if it already exists (left untouched), `403` for a
  read-only token. Any request to a namespace that does not exist, including `/ingest`, `/embed`,
  `/compact`, `/reset`, `/mcp` and the console page, answers `404` and creates nothing. Create the
  namespace first, then ingest into it.
- **Lifetime.** Each namespace's database is opened on first use, with the same
  `--max-unmerged` bound as `default`, and stays open until the server exits.
- **Listing.** `GET /namespaces` returns `{"namespaces":[{"name","nodes","facts","loaded","durable_head"}, …]}`,
  `default` first then the existing named ones sorted, each with its node and fact counts (the
  same counters `GET /stats` reports). Listing never opens a namespace: an open one (`"loaded":
  true`) reports its live counters, an unopened one the counts its directory last persisted in
  `counts.json` (rewritten after every write), or `null` counts if it has none yet. `durable_head` is the live durable head of an open namespace
  and `null` for an unopened one. The console's
  topbar namespace selector is built from this.
- **Access.** Sessions and tokens are server-wide: one login or token reaches every namespace, and
  a token's label cap, read-only bit and provenance stamping apply unchanged inside each. There
  are no per-namespace credentials, so `GET /namespaces` lists every namespace to every
  authenticated caller alike (there is nothing narrower to show a non-admin one).
  `--allow-reset` lets `/ns/<name>/reset` clear that namespace only; `--demo` seeds only `default`.

The console works under a namespace too: open `http://localhost:7687/ns/ocel/` and every call it
makes goes to `ocel`, with the namespace name shown next to the logo. MCP clients are pointed at
one namespace by its URL, e.g. `http://localhost:7687/ns/ocel/mcp`.

Because a namespace is just a database directory, the offline tools address one by path:
`stroma init --db <db>/ns/<name>` creates it, `stroma import data.csv --db <db>/ns/<name> …`
loads into it, and `stroma-mcp --db <db>/ns/<name>` serves it over stdio. The usual directory lock
applies: once the server has opened a namespace it holds that directory, so stop the server before
writing to it offline.

## Using a `.env` file

The binaries read variables from the process environment; they do not auto-load `.env`. Copy
[`.env.example`](../.env.example) and either export it —

```bash
set -a; . ./.env; set +a
stroma serve
```

— or, with Docker Compose, reference it via `env_file:` (or the `environment:` block already in
[`docker-compose.yml`](../docker-compose.yml)).

## Deployment shape

The server runs a worker pool sharing its databases (the default one plus any open namespaces): reads (`/query`, `/stats`, `/health`) are
**lock-free** — each pins the current read view and runs on it with no lock held, so an in-flight
write never blocks a read; writes (`/ingest`, `/embed`) serialize on the database's internal write
mutex and publish a fresh view on completion. The worker count defaults to the available
parallelism (clamped to 2–32). A thread-count setting, TLS, and structured logging are on the
roadmap; none of those are configurable yet because they are not built yet.
