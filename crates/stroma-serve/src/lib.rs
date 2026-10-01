//! The StromaDB HTTP serving surface: a minimal server over a directory-backed database, so an
//! agent (or any client) can query and ingest over the network instead of embedding the engine.
//! Ships as the `stroma-serve` binary and as a library entrypoint ([`run`]) the `stroma` CLI's
//! `serve` / `up` subcommands call — one install carries the whole application.
//!
//! Endpoints (JSON):
//!   GET  /health          → {"status":"ok"}          (public — container probes)
//!   GET  /login           → login page               (public)
//!   POST /login  {user,password} → session cookie     (public)
//!   POST /logout          → clears the session
//!   GET  /me              → caller's permissions {"user", "read_only", "allow_reset", "reset_hint",
//!                 "auth", "token_name", "labels"} plus server info {"version", "db_path",
//!                 "workers", "mcp_url", "app_url"} (the console's settings panel)
//!   GET  /events?since=N  → long-poll; returns {"head": M} when the durable head advances (or ~20s)
//!   GET  /stats           → engine/schema/embedding/storage counters
//!   POST /query   {op,...} → point / expand / search / neighborhood / node (see stromadb_store::Db::query)
//!   POST /ingest  <jsonl> → {defs,nodes,facts,retracts,closes,suppressed,durable_head}
//!   POST /embed   <jsonl> → {embedded: N}
//!   POST /compact         → {compacted_upto, wal_bytes, snapshot_bytes}  (snapshot + truncate)
//!   POST /mcp     <json-rpc> → MCP streamable HTTP transport: one JSON-RPC message per request;
//!                 a request gets its JSON-RPC response (200), a notification gets 202 with an
//!                 empty body. Stateless (no session ids); GET /mcp is 405 (no server stream).
//!                 Same tool set as `stroma-mcp` (shared `stromadb_store::mcp` dispatch).
//!   POST /reset           → clears the addressed database (opt-in: only when started with --allow-reset)
//!   GET  /namespaces      → {"namespaces":[{"name","nodes","facts","loaded"}, ...]}  (every
//!                 namespace that exists, default first then sorted, with its node/fact counts;
//!                 never opens a namespace — an unopened one reports the counts it last persisted
//!                 (updated on every write), or null when it has none)
//!
//! Namespaces: several isolated databases behind one server (D27). The `--db` directory is the
//! `default` namespace; a named one is an ordinary database directory at `<db>/ns/<name>/`
//! (`[a-z0-9_-]{1,64}`). A path `/ns/<name>/<rest>` addresses `<name>` with `/<rest>` as the
//! endpoint — `/query`, `/ingest`, `/embed`, `/events`, `/stats`, `/compact`, `/reset`, `/mcp` and
//! the console `/` — and any unprefixed path addresses `default`, so `/ns/default/...` is an
//! alias. `/health`, `/login`, `/logout`, `/me` and `/namespaces` are global. A namespace is
//! created by its first `POST /ingest`; any other request to a missing one is 404, a malformed
//! name 400. Databases open lazily and stay cached for the life of the process. Auth, sessions and
//! token scopes are server-wide and apply unchanged inside every namespace.
//!
//! Auth: every endpoint except `/health` and the login page/POST requires either a valid session
//! cookie (issued by `POST /login`, in-memory, 12h) or, for programmatic clients, a registered
//! bearer token. Credentials are `--admin-user`/`$STROMA_ADMIN_USER` (default `admin`) and
//! `--admin-password`/`$STROMA_ADMIN_PASSWORD` (default `password`, warned).
//!
//! Tokens: `--tokens <file>`/`$STROMA_TOKENS` loads a registry of **named tokens**
//! (`{"tokens":[{"name":"support-agent","token":"...","labels":15,"read_only":true}, ...]}`) —
//! each carries a client identity (stamped as provenance on its un-sourced writes), an optional
//! ABAC label cap (intersected with every read's `allowed_labels`, never widened), and an optional
//! read-only bit (writes get a clear 403). `--api-token`/`$STROMA_API_TOKEN` remains the legacy
//! single unnamed, unrestricted token (none configured = bearer auth disabled, cookie-only).
//! Sessions (the console) are unrestricted. The same scopes govern `/mcp`.
//!
//! Concurrency: a worker pool shares each database as a plain `Arc<Db>`. Reads (`/query`) are
//! lock-free — each pins the current read view (a momentary lock + `Arc` clone) and then runs on it
//! with no lock held, so an in-flight write never blocks a read. Writes (`/ingest`, `/embed`,
//! `/reset`) serialize on the database's internal write mutex and publish a fresh read view on
//! completion. Addresses #25.
//!
//! Config: `--db <dir>` / `$STROMA_DB` (default `.`), `--addr <host:port>` / `$STROMA_ADDR`
//! (default `127.0.0.1:7687`), `--max-unmerged` / `$STROMA_MAX_UNMERGED`. A flag overrides the env
//! var overrides the default.

use std::collections::HashMap;
use std::process::exit;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use stromadb_store::Db;
use stromadb_store::mcp;
use tiny_http::{Header, Method, Request, Response, Server};

type SharedDb = Arc<Db>;

/// Console credentials (flag/env, default `admin`/`password`) plus the API token registry for
/// programmatic clients. No tokens = bearer auth disabled (cookie-only, as before).
struct Auth {
    user: String,
    pass: String,
    tokens: Vec<TokenEntry>,
    /// Opt-in: allow `POST /reset` to clear the whole database (dev/demo). Off by default.
    allow_reset: bool,
    /// Opt-in: disable the auth gate entirely (local dev only). Off by default.
    no_auth: bool,
}

/// Active session tokens → unix-seconds expiry (in-memory; cleared on restart).
type Sessions = Arc<Mutex<HashMap<String, u64>>>;
const SESSION_TTL_SECS: u64 = 12 * 3600;

const LOGIN_HTML: &str = include_str!("login.html");

/// The refusal `POST /reset` returns without `--allow-reset`; `/me` reports the same text so the
/// console can show why its reset action is disabled.
const RESET_DISABLED: &str = "reset is disabled (start with --allow-reset to enable)";

/// Static server facts `/me` reports for the console's settings panel, fixed at startup.
struct ServerInfo {
    db_path: String,
    workers: usize,
    /// The MCP endpoint on the address actually bound (so `--addr host:0` reports the real port).
    mcp_url: String,
    /// Optional link back to the application this database feeds (`--app-url`/`$STROMA_APP_URL`).
    /// The console renders it as a topbar "Back to app ↗" link when set; `None` hides it.
    app_url: Option<String>,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 24 random bytes from the OS CSPRNG (via `getrandom`, so every supported platform gets real
/// entropy), hex-encoded — the session token. Fails closed: a CSPRNG error yields `None` and the
/// caller refuses to mint a session rather than falling back to predictable bytes.
fn new_token() -> Option<String> {
    let mut buf = [0u8; 24];
    getrandom::fill(&mut buf).ok()?;
    Some(buf.iter().map(|b| format!("{b:02x}")).collect())
}

/// Length-checked constant-time string equality (avoids per-byte early-exit timing leaks).
fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

fn header_value<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    req.headers()
        .iter()
        .find(|h| h.field.as_str().as_str().eq_ignore_ascii_case(name))
        .map(|h| h.value.as_str())
}

fn cookie_token(req: &Request) -> Option<String> {
    header_value(req, "cookie")?
        .split(';')
        .map(str::trim)
        .find_map(|kv| kv.strip_prefix("stroma_session="))
        .map(str::to_string)
}

/// A registered API token: its secret plus the identity/visibility it carries. The legacy single
/// `--api-token` becomes one unnamed, unrestricted entry, so existing deployments are unchanged.
#[derive(Clone, Debug)]
struct TokenEntry {
    /// Provenance name stamped on this client's un-sourced writes (empty = no stamping).
    name: String,
    token: String,
    /// ABAC label cap applied to every read (`None` = unrestricted).
    labels: Option<u64>,
    read_only: bool,
}

impl TokenEntry {
    fn scope(&self) -> mcp::Scope {
        mcp::Scope {
            default_source: (!self.name.is_empty()).then(|| self.name.clone()),
            allowed_labels: self.labels,
            read_only: self.read_only,
        }
    }
}

/// Parse a token registry file: `{"tokens":[{"name":"support-agent","token":"...",` `"labels":15,`
/// `"read_only":true}, ...]}`. `labels` and `read_only` are optional (default unrestricted /
/// writable); `name` and a non-empty `token` are required — a registry entry exists to carry an
/// identity, so an anonymous one is a config error, not a default.
fn parse_tokens(text: &str, where_: &str) -> Result<Vec<TokenEntry>, String> {
    let v: Value = serde_json::from_str(text).map_err(|e| format!("{where_}: bad json: {e}"))?;
    let arr = v["tokens"]
        .as_array()
        .ok_or(format!("{where_}: expected {{\"tokens\": [...]}}"))?;
    let mut out = Vec::with_capacity(arr.len());
    for (i, t) in arr.iter().enumerate() {
        let name = t["name"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(format!("{where_}: tokens[{i}].name missing"))?;
        let token = t["token"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(format!("{where_}: tokens[{i}].token missing"))?;
        out.push(TokenEntry {
            name: name.to_string(),
            token: token.to_string(),
            labels: t.get("labels").and_then(|x| x.as_u64()),
            read_only: t["read_only"].as_bool().unwrap_or(false),
        });
    }
    Ok(out)
}

/// The token entry the request's `Authorization: Bearer <token>` matches, if any. Every candidate
/// is compared in constant time; no configured tokens = bearer auth stays opt-in.
fn bearer_entry<'a>(auth: &'a Auth, req: &Request) -> Option<&'a TokenEntry> {
    let presented = header_value(req, "authorization").and_then(|h| {
        h.strip_prefix("Bearer ")
            .or_else(|| h.strip_prefix("bearer "))
    })?;
    let presented = presented.trim();
    auth.tokens
        .iter()
        .fold(None, |hit, e| match ct_eq(presented, &e.token) {
            true => hit.or(Some(e)),
            false => hit,
        })
}

/// True iff the request carries a live (unexpired) session cookie. Expired tokens are purged.
fn authed(sessions: &Sessions, req: &Request) -> bool {
    let Some(tok) = cookie_token(req) else {
        return false;
    };
    let mut s = sessions.lock().unwrap_or_else(|e| e.into_inner());
    match s.get(&tok).copied() {
        Some(exp) if exp > now_secs() => true,
        Some(_) => {
            s.remove(&tok);
            false
        }
        None => false,
    }
}

/// Resolve a setting: `--flag <v>` overrides `$ENV` overrides `default`.
fn opt(args: &[String], name: &str, env: &str, default: &str) -> String {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
        .or_else(|| std::env::var(env).ok())
        .unwrap_or_else(|| default.into())
}

const UI_HTML: &str = include_str!("ui.html");

/// The bundled demo dataset (`--demo`): a small org graph sized so every headline read fires on the
/// first screen — three department transfers (multi-segment timelines), a manager change, releases
/// whose approvals include a self-approval and a stale approval plus one missing sign-off (mixed
/// conformance verdicts), names corroborated by zero/one/two sources (confidence tiers), and six
/// docs with pre-computed 8-d embeddings (offline vector search).
pub mod demo {
    /// Schema + nodes + facts + the stored `release-approval` rule (JSONL ingest lines).
    pub const GRAPH_JSONL: &str = include_str!("../data/demo.jsonl");
    /// Pre-computed document embeddings (JSONL embed lines) — no external model needed.
    pub const EMBED_JSONL: &str = include_str!("../data/demo-embed.jsonl");
}

/// The namespace an unprefixed path addresses: the `--db` directory itself.
const DEFAULT_NS: &str = "default";

/// A namespace name: `[a-z0-9_-]{1,64}` — safe as one path component on every platform, and
/// never `.`/`..`.
fn valid_ns_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

/// Split a request path into (namespace, inner path): `/ns/<name>/<rest>` addresses `<name>` with
/// `/<rest>` as the path (`/ns/<name>` alone is its console, `/`); any other path addresses
/// `default` unchanged. A malformed name is an error (the caller answers 400).
fn route(path: &str) -> Result<(&str, &str), String> {
    let Some(rest) = path.strip_prefix("/ns/") else {
        return Ok((DEFAULT_NS, path));
    };
    let (name, inner) = match rest.find('/') {
        Some(i) => rest.split_at(i),
        None => (rest, "/"),
    };
    if !valid_ns_name(name) {
        return Err(format!(
            "bad namespace name '{name}': expected [a-z0-9_-]{{1,64}}"
        ));
    }
    Ok((name, inner))
}

/// One namespace's open slot: empty until the first request that finds its directory opens it.
type Slot = Arc<Mutex<Option<SharedDb>>>;

/// The databases one server fronts (D27). The `--db` directory is the `default` namespace; each
/// named namespace is an ordinary database directory at `<db>/ns/<name>/`, opened lazily with the
/// same backlog bound and cached for the life of the process. The engine stays one database per
/// directory — isolation is by directory, so a namespace can also be opened offline by pointing
/// `--db` at it (while this server is not holding it).
struct Namespaces {
    root: std::path::PathBuf,
    n_max: usize,
    default: SharedDb,
    /// name → slot. The map lock is held only to find or insert a slot; opening runs under the
    /// slot's own lock, so a slow first open never stalls requests to other namespaces, and two
    /// concurrent first requests for one name open it exactly once.
    open: Mutex<HashMap<String, Slot>>,
}

impl Namespaces {
    fn new(root: &std::path::Path, n_max: usize, default: SharedDb) -> Namespaces {
        Namespaces {
            root: root.to_path_buf(),
            n_max,
            default,
            open: Mutex::new(HashMap::new()),
        }
    }

    fn dir(&self, name: &str) -> std::path::PathBuf {
        self.root.join("ns").join(name)
    }

    /// The database for a (validated) namespace name. `create` (a write) initializes a missing
    /// one; otherwise a missing namespace is `Ok(None)` and leaves nothing behind — neither a
    /// directory nor a cache entry.
    fn get(&self, name: &str, create: bool) -> Result<Option<SharedDb>, String> {
        if name == DEFAULT_NS {
            return Ok(Some(self.default.clone()));
        }
        let dir = self.dir(name);
        let slot = {
            let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
            match open.get(name) {
                Some(slot) => slot.clone(),
                None if !create && !dir.join("wal.log").exists() => return Ok(None),
                None => open.entry(name.to_string()).or_default().clone(),
            }
        };
        let mut slot = slot.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(db) = slot.as_ref() {
            return Ok(Some(db.clone()));
        }
        let db = match create {
            true => Db::open_or_init_with(&dir, self.n_max),
            false => Db::open_with(&dir, self.n_max),
        }
        .map_err(|e| format!("namespace '{name}': {e}"))?;
        let db = Arc::new(db);
        *slot = Some(db.clone());
        Ok(Some(db))
    }

    /// `default` first, then every database directory under `<db>/ns/`, sorted.
    fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(self.root.join("ns"))
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|n| n != DEFAULT_NS && valid_ns_name(n) && self.dir(n).join("wal.log").exists())
            .collect();
        names.sort();
        names.insert(0, DEFAULT_NS.to_string());
        names
    }

    /// An already-open namespace's database, without opening it or waiting on a first open that
    /// is in flight.
    fn loaded(&self, name: &str) -> Option<SharedDb> {
        if name == DEFAULT_NS {
            return Some(self.default.clone());
        }
        let slot = self
            .open
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)?
            .clone();
        let slot = slot.try_lock().ok()?;
        slot.clone()
    }

    /// Every namespace with its node and fact counts, for the console's namespace selector:
    /// `[{"name", "nodes", "facts", "loaded"}, ...]`, `default` first then sorted. Never opens a
    /// namespace: an open one reports its live counters (O(1), lock-free), an unopened one the
    /// counts its directory last persisted (`counts.json`, rewritten after every write), or
    /// `null` counts when it has none yet.
    fn list_with_counts(&self) -> Vec<Value> {
        self.list()
            .into_iter()
            .map(|name| {
                let db = self.loaded(&name);
                let counts = match &db {
                    Some(db) => Some(db.counts()),
                    None => stromadb_store::persisted_counts(&self.dir(&name)),
                };
                json!({
                    "name": name,
                    "nodes": counts.map(|c| c.nodes),
                    "facts": counts.map(|c| c.facts),
                    "loaded": db.is_some(),
                })
            })
            .collect()
    }
}

fn json_response(status: u16, body: &Value) -> Response<std::io::Cursor<Vec<u8>>> {
    let ct = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    Response::from_string(body.to_string())
        .with_status_code(status)
        .with_header(ct)
}

fn html_response() -> Response<std::io::Cursor<Vec<u8>>> {
    let ct = Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..]).unwrap();
    // send with a Content-Length rather than chunked, so the page arrives as one clean body
    // (chunk framing can otherwise split a multi-byte UTF-8 char across boundaries for naive readers)
    Response::from_string(UI_HTML)
        .with_header(ct)
        .with_chunked_threshold(usize::MAX)
}

fn login_response() -> Response<std::io::Cursor<Vec<u8>>> {
    let ct = Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..]).unwrap();
    Response::from_string(LOGIN_HTML)
        .with_header(ct)
        .with_chunked_threshold(usize::MAX)
}

fn json_cookie_response(
    status: u16,
    body: &Value,
    set_cookie: &str,
) -> Response<std::io::Cursor<Vec<u8>>> {
    let ct = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let sc = Header::from_bytes(&b"Set-Cookie"[..], set_cookie.as_bytes()).unwrap();
    Response::from_string(body.to_string())
        .with_status_code(status)
        .with_header(ct)
        .with_header(sc)
}

fn read_body(req: &mut Request) -> String {
    let mut s = String::new();
    let _ = std::io::Read::read_to_string(req.as_reader(), &mut s);
    s
}

/// What a namespaced endpoint answers: a JSON body, the console page, or an empty 202 (an MCP
/// notification). The worker turns it into the wire response.
enum Reply {
    Json(u16, Value),
    Page,
    Accepted,
}

/// Serve one authenticated request against the namespace its path addresses: parse the
/// `/ns/<name>` prefix (400 on a bad name), resolve that namespace's database (created only by a
/// `POST /ingest`; anything else on a missing one is 404), then dispatch the inner path to it.
fn dispatch(
    nss: &Namespaces,
    allow_reset: bool,
    req: &mut Request,
    path: &str,
    scope: &mcp::Scope,
) -> Reply {
    let (name, inner) = match route(path) {
        Ok(r) => r,
        Err(e) => return Reply::Json(400, json!({ "error": e })),
    };
    // a read-only token's ingest is refused (403), so it must not create the namespace either
    let create = *req.method() == Method::Post && inner == "/ingest" && !scope.read_only;
    match nss.get(name, create) {
        Ok(Some(db)) => handle(&db, req, inner, scope, allow_reset),
        Ok(None) => Reply::Json(
            404,
            json!({ "error": format!("namespace '{name}' does not exist (it is created by its first /ingest)") }),
        ),
        Err(e) => Reply::Json(500, json!({ "error": e })),
    }
}

fn handle(
    db: &SharedDb,
    req: &mut Request,
    path: &str,
    scope: &mcp::Scope,
    allow_reset: bool,
) -> Reply {
    let method = req.method().clone();
    let read_only_err = || {
        Reply::Json(
            403,
            json!({ "error": "this token is read-only: writes are not allowed" }),
        )
    };
    match (&method, path) {
        (Method::Get, "/" | "/ui") => Reply::Page,
        // reads: lock-free over a pinned read view (query internally pins the current Arc<ReadState>).
        (Method::Get, "/stats") => Reply::Json(200, db.stats()),
        (Method::Post, "/query") => {
            let body = read_body(req);
            match serde_json::from_str::<Value>(&body) {
                Ok(mut v) => {
                    // a token's label cap bounds what this client can ever see — the request's own
                    // allowed_labels is intersected, never widened
                    scope.cap_labels(&mut v);
                    match db.query(&v) {
                        Ok(r) => Reply::Json(200, r),
                        Err(e) => Reply::Json(400, json!({ "error": e })),
                    }
                }
                Err(e) => Reply::Json(400, json!({ "error": format!("bad json: {e}") })),
            }
        }
        // writes: serialize on the database's internal write mutex, then publish a fresh read
        // view. A named token's writes carry its name as the default provenance.
        (Method::Post, "/ingest") => {
            if scope.read_only {
                return read_only_err();
            }
            let body = read_body(req);
            match db.ingest_str_as(&body, scope.default_source.as_deref()) {
                Ok(s) => Reply::Json(
                    200,
                    json!({ "defs": s.defs, "nodes": s.nodes, "facts": s.facts, "retracts": s.retracts, "closes": s.closes, "suppressed": s.suppressed, "durable_head": s.durable_head }),
                ),
                Err(e) => Reply::Json(400, json!({ "error": e })),
            }
        }
        (Method::Post, "/embed") => {
            if scope.read_only {
                return read_only_err();
            }
            let body = read_body(req);
            match db.embed_str(&body) {
                Ok(n) => Reply::Json(200, json!({ "embedded": n })),
                Err(e) => Reply::Json(400, json!({ "error": e })),
            }
        }
        // Snapshot + truncate the changelog: non-destructive (as-of reads keep answering across
        // the boundary), so unlike /reset it needs no opt-in flag — but it is an explicit admin
        // action, never triggered automatically (and not for read-only tokens).
        (Method::Post, "/compact") => {
            if scope.read_only {
                return read_only_err();
            }
            match db.compact() {
                Ok(s) => Reply::Json(
                    200,
                    json!({ "compacted_upto": s.covered, "wal_bytes": s.wal_bytes, "snapshot_bytes": s.snapshot_bytes }),
                ),
                Err(e) => Reply::Json(400, json!({ "error": e })),
            }
        }
        // opt-in, destructive: clear the addressed database. Off unless --allow-reset is set.
        (Method::Post, "/reset") => {
            if scope.read_only {
                Reply::Json(
                    403,
                    json!({ "error": "this token is read-only: reset is not allowed" }),
                )
            } else if !allow_reset {
                Reply::Json(403, json!({ "error": RESET_DISABLED }))
            } else {
                match db.reset() {
                    Ok(()) => Reply::Json(200, json!({ "ok": true })),
                    Err(e) => Reply::Json(500, json!({ "error": e })),
                }
            }
        }
        (Method::Get, "/events") => {
            // long-poll: block until the durable head advances past `since` (or ~20s), so the
            // console can re-query its current slice the moment the database changes.
            let since = req
                .url()
                .split("since=")
                .nth(1)
                .and_then(|s| s.split('&').next())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let mut head = db.durable_head();
            let mut waited = 0u32;
            while head == since && waited < 20_000 {
                std::thread::sleep(std::time::Duration::from_millis(250));
                waited += 250;
                head = db.durable_head();
            }
            Reply::Json(200, json!({ "head": head }))
        }
        // MCP streamable HTTP transport, stateless: one JSON-RPC message per POST, no session
        // ids, no server-initiated stream. Same auth as the other endpoints (the worker's gate).
        // Reads run lock-free on a pinned view; a `tools/call ingest` serializes on the database's
        // internal write mutex exactly like POST /ingest.
        (Method::Post, "/mcp") => {
            let body = read_body(req);
            match serde_json::from_str::<Value>(&body) {
                // a request (has an id) → its JSON-RPC response, under the caller's scope
                Ok(msg) => match mcp::handle_message_scoped(db, &msg, scope) {
                    Some(resp) => Reply::Json(200, resp),
                    // a notification → accepted, empty body
                    None => Reply::Accepted,
                },
                Err(e) => Reply::Json(
                    400,
                    mcp::rpc_error(&Value::Null, -32700, &format!("parse error: {e}")),
                ),
            }
        }
        (_, "/mcp") => Reply::Json(
            405,
            json!({ "error": "method not allowed: POST one JSON-RPC message to /mcp" }),
        ),
        _ => Reply::Json(404, json!({ "error": "not found" })),
    }
}

/// Flags that take a value (`--flag <value>`), matching `docs/CONFIGURATION.md`.
const VALUE_FLAGS: &[&str] = &[
    "--db",
    "--addr",
    "--max-unmerged",
    "--admin-user",
    "--admin-password",
    "--api-token",
    "--tokens",
    "--app-url",
];

/// Flags that take no value.
const BOOL_FLAGS: &[&str] = &["--demo", "--allow-reset", "--no-auth", "-h", "--help"];

/// `serve` / `up` usage: every flag `run` understands, one line each, matching
/// `docs/CONFIGURATION.md`.
fn usage() -> &'static str {
    "usage: stroma serve|up [options]  (same flags for the stroma-serve binary)\n\
     \n\
     options:\n\
     \x20 --db <dir>              database directory (default: .; `up` defaults to ./stroma-db)\n\
     \x20 --addr <host:port>      HTTP bind address (default: 127.0.0.1:7687)\n\
     \x20 --max-unmerged <n>      backpressure threshold for un-merged writes (default: 8000000)\n\
     \x20 --admin-user <name>     console login username (default: admin)\n\
     \x20 --admin-password <pw>   console login password (default: password)\n\
     \x20 --api-token <token>     legacy single unnamed, unrestricted bearer token\n\
     \x20 --tokens <file>         named token registry (JSON)\n\
     \x20 --app-url <url>         optional link back to the app this database feeds; shown in the\n\
     \x20                         console topbar as \"Back to app\" (hidden when unset)\n\
     \x20 --demo                  boot with the bundled sample org graph\n\
     \x20 --allow-reset           enable POST /reset, which clears the database\n\
     \x20 --no-auth               disable the auth gate (local dev only)\n\
     \x20 -h, --help              print this help message\n"
}

/// The first argument that looks like a flag (starts with `-`) but is not one `run` recognizes,
/// skipping over each known value-flag's value so it is never misread as a stray flag.
fn unknown_flag(args: &[String]) -> Option<&str> {
    let mut i = 0;
    while i < args.len() {
        let a = args[i].as_str();
        if a.starts_with('-') {
            if VALUE_FLAGS.contains(&a) {
                i += 2;
                continue;
            }
            if !BOOL_FLAGS.contains(&a) {
                return Some(a);
            }
        }
        i += 1;
    }
    None
}

/// Run the HTTP server with CLI-style `args` (everything after the program/subcommand name).
/// Blocks for the life of the server; exits the process on a fatal startup error (bad dir, bind
/// failure). Called by the `stroma-serve` binary and by the `stroma serve` / `stroma up`
/// subcommands, so one install carries the whole application.
///
/// `-h`/`--help` and any unrecognized flag are handled first, before anything with a side effect
/// (opening/creating the database directory, binding the socket): help prints usage and exits 0,
/// an unknown flag prints an error plus usage and exits 2.
pub fn run(args: &[String]) {
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print!("{}", usage());
        exit(0);
    }
    if let Some(bad) = unknown_flag(args) {
        eprintln!("error: unknown flag {bad}");
        eprint!("{}", usage());
        exit(2);
    }
    let demo = args.iter().any(|a| a == "--demo");
    // --demo with no explicit location gets its own directory under the OS temp dir, so trying
    // the demo never litters the working directory; an explicit --db / $STROMA_DB still wins.
    let dir = if demo && !args.iter().any(|a| a == "--db") && std::env::var("STROMA_DB").is_err() {
        std::env::temp_dir()
            .join("stroma-demo")
            .to_string_lossy()
            .into_owned()
    } else {
        opt(args, "--db", "STROMA_DB", ".")
    };
    let addr = opt(args, "--addr", "STROMA_ADDR", "127.0.0.1:7687");
    let n_max: usize = opt(args, "--max-unmerged", "STROMA_MAX_UNMERGED", "")
        .parse()
        .unwrap_or(stromadb_store::DEFAULT_N_MAX);
    // The token registry: named tokens from --tokens/$STROMA_TOKENS (per-client identity, label
    // cap, read-only), plus the legacy single --api-token as an unnamed unrestricted entry.
    // --demo mints a named token when nothing is configured, so the printed MCP snippet works out
    // of the box without disabling the auth gate — and the demo agent's writes carry provenance.
    let mut tokens: Vec<TokenEntry> = Vec::new();
    let tokens_path = opt(args, "--tokens", "STROMA_TOKENS", "");
    if !tokens_path.is_empty() {
        let text = std::fs::read_to_string(&tokens_path).unwrap_or_else(|e| {
            eprintln!("error: read {tokens_path}: {e}");
            exit(1);
        });
        tokens = parse_tokens(&text, &tokens_path).unwrap_or_else(|e| {
            eprintln!("error: {e}");
            exit(1);
        });
    }
    let legacy = opt(args, "--api-token", "STROMA_API_TOKEN", "");
    if !legacy.is_empty() {
        tokens.push(TokenEntry {
            name: String::new(),
            token: legacy,
            labels: None,
            read_only: false,
        });
    }
    if demo
        && tokens.is_empty()
        && let Some(t) = new_token()
    {
        tokens.push(TokenEntry {
            name: "demo-agent".into(),
            token: t,
            labels: None,
            read_only: false,
        });
    }
    let auth = Arc::new(Auth {
        user: opt(args, "--admin-user", "STROMA_ADMIN_USER", "admin"),
        pass: opt(
            args,
            "--admin-password",
            "STROMA_ADMIN_PASSWORD",
            "password",
        ),
        tokens,
        allow_reset: args.iter().any(|a| a == "--allow-reset")
            || std::env::var("STROMA_ALLOW_RESET").is_ok_and(|v| v == "1" || v == "true"),
        no_auth: args.iter().any(|a| a == "--no-auth")
            || std::env::var("STROMA_NO_AUTH").is_ok_and(|v| v == "1" || v == "true"),
    });
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));

    // open_or_init: a fresh directory (e.g. an empty Docker volume) is created on first run.
    let db: SharedDb = match Db::open_or_init_with(std::path::Path::new(&dir), n_max) {
        Ok(db) => Arc::new(db),
        Err(e) => {
            eprintln!("error: {e}");
            exit(1);
        }
    };
    // Seed the sample graph exactly once: only an empty database is written to, so restarting
    // --demo (or pointing it at real data by mistake) never duplicates or disturbs anything.
    if demo && db.durable_head() == 0 {
        if let Err(e) = db.ingest_str(demo::GRAPH_JSONL) {
            eprintln!("error: demo ingest: {e}");
            exit(1);
        }
        if let Err(e) = db.embed_str(demo::EMBED_JSONL) {
            eprintln!("error: demo embeddings: {e}");
            exit(1);
        }
    }
    let server = match Server::http(&addr) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("error: bind {addr}: {e}");
            exit(1);
        }
    };
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(2, 32);
    let bound = server
        .server_addr()
        .to_ip()
        .map_or_else(|| addr.clone(), |a| a.to_string());
    let app_url = opt(args, "--app-url", "STROMA_APP_URL", "");
    let info = Arc::new(ServerInfo {
        db_path: std::fs::canonicalize(&dir)
            .map_or_else(|_| dir.clone(), |p| p.to_string_lossy().into_owned()),
        workers,
        mcp_url: format!("http://{bound}/mcp"),
        app_url: (!app_url.is_empty()).then_some(app_url),
    });
    eprintln!("stromadb serving on http://{addr}  (db: {dir}, {workers} workers)");
    eprintln!("console: open http://{addr}/ in a browser");
    if demo {
        eprintln!();
        eprintln!(
            "demo: sample org graph loaded — 6 people, 3 departments (with transfers), 5 issues, 6 docs"
        );
        // never echo a custom password to the log — only the well-known default
        if auth.pass == "password" {
            eprintln!("  console login: {} / password", auth.user);
        }
        eprintln!("  try these in the console's Query tab:");
        eprintln!("    1. a property value — node 1, predicate member-of, as of 2024-09-01");
        eprintln!("       (Alice's department back then; blank = where she is now)");
        eprintln!("    2. a value over time — node 1, hops: member-of, manager-of");
        eprintln!("       (who Alice's manager was, over time — three intervals)");
        eprintln!("    3. rule verdicts — stored rule: release-approval");
        eprintln!("       (one OK, a self-approval, a stale approval, and a missing sign-off)");
        if let Some(t) = auth.tokens.iter().find(|t| t.name == "demo-agent") {
            eprintln!("  connect an agent to the same live graph (MCP over HTTP):");
            eprintln!(
                "    claude mcp add stroma --transport http http://{addr}/mcp --header \"Authorization: Bearer {}\"",
                t.token
            );
            eprintln!("    (writes made through this token carry the provenance \"demo-agent\")");
        }
        eprintln!();
    }
    if auth.no_auth {
        eprintln!(
            "WARNING: auth gate DISABLED (--no-auth / $STROMA_NO_AUTH) — local dev only, never expose this server."
        );
    } else if auth.pass == "password" {
        eprintln!(
            "WARNING: default console password in use — set --admin-password / $STROMA_ADMIN_PASSWORD before exposing this server."
        );
    }

    let nss = Arc::new(Namespaces::new(std::path::Path::new(&dir), n_max, db));
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (nss, server, auth, sessions, info) = (
            nss.clone(),
            server.clone(),
            auth.clone(),
            sessions.clone(),
            info.clone(),
        );
        handles.push(std::thread::spawn(move || {
            while let Ok(mut req) = server.recv() {
                let method = req.method().clone();
                let path = req.url().split('?').next().unwrap_or("").to_string();

                // public: container health probe, login page, login attempt
                if method == Method::Get && path == "/health" {
                    let _ = req.respond(json_response(200, &json!({ "status": "ok" })));
                    continue;
                }
                if method == Method::Get && path == "/login" {
                    let _ = req.respond(login_response());
                    continue;
                }
                if method == Method::Post && path == "/login" {
                    let body = read_body(&mut req);
                    let v: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
                    let ok = ct_eq(v["user"].as_str().unwrap_or(""), &auth.user)
                        && ct_eq(v["password"].as_str().unwrap_or(""), &auth.pass);
                    if ok {
                        // fail closed: no OS entropy → no session, never a predictable token
                        let Some(tok) = new_token() else {
                            let _ = req.respond(json_response(
                                500,
                                &json!({ "error": "no OS entropy available to mint a session token" }),
                            ));
                            continue;
                        };
                        sessions
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .insert(tok.clone(), now_secs() + SESSION_TTL_SECS);
                        let cookie = format!(
                            "stroma_session={tok}; HttpOnly; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}"
                        );
                        let _ = req.respond(json_cookie_response(200, &json!({ "ok": true }), &cookie));
                    } else {
                        let _ =
                            req.respond(json_response(401, &json!({ "error": "invalid credentials" })));
                    }
                    continue;
                }

                // everything else needs a live session (browser) or a registered API token
                // (programmatic), unless the auth gate is disabled for local dev (--no-auth).
                // A session (or --no-auth) is unrestricted; a named token carries its own scope
                // — provenance stamping, a label cap on reads, an optional read-only bit.
                // `via` names how the caller got in (reported by /me): the open gate, a console
                // session, or a registered token (whose name rides along; the legacy token has none).
                let (scope, via, token_name): (mcp::Scope, &str, Option<&str>) = if auth.no_auth {
                    (mcp::Scope::default(), "open", None)
                } else if authed(&sessions, &req) {
                    (mcp::Scope::default(), "session", None)
                } else if let Some(entry) = bearer_entry(&auth, &req) {
                    let name = (!entry.name.is_empty()).then_some(entry.name.as_str());
                    (entry.scope(), "token", name)
                } else {
                    // browser → login page, also for a namespace's console (`/ns/<name>/`)
                    let page = route(&path).is_ok_and(|(_, p)| p == "/" || p == "/ui");
                    if method == Method::Get && page {
                        let _ = req.respond(login_response());
                    } else {
                        let _ = req.respond(json_response(401, &json!({ "error": "unauthorized" })));
                    }
                    continue;
                };

                if method == Method::Post && path == "/logout" {
                    if let Some(tok) = cookie_token(&req) {
                        sessions.lock().unwrap_or_else(|e| e.into_inner()).remove(&tok);
                    }
                    let clear = "stroma_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0";
                    let _ = req.respond(json_cookie_response(200, &json!({ "ok": true }), clear));
                } else if method == Method::Get && path == "/me" {
                    // what the caller may do, so the console renders admin actions without probing,
                    // plus the static server facts its settings panel shows
                    let _ = req.respond(json_response(
                        200,
                        &json!({
                            "user": auth.user,
                            "read_only": scope.read_only,
                            "allow_reset": auth.allow_reset,
                            "reset_hint": (!auth.allow_reset).then_some(RESET_DISABLED),
                            "auth": via,
                            "token_name": token_name,
                            "labels": scope.allowed_labels,
                            "version": env!("CARGO_PKG_VERSION"),
                            "db_path": info.db_path,
                            "workers": info.workers,
                            "mcp_url": info.mcp_url,
                            "app_url": info.app_url,
                        }),
                    ));
                } else if method == Method::Get && path == "/namespaces" {
                    // global like /me: every authenticated caller (session or token) sees every
                    // namespace with its node/fact counts — namespace access is not currently
                    // restricted per-token, so there is nothing narrower to show a non-admin caller.
                    let _ = req.respond(json_response(
                        200,
                        &json!({ "namespaces": nss.list_with_counts() }),
                    ));
                } else {
                    match dispatch(&nss, auth.allow_reset, &mut req, &path, &scope) {
                        Reply::Json(status, body) => {
                            let _ = req.respond(json_response(status, &body));
                        }
                        Reply::Page => {
                            let _ = req.respond(html_response());
                        }
                        Reply::Accepted => {
                            let _ = req.respond(Response::empty(202));
                        }
                    }
                }
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
}

#[cfg(test)]
mod tests {
    use super::{Namespaces, Reply, dispatch, new_token, parse_tokens, route, valid_ns_name};
    use std::sync::Arc;
    use stromadb_store::{DEFAULT_N_MAX, Db, mcp};
    use tiny_http::{Method, TestRequest};

    #[test]
    fn namespace_names_and_routing() {
        assert!(valid_ns_name("ocel"));
        assert!(valid_ns_name("a-b_9"));
        assert!(valid_ns_name(&"x".repeat(64)));
        for bad in ["", "Ocel", "a.b", "..", "a/b", "é", "Bad!"] {
            assert!(!valid_ns_name(bad), "{bad:?} must be rejected");
        }
        assert!(!valid_ns_name(&"x".repeat(65)));

        assert_eq!(route("/query"), Ok(("default", "/query")));
        assert_eq!(route("/"), Ok(("default", "/")));
        assert_eq!(route("/ns/a/query"), Ok(("a", "/query")));
        assert_eq!(route("/ns/a/"), Ok(("a", "/")));
        assert_eq!(route("/ns/a"), Ok(("a", "/")));
        assert_eq!(route("/ns/default/stats"), Ok(("default", "/stats")));
        // only a leading /ns/ is a prefix; a nested one is just the inner path
        assert_eq!(route("/ns/a/ns/b/stats"), Ok(("a", "/ns/b/stats")));
        assert_eq!(route("/nsx/stats"), Ok(("default", "/nsx/stats")));
        assert!(route("/ns/Bad!/stats").is_err());
        assert!(route("/ns//stats").is_err());
        assert!(route("/ns/").is_err());
    }

    /// Drive one request through the namespace dispatcher (unrestricted scope).
    fn call(nss: &Namespaces, method: Method, path: &str, body: &'static str) -> (u16, String) {
        let mut req = TestRequest::new()
            .with_method(method)
            .with_path(path)
            .with_body(body)
            .into();
        match dispatch(nss, true, &mut req, path, &mcp::Scope::default()) {
            Reply::Json(status, v) => (status, v.to_string()),
            Reply::Page => (200, "<page>".into()),
            Reply::Accepted => (202, String::new()),
        }
    }

    fn open_root(root: &std::path::Path) -> Namespaces {
        let db = Db::open_or_init_with(root, DEFAULT_N_MAX).unwrap();
        Namespaces::new(root, DEFAULT_N_MAX, Arc::new(db))
    }

    // Writes to /ns/a are invisible from default and from /ns/b; unknown namespaces 404 without
    // being created; bad names 400; a fresh server state over the same root finds `a` again.
    #[test]
    fn namespaces_isolate_create_on_write_and_reopen() {
        let root = std::env::temp_dir().join(format!("stroma_ns_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let graph = concat!(
            "{\"type_def\":{\"name\":\"Person\"}}\n",
            "{\"pred_def\":{\"name\":\"knows\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
            "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
            "{\"node\":{\"id\":2,\"type\":\"Person\"}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":2}}}\n",
        );
        let expand = r#"{"op":"expand","subject":1,"predicate":"knows"}"#;
        {
            let nss = open_root(&root);
            // reads, the console and non-ingest writes on a missing namespace are 404 and leave
            // no trace
            for (m, p) in [
                (Method::Get, "/ns/a/stats"),
                (Method::Get, "/ns/a/"),
                (Method::Post, "/ns/a/embed"),
                (Method::Post, "/ns/a/compact"),
                (Method::Post, "/ns/a/reset"),
                (Method::Post, "/ns/a/mcp"),
            ] {
                assert_eq!(call(&nss, m, p, "").0, 404, "{p}");
            }
            assert!(!root.join("ns").exists());
            assert_eq!(call(&nss, Method::Get, "/ns/Bad!/stats", "").0, 400);

            let (st, body) = call(&nss, Method::Post, "/ns/a/ingest", graph);
            assert_eq!(st, 200, "{body}");
            assert!(root.join("ns/a/wal.log").exists());
            let (st, body) = call(&nss, Method::Post, "/ns/a/query", expand);
            assert_eq!(st, 200, "{body}");
            assert!(body.contains("[2]"), "a sees its fact: {body}");

            // default (both spellings) and a sibling namespace see nothing of it
            for p in ["/query", "/ns/default/query"] {
                let (_, body) = call(&nss, Method::Post, p, expand);
                assert!(!body.contains("[2]"), "{p} leaked: {body}");
            }
            call(
                &nss,
                Method::Post,
                "/ns/b/ingest",
                "{\"type_def\":{\"name\":\"Other\"}}\n",
            );
            let (_, body) = call(&nss, Method::Post, "/ns/b/query", expand);
            assert!(!body.contains("[2]"), "b leaked: {body}");
            assert_eq!(call(&nss, Method::Get, "/ns/nope/stats", "").0, 404);
            // a global endpoint is not reachable under a namespace
            assert_eq!(call(&nss, Method::Get, "/ns/a/me", "").0, 404);
            assert_eq!(call(&nss, Method::Get, "/ns/a/", "").0, 200);
            assert_eq!(nss.list(), ["default", "a", "b"]);

            // resetting default clears only default: the ns/ subtree is not its file
            assert_eq!(call(&nss, Method::Post, "/reset", "").0, 200);
            let (_, body) = call(&nss, Method::Post, "/ns/a/query", expand);
            assert!(body.contains("[2]"), "default reset hit a: {body}");
        }
        // a new server state over the same root: the parent db opens undisturbed by ns/, and
        // `a` reopens lazily with its data
        let nss = open_root(&root);
        assert_eq!(nss.list(), ["default", "a", "b"]);
        let (st, body) = call(&nss, Method::Post, "/ns/a/query", expand);
        assert_eq!(st, 200, "{body}");
        assert!(body.contains("[2]"), "a after reopen: {body}");
        drop(nss);
        let _ = std::fs::remove_dir_all(&root);
    }

    // list_with_counts: default first, each entry carries its node/fact counts, and an empty
    // default with one populated namespace still lists both.
    #[test]
    fn namespace_counts() {
        let root =
            std::env::temp_dir().join(format!("stroma_ns_counts_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let nss = open_root(&root);
        let graph = concat!(
            "{\"type_def\":{\"name\":\"Person\"}}\n",
            "{\"pred_def\":{\"name\":\"knows\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
            "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
            "{\"node\":{\"id\":2,\"type\":\"Person\"}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":2}}}\n",
        );
        call(&nss, Method::Post, "/ns/a/ingest", graph);

        let counts = nss.list_with_counts();
        assert_eq!(counts.len(), 2);
        assert_eq!(counts[0]["name"], "default");
        assert_eq!(counts[0]["nodes"], 0);
        assert_eq!(counts[0]["facts"], 0);
        assert_eq!(counts[1]["name"], "a");
        assert_eq!(counts[1]["nodes"], 2);
        // durable_head counts durable ops (each node's type assignment plus the one relation),
        // not just explicit `fact` lines — same counter /stats reports as "facts.durable_head".
        assert_eq!(counts[1]["facts"], 3);
        assert_eq!(counts[1]["loaded"], true);

        // after a restart, listing must not open "a": its counts come from what it persisted
        // on its last write, and it stays out of the open cache
        drop(nss);
        let nss = open_root(&root);
        let counts = nss.list_with_counts();
        assert_eq!(counts[1]["name"], "a");
        assert_eq!(counts[1]["nodes"], 2);
        assert_eq!(counts[1]["facts"], 3);
        assert_eq!(counts[1]["loaded"], false);
        assert!(nss.open.lock().unwrap().is_empty());

        // no persisted counts yet (e.g. written by an older version): names still list, counts null
        std::fs::remove_file(root.join("ns").join("a").join("counts.json")).unwrap();
        let counts = nss.list_with_counts();
        assert_eq!(counts[1]["name"], "a");
        assert!(counts[1]["nodes"].is_null());
        assert_eq!(counts[1]["loaded"], false);

        drop(nss);
        let _ = std::fs::remove_dir_all(&root);
    }

    // Concurrent first requests for one namespace open it once (a second open of the same
    // directory would fail on its LOCK) and all get the same database.
    #[test]
    fn concurrent_first_open_opens_once() {
        let root = std::env::temp_dir().join(format!("stroma_ns_race_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let nss = Arc::new(open_root(&root));
        let dbs: Vec<_> = (0..8)
            .map(|_| {
                let nss = nss.clone();
                std::thread::spawn(move || nss.get("r", true).unwrap().unwrap())
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();
        assert!(dbs.iter().all(|d| Arc::ptr_eq(d, &dbs[0])));
        drop(dbs);
        drop(nss);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn token_registry_parses_and_fails_loudly() {
        let ts = parse_tokens(
            r#"{"tokens":[{"name":"support","token":"s3","labels":15,"read_only":true},
                          {"name":"analytics","token":"a7"}]}"#,
            "test",
        )
        .unwrap();
        assert_eq!(ts.len(), 2);
        assert_eq!(ts[0].labels, Some(15));
        assert!(ts[0].read_only);
        assert_eq!(ts[1].labels, None);
        assert!(!ts[1].read_only);
        let scope = ts[0].scope();
        assert_eq!(scope.default_source.as_deref(), Some("support"));
        assert_eq!(scope.allowed_labels, Some(15));

        // an anonymous or secret-less entry is a config error, named by index
        assert!(
            parse_tokens(r#"{"tokens":[{"token":"x"}]}"#, "f")
                .unwrap_err()
                .contains("tokens[0].name")
        );
        assert!(
            parse_tokens(r#"{"tokens":[{"name":"a"}]}"#, "f")
                .unwrap_err()
                .contains("tokens[0].token")
        );
        assert!(parse_tokens(r#"[]"#, "f").unwrap_err().contains("tokens"));
    }

    // Two minted tokens are present, distinct, and never the all-zero fallback the old
    // /dev/urandom path could silently produce on platforms without that device.
    #[test]
    fn session_tokens_are_random_and_nonzero() {
        let a = new_token().expect("OS CSPRNG available");
        let b = new_token().expect("OS CSPRNG available");
        assert_eq!(a.len(), 48);
        assert_ne!(a, b);
        assert_ne!(a, "0".repeat(48));
    }
}
