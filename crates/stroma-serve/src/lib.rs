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
//!   GET  /me              → {"user": name}
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
//!   POST /reset           → clears the whole database (opt-in: only when started with --allow-reset)
//!
//! Auth: every endpoint except `/health` and the login page/POST requires either a valid session
//! cookie (issued by `POST /login`, in-memory, 12h) or, for programmatic clients, the API token as
//! `Authorization: Bearer <token>`. Credentials are `--admin-user`/`$STROMA_ADMIN_USER` (default
//! `admin`) and `--admin-password`/`$STROMA_ADMIN_PASSWORD` (default `password`, warned). The API
//! token is `--api-token`/`$STROMA_API_TOKEN` (unset = bearer auth disabled, cookie-only).
//!
//! Concurrency: a worker pool shares the database as a plain `Arc<Db>`. Reads (`/query`) are
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
use tiny_http::{Header, Method, Request, Response, Server};

type SharedDb = Arc<Db>;

/// Console credentials (flag/env, default `admin`/`password`) plus an optional API token for
/// programmatic clients. An empty `api_token` disables bearer auth (cookie-only, as before).
struct Auth {
    user: String,
    pass: String,
    api_token: String,
    /// Opt-in: allow `POST /reset` to clear the whole database (dev/demo). Off by default.
    allow_reset: bool,
    /// Opt-in: disable the auth gate entirely (local dev only). Off by default.
    no_auth: bool,
}

/// Active session tokens → unix-seconds expiry (in-memory; cleared on restart).
type Sessions = Arc<Mutex<HashMap<String, u64>>>;
const SESSION_TTL_SECS: u64 = 12 * 3600;

const LOGIN_HTML: &str = include_str!("login.html");

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

/// True iff the request presents the configured API token as `Authorization: Bearer <token>`.
/// Always false when no token is configured, so bearer auth is opt-in. Constant-time compare.
fn bearer_authed(auth: &Auth, req: &Request) -> bool {
    if auth.api_token.is_empty() {
        return false;
    }
    header_value(req, "authorization")
        .and_then(|h| {
            h.strip_prefix("Bearer ")
                .or_else(|| h.strip_prefix("bearer "))
        })
        .is_some_and(|tok| ct_eq(tok.trim(), &auth.api_token))
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

fn handle(db: &SharedDb, req: &mut Request) -> (u16, Value) {
    let url = req.url().to_string();
    let path = url.split('?').next().unwrap_or("");
    match (req.method(), path) {
        (Method::Get, "/health") => (200, json!({ "status": "ok" })),
        // reads: lock-free over a pinned read view (query internally pins the current Arc<ReadState>).
        (Method::Get, "/stats") => (200, db.stats()),
        (Method::Post, "/query") => {
            let body = read_body(req);
            match serde_json::from_str::<Value>(&body) {
                Ok(v) => match db.query(&v) {
                    Ok(r) => (200, r),
                    Err(e) => (400, json!({ "error": e })),
                },
                Err(e) => (400, json!({ "error": format!("bad json: {e}") })),
            }
        }
        // writes: serialize on the database's internal write mutex, then publish a fresh read view.
        (Method::Post, "/ingest") => {
            let body = read_body(req);
            match db.ingest_str(&body) {
                Ok(s) => (
                    200,
                    json!({ "defs": s.defs, "nodes": s.nodes, "facts": s.facts, "retracts": s.retracts, "closes": s.closes, "suppressed": s.suppressed, "durable_head": s.durable_head }),
                ),
                Err(e) => (400, json!({ "error": e })),
            }
        }
        (Method::Post, "/embed") => {
            let body = read_body(req);
            match db.embed_str(&body) {
                Ok(n) => (200, json!({ "embedded": n })),
                Err(e) => (400, json!({ "error": e })),
            }
        }
        // Snapshot + truncate the changelog: non-destructive (as-of reads keep answering across
        // the boundary), so unlike /reset it needs no opt-in flag — but it is an explicit admin
        // action, never triggered automatically.
        (Method::Post, "/compact") => match db.compact() {
            Ok(s) => (
                200,
                json!({ "compacted_upto": s.covered, "wal_bytes": s.wal_bytes, "snapshot_bytes": s.snapshot_bytes }),
            ),
            Err(e) => (400, json!({ "error": e })),
        },
        _ => (404, json!({ "error": "not found" })),
    }
}

/// Run the HTTP server with CLI-style `args` (everything after the program/subcommand name).
/// Blocks for the life of the server; exits the process on a fatal startup error (bad dir, bind
/// failure). Called by the `stroma-serve` binary and by the `stroma serve` / `stroma up`
/// subcommands, so one install carries the whole application.
pub fn run(args: &[String]) {
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
    // --demo mints a bearer token when none is configured, so the printed MCP snippet works
    // out of the box without disabling the auth gate.
    let api_token = {
        let configured = opt(args, "--api-token", "STROMA_API_TOKEN", "");
        if demo && configured.is_empty() {
            new_token().unwrap_or_default()
        } else {
            configured
        }
    };
    let auth = Arc::new(Auth {
        user: opt(args, "--admin-user", "STROMA_ADMIN_USER", "admin"),
        pass: opt(
            args,
            "--admin-password",
            "STROMA_ADMIN_PASSWORD",
            "password",
        ),
        api_token,
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
        if !auth.api_token.is_empty() {
            eprintln!("  connect an agent to the same live graph (MCP over HTTP):");
            eprintln!(
                "    claude mcp add stroma --transport http http://{addr}/mcp --header \"Authorization: Bearer {}\"",
                auth.api_token
            );
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

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (db, server, auth, sessions) =
            (db.clone(), server.clone(), auth.clone(), sessions.clone());
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

                // everything else needs a live session (browser) or the API token (programmatic),
                // unless the auth gate is disabled for local dev (--no-auth / $STROMA_NO_AUTH)
                if !auth.no_auth && !authed(&sessions, &req) && !bearer_authed(&auth, &req) {
                    if method == Method::Get && (path == "/" || path == "/ui") {
                        let _ = req.respond(login_response()); // browser → login page
                    } else {
                        let _ = req.respond(json_response(401, &json!({ "error": "unauthorized" })));
                    }
                    continue;
                }

                if method == Method::Post && path == "/logout" {
                    if let Some(tok) = cookie_token(&req) {
                        sessions.lock().unwrap_or_else(|e| e.into_inner()).remove(&tok);
                    }
                    let clear = "stroma_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0";
                    let _ = req.respond(json_cookie_response(200, &json!({ "ok": true }), clear));
                } else if method == Method::Get && path == "/me" {
                    let _ = req.respond(json_response(200, &json!({ "user": auth.user })));
                } else if method == Method::Post && path == "/reset" {
                    // opt-in, destructive: clear the whole database. Off unless --allow-reset is set.
                    if !auth.allow_reset {
                        let _ = req.respond(json_response(
                            403,
                            &json!({ "error": "reset is disabled (start with --allow-reset to enable)" }),
                        ));
                    } else {
                        let r = db.reset();
                        match r {
                            Ok(()) => {
                                let _ = req.respond(json_response(200, &json!({ "ok": true })));
                            }
                            Err(e) => {
                                let _ = req.respond(json_response(500, &json!({ "error": e })));
                            }
                        }
                    }
                } else if method == Method::Get && path == "/events" {
                    // long-poll: block until the durable head advances past `since` (or ~20s), so the
                    // console can re-query its current slice the moment the database changes.
                    let since = req
                        .url()
                        .split("since=")
                        .nth(1)
                        .and_then(|s| s.split('&').next())
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(0);
                    let head_now = || db.durable_head();
                    let mut head = head_now();
                    let mut waited = 0u32;
                    while head == since && waited < 20_000 {
                        std::thread::sleep(std::time::Duration::from_millis(250));
                        waited += 250;
                        head = head_now();
                    }
                    let _ = req.respond(json_response(200, &json!({ "head": head })));
                } else if path == "/mcp" {
                    // MCP streamable HTTP transport, stateless: one JSON-RPC message per POST,
                    // no session ids, no server-initiated stream. Same auth as the other endpoints
                    // (the gate above). Reads run lock-free on a pinned view; a `tools/call ingest`
                    // serializes on the database's internal write mutex exactly like POST /ingest.
                    if method == Method::Post {
                        let body = read_body(&mut req);
                        match serde_json::from_str::<Value>(&body) {
                            // a request (has an id) → its JSON-RPC response
                            Ok(msg) => match stromadb_store::mcp::handle_message(&db, &msg) {
                                Some(resp) => {
                                    let _ = req.respond(json_response(200, &resp));
                                }
                                // a notification → accepted, empty body
                                None => {
                                    let _ = req.respond(Response::empty(202));
                                }
                            },
                            Err(e) => {
                                let _ = req.respond(json_response(
                                    400,
                                    &stromadb_store::mcp::rpc_error(
                                        &Value::Null,
                                        -32700,
                                        &format!("parse error: {e}"),
                                    ),
                                ));
                            }
                        }
                    } else {
                        let _ = req.respond(json_response(
                            405,
                            &json!({ "error": "method not allowed: POST one JSON-RPC message to /mcp" }),
                        ));
                    }
                } else if method == Method::Get && (path == "/" || path == "/ui") {
                    let _ = req.respond(html_response());
                } else {
                    let (status, body) = handle(&db, &mut req);
                    let _ = req.respond(json_response(status, &body));
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
    use super::new_token;

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
