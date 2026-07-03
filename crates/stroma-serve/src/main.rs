//! `stroma-serve` — a minimal HTTP surface over a directory-backed StromaDB, so an agent (or any
//! client) can query and ingest over the network instead of embedding the engine.
//!
//! Endpoints (JSON):
//!   GET  /health          → {"status":"ok"}
//!   GET  /stats           → engine/schema/embedding/storage counters
//!   POST /query   {op,...} → point / expand / search (see stroma_db::Db::query)
//!   POST /ingest  <jsonl> → {defs,nodes,facts,retracts,durable_head}
//!   POST /embed   <jsonl> → {embedded: N}
//!
//! Concurrency: a worker pool shares the database behind an `RwLock` — reads (`/query`, `/stats`,
//! `/health`) run concurrently; writes (`/ingest`, `/embed`) take the write lock and are exclusive
//! (reads briefly wait for the duration of a write). Fully lock-free reads *during* a write (over a
//! pinned snapshot) are a follow-up. Addresses #25.
//!
//! Config: `--db <dir>` / `$STROMA_DB` (default `.`), `--addr <host:port>` / `$STROMA_ADDR`
//! (default `127.0.0.1:7687`), `--max-unmerged` / `$STROMA_MAX_UNMERGED`. A flag overrides the env
//! var overrides the default.

use std::process::exit;
use std::sync::{Arc, RwLock};

use serde_json::{Value, json};
use stroma_db::Db;
use tiny_http::{Header, Method, Request, Response, Server};

type SharedDb = Arc<RwLock<Db>>;

/// Resolve a setting: `--flag <v>` overrides `$ENV` overrides `default`.
fn opt(args: &[String], name: &str, env: &str, default: &str) -> String {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
        .or_else(|| std::env::var(env).ok())
        .unwrap_or_else(|| default.into())
}

fn json_response(status: u16, body: &Value) -> Response<std::io::Cursor<Vec<u8>>> {
    let ct = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    Response::from_string(body.to_string())
        .with_status_code(status)
        .with_header(ct)
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
        // reads: shared lock (concurrent). recover from a poisoned lock rather than crash the pool.
        (Method::Get, "/stats") => (200, db.read().unwrap_or_else(|e| e.into_inner()).stats()),
        (Method::Post, "/query") => {
            let body = read_body(req);
            match serde_json::from_str::<Value>(&body) {
                Ok(v) => match db.read().unwrap_or_else(|e| e.into_inner()).query(&v) {
                    Ok(r) => (200, r),
                    Err(e) => (400, json!({ "error": e })),
                },
                Err(e) => (400, json!({ "error": format!("bad json: {e}") })),
            }
        }
        // writes: exclusive lock.
        (Method::Post, "/ingest") => {
            let body = read_body(req);
            match db
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .ingest_str(&body)
            {
                Ok(s) => (
                    200,
                    json!({ "defs": s.defs, "nodes": s.nodes, "facts": s.facts, "retracts": s.retracts, "durable_head": s.durable_head }),
                ),
                Err(e) => (400, json!({ "error": e })),
            }
        }
        (Method::Post, "/embed") => {
            let body = read_body(req);
            match db
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .embed_str(&body)
            {
                Ok(n) => (200, json!({ "embedded": n })),
                Err(e) => (400, json!({ "error": e })),
            }
        }
        _ => (404, json!({ "error": "not found" })),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let dir = opt(&args, "--db", "STROMA_DB", ".");
    let addr = opt(&args, "--addr", "STROMA_ADDR", "127.0.0.1:7687");
    let n_max: usize = opt(&args, "--max-unmerged", "STROMA_MAX_UNMERGED", "")
        .parse()
        .unwrap_or(stroma_db::DEFAULT_N_MAX);

    // open_or_init: a fresh directory (e.g. an empty Docker volume) is created on first run.
    let db: SharedDb = match Db::open_or_init_with(std::path::Path::new(&dir), n_max) {
        Ok(db) => Arc::new(RwLock::new(db)),
        Err(e) => {
            eprintln!("error: {e}");
            exit(1);
        }
    };
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
    eprintln!("stroma-serve: http://{addr}  (db: {dir}, {workers} workers)");

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (db, server) = (db.clone(), server.clone());
        handles.push(std::thread::spawn(move || {
            while let Ok(mut req) = server.recv() {
                let (status, body) = handle(&db, &mut req);
                let _ = req.respond(json_response(status, &body));
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
}
