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
//! v1 is **single-threaded**: one writer, requests handled sequentially (documented — the engine is
//! single-threaded pre-1.0; the Arc-shared snapshot makes concurrent reads a later, additive step).
//!
//! Config: `--db <dir>` / `$STROMA_DB` (default `.`), `--addr <host:port>` / `$STROMA_ADDR`
//! (default `127.0.0.1:7687`). A flag overrides the env var overrides the default.

use std::process::exit;

use serde_json::{Value, json};
use stroma_db::Db;
use tiny_http::{Header, Method, Request, Response, Server};

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

fn handle(db: &mut Db, req: &mut Request) -> (u16, Value) {
    let url = req.url().to_string();
    let path = url.split('?').next().unwrap_or("");
    match (req.method(), path) {
        (Method::Get, "/health") => (200, json!({ "status": "ok" })),
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
        (Method::Post, "/ingest") => {
            let body = read_body(req);
            match db.ingest_str(&body) {
                Ok(s) => (
                    200,
                    json!({ "defs": s.defs, "nodes": s.nodes, "facts": s.facts, "retracts": s.retracts, "durable_head": s.durable_head }),
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
    let mut db = match Db::open_or_init_with(std::path::Path::new(&dir), n_max) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("error: {e}");
            exit(1);
        }
    };
    let server = match Server::http(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: bind {addr}: {e}");
            exit(1);
        }
    };
    eprintln!("stroma-serve: http://{addr}  (db: {dir}, single-threaded)");

    for mut req in server.incoming_requests() {
        let (status, body) = handle(&mut db, &mut req);
        let _ = req.respond(json_response(status, &body));
    }
}
