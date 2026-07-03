//! `stroma-mcp` — a Model Context Protocol server exposing a StromaDB database as tools an LLM agent
//! can call directly. Transport: newline-delimited JSON-RPC 2.0 over stdio (the MCP stdio transport).
//!
//! Tools: `point`, `expand`, `search` (authz-scoped hybrid), `stats`, `ingest`. Read tools map to
//! `stroma_db::Db::query`; `ingest` writes facts. Requests are handled sequentially (single writer).
//!
//! Usage: stroma-mcp --db <dir>   (spoken to by an MCP client over stdin/stdout)

use std::io::{BufRead, Write};

use serde_json::{Value, json};
use stroma_db::Db;

const PROTOCOL_VERSION: &str = "2024-11-05";

fn tools() -> Value {
    json!([
        {
            "name": "point",
            "description": "Look up the value(s) of a (subject, predicate) fact. Returns {one:..} for cardinality-one predicates or {many:[..]} for cardinality-many.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "subject": { "type": "integer", "description": "subject node id" },
                    "predicate": { "type": "string", "description": "predicate name" }
                },
                "required": ["subject", "predicate"]
            }
        },
        {
            "name": "expand",
            "description": "1-hop expand: node ids reachable from a subject via a predicate.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "subject": { "type": "integer" },
                    "predicate": { "type": "string" }
                },
                "required": ["subject", "predicate"]
            }
        },
        {
            "name": "search",
            "description": "Type-aware hybrid search: k nearest nodes of a type to a query vector, authz-scoped, optionally 1-hop expanded. Returns ids + scores + as_of.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "type": { "type": "string", "description": "target node type name" },
                    "vector": { "type": "array", "items": { "type": "number" }, "description": "query embedding" },
                    "k": { "type": "integer", "default": 10 },
                    "allowed_labels": { "type": "integer", "description": "caller ABAC label bitmask (default: all)" },
                    "expand": { "type": "string", "description": "optional predicate to 1-hop expand results" },
                    "mode": { "type": "string", "enum": ["fresh", "strict"], "default": "fresh" }
                },
                "required": ["type", "vector"]
            }
        },
        {
            "name": "retrieve_context",
            "description": "Assemble LLM-ready context from a hybrid search: each hit's current value of a `content` predicate with a calendar-framed timestamp of its `date` predicate (weekday, days relative to `as_of`, business hours), ordered oldest→newest. Returns a ready-to-inject context block + structured hits.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "type": { "type": "string", "description": "target node type name" },
                    "vector": { "type": "array", "items": { "type": "number" }, "description": "query embedding" },
                    "content": { "type": "string", "description": "predicate whose text value is the excerpt" },
                    "date": { "type": "string", "description": "predicate whose Int value (epoch seconds) is the valid-time to stamp" },
                    "k": { "type": "integer", "default": 10 },
                    "allowed_labels": { "type": "integer", "description": "caller ABAC label bitmask (default: all)" },
                    "as_of": { "type": "integer", "description": "reference instant (epoch seconds) for relative-day stamping; default = newest hit" },
                    "tz_offset_min": { "type": "integer", "description": "calendar frame: minutes offset from UTC (default 0)" }
                },
                "required": ["type", "vector", "content"]
            }
        },
        {
            "name": "stats",
            "description": "Database counters: durable head, schema/embedding counts, storage bytes.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "ingest",
            "description": "Ingest a JSONL batch (type_def / pred_def / node / fact / retract records, one per line). Durable on return.",
            "inputSchema": {
                "type": "object",
                "properties": { "jsonl": { "type": "string", "description": "newline-delimited records" } },
                "required": ["jsonl"]
            }
        }
    ])
}

fn call_tool(db: &mut Db, name: &str, args: &Value) -> Result<Value, String> {
    match name {
        "point" | "expand" | "search" | "retrieve_context" => {
            let mut req = args.clone();
            req["op"] = json!(name);
            db.query(&req)
        }
        "stats" => Ok(db.stats()),
        "ingest" => {
            let jsonl = args["jsonl"]
                .as_str()
                .ok_or("ingest requires a `jsonl` string")?;
            let s = db.ingest_str(jsonl)?;
            Ok(
                json!({ "defs": s.defs, "nodes": s.nodes, "facts": s.facts, "retracts": s.retracts, "durable_head": s.durable_head }),
            )
        }
        other => Err(format!("unknown tool: {other}")),
    }
}

/// JSON-RPC error object.
fn rpc_error(id: &Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn rpc_result(id: &Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// Handle one request; returns Some(response) for requests, None for notifications.
fn handle(db: &mut Db, msg: &Value) -> Option<Value> {
    let method = msg["method"].as_str().unwrap_or("");
    // Notifications have no id and expect no response (`?` returns None here).
    let id = msg.get("id").cloned()?;
    let params = msg.get("params").cloned().unwrap_or(json!({}));

    let resp = match method {
        "initialize" => rpc_result(
            &id,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "stroma-mcp", "version": env!("CARGO_PKG_VERSION") }
            }),
        ),
        "ping" => rpc_result(&id, json!({})),
        "tools/list" => rpc_result(&id, json!({ "tools": tools() })),
        "tools/call" => {
            let name = params["name"].as_str().unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            match call_tool(db, name, &args) {
                Ok(v) => rpc_result(
                    &id,
                    json!({ "content": [{ "type": "text", "text": v.to_string() }] }),
                ),
                // Tool-level failures are reported in the result (isError), not as protocol errors.
                Err(e) => rpc_result(
                    &id,
                    json!({ "content": [{ "type": "text", "text": format!("error: {e}") }], "isError": true }),
                ),
            }
        }
        other => rpc_error(&id, -32601, &format!("method not found: {other}")),
    };
    Some(resp)
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

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let dir = opt(&args, "--db", "STROMA_DB", ".");
    let n_max: usize = opt(&args, "--max-unmerged", "STROMA_MAX_UNMERGED", "")
        .parse()
        .unwrap_or(stroma_db::DEFAULT_N_MAX);
    let mut db = match Db::open_with(std::path::Path::new(&dir), n_max) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };
    eprintln!("stroma-mcp: serving db {dir} over stdio (MCP {PROTOCOL_VERSION})");

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) if !l.trim().is_empty() => l,
            Ok(_) => continue,
            Err(_) => break,
        };
        let msg: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                let err = rpc_error(&Value::Null, -32700, &format!("parse error: {e}"));
                let _ = writeln!(stdout, "{err}");
                let _ = stdout.flush();
                continue;
            }
        };
        if let Some(resp) = handle(&mut db, &msg) {
            if writeln!(stdout, "{resp}").is_err() {
                break;
            }
            let _ = stdout.flush();
        }
    }
}
