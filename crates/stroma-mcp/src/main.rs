//! `stroma-mcp` — a Model Context Protocol server exposing a StromaDB database as tools an LLM agent
//! can call directly. Transport: newline-delimited JSON-RPC 2.0 over stdio (the MCP stdio transport).
//!
//! The tool schemas and JSON-RPC dispatch live in `stroma_db::mcp` (shared with the `stroma-serve`
//! `POST /mcp` endpoint); this binary is only the stdio framing around it. Requests are handled
//! sequentially (single writer).
//!
//! Usage: stroma-mcp --db <dir>   (spoken to by an MCP client over stdin/stdout)

use std::io::{BufRead, Write};

use serde_json::Value;
use stroma_db::Db;
use stroma_db::mcp::{PROTOCOL_VERSION, handle_message, rpc_error};

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
    let db = match Db::open_with(std::path::Path::new(&dir), n_max) {
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
        if let Some(resp) = handle_message(&db, &msg) {
            if writeln!(stdout, "{resp}").is_err() {
                break;
            }
            let _ = stdout.flush();
        }
    }
}
