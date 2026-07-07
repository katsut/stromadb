//! `stroma` — the StromaDB CLI: init / ingest / embed / query / stats / serve. A thin frontend over
//! the `stroma-db` directory-backed database (which owns the on-disk layout and query dispatch).

use std::path::Path;
use std::process::exit;

use serde_json::{Value, json};
use stroma_db::Db;

fn die(msg: &str) -> ! {
    eprintln!("error: {msg}");
    exit(1)
}

fn parse_flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn read_file(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| die(&format!("read {path}: {e}")))
}

fn cmd_query(dir: &Path, args: &[String]) {
    let db = Db::open(dir).unwrap_or_else(|e| die(&e));
    let sub = args.first().map(|s| s.as_str()).unwrap_or("");
    let req: Value = match sub {
        "point" | "expand" => {
            let subject: u64 = args
                .get(1)
                .and_then(|a| a.parse().ok())
                .unwrap_or_else(|| die(&format!("usage: query {sub} <subject> <predicate>")));
            let predicate = args
                .get(2)
                .unwrap_or_else(|| die(&format!("usage: query {sub} <subject> <predicate>")));
            json!({ "op": sub, "subject": subject, "predicate": predicate })
        }
        "search" => {
            let ty = parse_flag(args, "--type")
                .unwrap_or_else(|| die("search requires --type <TypeName>"));
            let vec_file = parse_flag(args, "--vector-file")
                .unwrap_or_else(|| die("search requires --vector-file <json array>"));
            let vector: Value = serde_json::from_str(&read_file(&vec_file))
                .unwrap_or_else(|e| die(&format!("vector json: {e}")));
            let mut req = json!({ "op": "search", "type": ty, "vector": vector });
            if let Some(k) = parse_flag(args, "--k").and_then(|s| s.parse::<u64>().ok()) {
                req["k"] = json!(k);
            }
            if let Some(m) =
                parse_flag(args, "--allowed-labels").and_then(|s| s.parse::<u64>().ok())
            {
                req["allowed_labels"] = json!(m);
            }
            if let Some(mode) = parse_flag(args, "--mode") {
                req["mode"] = json!(mode);
            }
            if let Some(p) = parse_flag(args, "--expand") {
                req["expand"] = json!(p);
            }
            req
        }
        _ => die("usage: stroma query <point|expand|search> ..."),
    };
    match db.query(&req) {
        Ok(v) => println!("{v}"),
        Err(e) => die(&e),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let usage = "usage: stroma <init|ingest|embed|query|stats|serve> --db <dir> [...]";
    let cmd = args
        .first()
        .map(|s| s.as_str())
        .unwrap_or_else(|| die(usage));
    let db_dir = parse_flag(&args, "--db").unwrap_or_else(|| ".".into());
    let dir = Path::new(&db_dir);
    let rest: Vec<String> = args
        .iter()
        .skip(1)
        .filter(|a| *a != "--db" && **a != db_dir)
        .cloned()
        .collect();
    match cmd {
        "init" => {
            Db::init(dir).unwrap_or_else(|e| die(&e));
            println!("initialized stroma database at {}", dir.display());
        }
        "ingest" => {
            let file = rest
                .first()
                .unwrap_or_else(|| die("usage: stroma ingest <file.jsonl> --db <dir>"));
            let db = Db::open(dir).unwrap_or_else(|e| die(&e));
            let s = db.ingest_str(&read_file(file)).unwrap_or_else(|e| die(&e));
            println!(
                "ingested: {} defs, {} nodes, {} facts, {} retracts (durable_head={})",
                s.defs, s.nodes, s.facts, s.retracts, s.durable_head
            );
        }
        "embed" => {
            let file = rest
                .first()
                .unwrap_or_else(|| die("usage: stroma embed <file.jsonl> --db <dir>"));
            let db = Db::open(dir).unwrap_or_else(|e| die(&e));
            let n = db.embed_str(&read_file(file)).unwrap_or_else(|e| die(&e));
            println!("embedded: {n} vectors");
        }
        "query" => cmd_query(dir, &rest),
        "stats" => {
            let db = Db::open(dir).unwrap_or_else(|e| die(&e));
            println!("{}", serde_json::to_string_pretty(&db.stats()).unwrap());
        }
        "serve" => die("run the `stroma-serve` binary for the HTTP surface (this CLI is offline)"),
        _ => die(usage),
    }
}
