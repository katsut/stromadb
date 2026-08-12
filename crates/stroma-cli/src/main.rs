//! `stroma` — the StromaDB CLI: init / ingest / embed / query / stats / serve / up. A thin frontend
//! over the `stromadb-store` directory-backed database (which owns the on-disk layout and query
//! dispatch); `serve` and `up` run the full HTTP surface in-process (`stromadb-serve` as a library),
//! so `cargo install stromadb` alone yields the whole application.

use std::path::Path;
use std::process::exit;

use serde_json::{Value, json};
use stromadb_store::Db;

mod import;

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
    let usage = "usage: stroma <init|ingest|import|embed|query|stats|serve|up> --db <dir> [...]";
    let cmd = args
        .first()
        .map(|s| s.as_str())
        .unwrap_or_else(|| die(usage));
    // `serve` / `up` hand the raw flags to the serving library (it does its own flag/env parsing,
    // e.g. --addr, --api-token). `up` is the just-run-it verb: same server, but a fresh directory
    // defaults to ./stroma-db instead of littering the current directory with db files (`--demo`
    // is left alone — the server gives the demo its own directory under the OS temp dir).
    if cmd == "serve" || cmd == "up" {
        let mut serve_args: Vec<String> = args[1..].to_vec();
        if cmd == "up"
            && !serve_args.iter().any(|a| a == "--db" || a == "--demo")
            && std::env::var("STROMA_DB").is_err()
        {
            serve_args.extend(["--db".into(), "./stroma-db".into()]);
        }
        stromadb_serve::run(&serve_args);
        return;
    }
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
                "ingested: {} defs, {} nodes, {} facts, {} retracts, {} closes, {} suppressed (durable_head={})",
                s.defs, s.nodes, s.facts, s.retracts, s.closes, s.suppressed, s.durable_head
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
        // CSV → graph, the mechanical mapping: `stroma import people.csv --db ./db --type Person
        // --id id [--valid-from hired] [--valid-to left] [--edge dept:Department:member-of]...
        // [--skip col]... [--source hr]`. Unmapped columns import as literal predicates.
        "import" => {
            let file = rest
                .first()
                .filter(|f| !f.starts_with("--"))
                .unwrap_or_else(|| {
                    die("usage: stroma import <file.csv> --db <dir> --type <Type> --id <col> [...]")
                });
            let bytes = std::fs::read(file).unwrap_or_else(|e| die(&format!("read {file}: {e}")));
            let text = String::from_utf8(bytes).unwrap_or_else(|_| {
                die(&format!(
                    "{file} is not UTF-8 — convert it first (e.g. `iconv -f SHIFT_JIS -t UTF-8`)"
                ))
            });
            let (headers, rows) =
                import::parse_csv(&text).unwrap_or_else(|e| die(&format!("{file}: {e}")));
            let node_type =
                parse_flag(&args, "--type").unwrap_or_else(|| die("import requires --type <Type>"));
            let id_col =
                parse_flag(&args, "--id").unwrap_or_else(|| die("import requires --id <column>"));
            let vf = parse_flag(&args, "--valid-from");
            let vt = parse_flag(&args, "--valid-to");
            // repeatable flags: every occurrence of --edge / --skip
            let all_flags = |name: &str| -> Vec<String> {
                args.iter()
                    .enumerate()
                    .filter(|(_, a)| *a == name)
                    .filter_map(|(i, _)| args.get(i + 1).cloned())
                    .collect()
            };
            let skips = all_flags("--skip");
            let mut edges: Vec<(String, String, String)> = Vec::new();
            for e in all_flags("--edge") {
                let parts: Vec<&str> = e.split(':').collect();
                let [col, ty, pred] = parts[..] else {
                    die(&format!(
                        "--edge must be <column>:<TargetType>:<predicate>, got {e:?}"
                    ));
                };
                edges.push((col.into(), ty.into(), pred.into()));
            }
            let roles = headers
                .iter()
                .map(|h| {
                    let role = if *h == id_col {
                        import::Role::Id
                    } else if vf.as_deref() == Some(h) {
                        import::Role::ValidFrom
                    } else if vt.as_deref() == Some(h) {
                        import::Role::ValidTo
                    } else if let Some((_, ty, pred)) = edges.iter().find(|(c, _, _)| c == h) {
                        import::Role::Edge {
                            target_type: ty.clone(),
                            predicate: pred.clone(),
                        }
                    } else if skips.contains(h) {
                        import::Role::Skip
                    } else {
                        import::Role::Literal
                    };
                    (h.clone(), role)
                })
                .collect();
            for wanted in [Some(&id_col), vf.as_ref(), vt.as_ref()]
                .into_iter()
                .flatten()
            {
                if !headers.contains(wanted) {
                    die(&format!(
                        "column {wanted:?} is not in the header: {headers:?}"
                    ));
                }
            }
            let mapping = import::Mapping {
                node_type,
                roles,
                source: parse_flag(&args, "--source"),
            };
            let jsonl = import::compile(&mapping, &headers, &rows).unwrap_or_else(|e| die(&e));
            let db = Db::open(dir).unwrap_or_else(|e| die(&e));
            let s = db.ingest_str(&jsonl).unwrap_or_else(|e| die(&e));
            println!(
                "imported {} rows: {} defs, {} nodes, {} facts, {} suppressed (durable_head={})",
                rows.len(),
                s.defs,
                s.nodes,
                s.facts,
                s.suppressed,
                s.durable_head
            );
        }
        "query" => cmd_query(dir, &rest),
        "stats" => {
            let db = Db::open(dir).unwrap_or_else(|e| die(&e));
            println!("{}", serde_json::to_string_pretty(&db.stats()).unwrap());
        }
        _ => die(usage),
    }
}
