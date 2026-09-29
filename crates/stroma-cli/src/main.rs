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

/// The first argument that looks like a flag (starts with `-`, other than `-h`/`--help`) but is
/// not in `value_flags`, skipping over each known flag's value so it is never misread as a stray
/// flag itself.
fn unknown_flag<'a>(args: &'a [String], value_flags: &[&str]) -> Option<&'a str> {
    let mut i = 0;
    while i < args.len() {
        let a = args[i].as_str();
        if a.starts_with('-') && a != "-h" && a != "--help" {
            if value_flags.contains(&a) {
                i += 2;
                continue;
            }
            return Some(a);
        }
        i += 1;
    }
    None
}

/// `-h`/`--help` prints `usage` and exits 0; an unrecognized flag (not in `value_flags`) prints
/// an error plus `usage` and exits 2 — both before the subcommand does anything with a side
/// effect (opening/creating a database directory, reading a file, etc).
fn check_flags(args: &[String], value_flags: &[&str], usage: &str) {
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print!("{usage}");
        exit(0);
    }
    if let Some(bad) = unknown_flag(args, value_flags) {
        eprintln!("error: unknown flag {bad}");
        eprint!("{usage}");
        exit(2);
    }
}

const INIT_USAGE: &str = "usage: stroma init --db <dir>\n\
     \n\
     options:\n\
     \x20 --db <dir>   database directory to create (default: .)\n\
     \x20 -h, --help   print this help message\n";

const INGEST_USAGE: &str = "usage: stroma ingest <file.jsonl> --db <dir>\n\
     \n\
     options:\n\
     \x20 --db <dir>   database directory (default: .)\n\
     \x20 -h, --help   print this help message\n";

const EMBED_USAGE: &str = "usage: stroma embed <file.jsonl> --db <dir>\n\
     \n\
     options:\n\
     \x20 --db <dir>   database directory (default: .)\n\
     \x20 -h, --help   print this help message\n";

const IMPORT_USAGE: &str = "usage: stroma import <file.csv> --db <dir> --type <Type> --id <col> [options]\n\
     \n\
     options:\n\
     \x20 --db <dir>            database directory (default: .)\n\
     \x20 --type <Type>         node type to create for each row (required)\n\
     \x20 --id <col>            column holding each row's node id (required)\n\
     \x20 --valid-from <col>    column holding a fact's valid-from timestamp\n\
     \x20 --valid-to <col>      column holding a fact's valid-to timestamp\n\
     \x20 --edge <col:Type:pred> repeatable: map a column to an edge (target type + predicate)\n\
     \x20 --skip <col>          repeatable: column to import unchanged\n\
     \x20 --source <name>       provenance name stamped on imported facts\n\
     \x20 -h, --help            print this help message\n";

const QUERY_USAGE: &str = "usage: stroma query <point|expand|search> ... --db <dir>\n\
     \n\
     options:\n\
     \x20 --db <dir>               database directory (default: .)\n\
     \x20 --type <TypeName>        (search) node type to search within\n\
     \x20 --vector-file <file>     (search) JSON array file with the query vector\n\
     \x20 --k <n>                  (search) number of results\n\
     \x20 --allowed-labels <mask>  (search) ABAC label mask\n\
     \x20 --mode <mode>            (search) search mode\n\
     \x20 --expand <predicate>     (search) predicate to expand results through\n\
     \x20 -h, --help               print this help message\n";

const STATS_USAGE: &str = "usage: stroma stats --db <dir>\n\
     \n\
     options:\n\
     \x20 --db <dir>   database directory (default: .)\n\
     \x20 -h, --help   print this help message\n";

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
    let usage = "usage: stroma <command> --db <dir> [...]\n\
         \n\
         commands:\n\
         \x20 init     create a database directory\n\
         \x20 ingest   ingest a JSONL file of defs/nodes/facts\n\
         \x20 import   import a CSV file as a mapped graph\n\
         \x20 embed    ingest a JSONL file of vector embeddings\n\
         \x20 query    run a point/expand/search query\n\
         \x20 stats    print database counters\n\
         \x20 serve    run the HTTP server\n\
         \x20 up       run the HTTP server (creates ./stroma-db if no --db is given)\n\
         \n\
         Run `stroma <command> --help` for a command's own flags.\n";
    if args.first().is_some_and(|a| a == "-h" || a == "--help") {
        print!("{usage}");
        exit(0);
    }
    let cmd = args
        .first()
        .map(|s| s.as_str())
        .unwrap_or_else(|| die(usage));
    // `serve` / `up` hand the raw flags to the serving library (it does its own flag/env parsing,
    // e.g. --addr, --api-token, and its own --help/unknown-flag handling). `up` is the
    // just-run-it verb: same server, but a fresh directory defaults to ./stroma-db instead of
    // littering the current directory with db files (`--demo` is left alone — the server gives
    // the demo its own directory under the OS temp dir).
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
    let sub_args = &args[1..];
    match cmd {
        "init" => check_flags(sub_args, &["--db"], INIT_USAGE),
        "ingest" => check_flags(sub_args, &["--db"], INGEST_USAGE),
        "embed" => check_flags(sub_args, &["--db"], EMBED_USAGE),
        "import" => check_flags(
            sub_args,
            &[
                "--db",
                "--type",
                "--id",
                "--valid-from",
                "--valid-to",
                "--edge",
                "--skip",
                "--source",
            ],
            IMPORT_USAGE,
        ),
        "query" => check_flags(
            sub_args,
            &[
                "--db",
                "--type",
                "--vector-file",
                "--k",
                "--allowed-labels",
                "--mode",
                "--expand",
            ],
            QUERY_USAGE,
        ),
        "stats" => check_flags(sub_args, &["--db"], STATS_USAGE),
        _ => {}
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
