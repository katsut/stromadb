//! `stroma` — the StromaDB CLI: init / ingest / embed / query / stats / serve.
//!
//! Database directory layout (authoritative inputs only; derived stores rebuild on open, per the
//! DR design):
//!   wal.log          append-only changelog (facts; crash-sound, group-commit fsync)
//!   schema.jsonl     type/predicate definitions, replayed in order (Field-ID interning is
//!                    order-deterministic, so ids are stable across restarts)
//!   nodes.jsonl      node type/label assignments, replayed
//!   embeddings.bin   received embeddings, flat f32 LE; embeddings.ids = u64 LE per row
//!   meta.json        { "dim": N }
//!
//! Ingest format: JSONL, one record per line —
//!   {"type_def":{"name":"Person"}}
//!   {"pred_def":{"name":"works-on","cardinality":"many","domain":"Person","range":"Project"}}
//!   {"pred_def":{"name":"age","cardinality":"one","domain":"Person","range_value":"int"}}
//!   {"node":{"id":1,"type":"Person","label":0}}
//!   {"fact":{"subject":1,"predicate":"works-on","object":{"node":2}}}
//!   {"fact":{"subject":1,"predicate":"age","object":{"int":34},"valid_from":1690000000}}
//!   {"retract":{"subject":1,"predicate":"works-on","object":{"node":2}}}
//! Embeddings: {"node":1,"vector":[...]} via `stroma embed`.

use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::exit;

use stroma_core::catalog::{Cardinality, Catalog, Range, RelProps, ValueType};
use stroma_core::changelog::WriteKind;
use stroma_core::engine::Engine;
use stroma_core::fold::ObjKey;
use stroma_core::ir::{Pipeline, Principal, Source, Transform, run};
use stroma_core::ivf::IvfPq;
use stroma_core::query;
use stroma_core::version::{ReadMode, VersionVector};

fn die(msg: &str) -> ! {
    eprintln!("error: {msg}");
    exit(1)
}

struct Db {
    dir: PathBuf,
    eng: Engine,
    cat: Catalog,
    cardinality: HashMap<String, Cardinality>, // predicate name -> cardinality (CLI-side registry)
    emb_ids: Vec<u64>,
    emb: Vec<f32>,
    dim: usize,
}

impl Db {
    fn open(dir: &Path) -> Db {
        if !dir.join("wal.log").exists() {
            die(&format!(
                "{} is not a stroma database (run `stroma init` first)",
                dir.display()
            ));
        }
        let eng = Engine::open(dir.join("wal.log"), 8_000_000)
            .unwrap_or_else(|e| die(&format!("open wal: {e}")));
        let mut cat = Catalog::new();
        let mut cardinality = HashMap::new();
        for line in read_lines(&dir.join("schema.jsonl")) {
            let v: Value =
                serde_json::from_str(&line).unwrap_or_else(|e| die(&format!("schema.jsonl: {e}")));
            apply_def(&mut cat, &mut cardinality, &v).unwrap_or_else(|e| die(&e));
        }
        for line in read_lines(&dir.join("nodes.jsonl")) {
            let v: Value =
                serde_json::from_str(&line).unwrap_or_else(|e| die(&format!("nodes.jsonl: {e}")));
            apply_node(&mut cat, &v["node"]).unwrap_or_else(|e| die(&e));
        }
        let meta: Value = fs::read_to_string(dir.join("meta.json"))
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(json!({}));
        let dim = meta["dim"].as_u64().unwrap_or(0) as usize;
        let emb = read_f32(&dir.join("embeddings.bin"));
        let emb_ids = read_u64(&dir.join("embeddings.ids"));
        if dim > 0 && emb.len() != emb_ids.len() * dim {
            die("embeddings.bin / embeddings.ids length mismatch");
        }
        Db {
            dir: dir.to_path_buf(),
            eng,
            cat,
            cardinality,
            emb_ids,
            emb,
            dim,
        }
    }

    fn append(&self, file: &str, line: &str) {
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(file))
            .unwrap();
        writeln!(f, "{line}").unwrap();
    }

    /// Build the vector index from the received embeddings (derived store; rebuilds on demand).
    fn index(&self) -> Option<IvfPq> {
        if self.emb_ids.is_empty() {
            return None;
        }
        let vecs: Vec<Vec<f32>> = self
            .emb_ids
            .iter()
            .enumerate()
            .map(|(i, _)| self.emb[i * self.dim..(i + 1) * self.dim].to_vec())
            .collect();
        let mut idx = IvfPq::new(
            self.dim,
            IvfPq::suggested_nlist(vecs.len()),
            pick_m(self.dim),
        );
        idx.train(&vecs[..vecs.len().min(20_000)]);
        idx.add_batch(
            self.emb_ids
                .iter()
                .zip(&vecs)
                .enumerate()
                .map(|(i, (&id, v))| {
                    let label = self.cat.node_label(id).unwrap_or(0) as u32;
                    (id, i as u64, v.clone(), label)
                })
                .collect(),
        );
        Some(idx)
    }
}

fn pick_m(dim: usize) -> usize {
    // largest divisor of dim that keeps subvectors >= 4 dims, capped at 96 subquantizers
    for m in (1..=96.min(dim)).rev() {
        if dim.is_multiple_of(m) && dim / m >= 4 {
            return m;
        }
    }
    1
}

fn read_lines(p: &Path) -> Vec<String> {
    match fs::File::open(p) {
        Ok(f) => BufReader::new(f)
            .lines()
            .map_while(Result::ok)
            .filter(|l| !l.trim().is_empty())
            .collect(),
        Err(_) => Vec::new(),
    }
}

fn read_f32(p: &Path) -> Vec<f32> {
    fs::read(p)
        .map(|b| {
            b.chunks_exact(4)
                .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
                .collect()
        })
        .unwrap_or_default()
}

fn read_u64(p: &Path) -> Vec<u64> {
    fs::read(p)
        .map(|b| {
            b.chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                .collect()
        })
        .unwrap_or_default()
}

fn apply_def(
    cat: &mut Catalog,
    card: &mut HashMap<String, Cardinality>,
    v: &Value,
) -> Result<(), String> {
    if let Some(t) = v.get("type_def") {
        let name = t["name"].as_str().ok_or("type_def.name missing")?;
        cat.register_type(name);
        return Ok(());
    }
    if let Some(p) = v.get("pred_def") {
        let name = p["name"].as_str().ok_or("pred_def.name missing")?;
        let c = match p["cardinality"].as_str().unwrap_or("many") {
            "one" => Cardinality::One,
            _ => Cardinality::Many,
        };
        let domain = p["domain"].as_str().ok_or("pred_def.domain missing")?;
        let domain_id = cat
            .field_id(domain)
            .ok_or(format!("unknown domain type: {domain}"))?;
        let range = if let Some(rt) = p["range"].as_str() {
            Range::Type(
                cat.field_id(rt)
                    .ok_or(format!("unknown range type: {rt}"))?,
            )
        } else {
            match p["range_value"].as_str().unwrap_or("text") {
                "int" => Range::Value(ValueType::Int),
                "float" => Range::Value(ValueType::Float),
                "bool" => Range::Value(ValueType::Bool),
                _ => Range::Value(ValueType::Text),
            }
        };
        cat.register_predicate(name, c, RelProps::default(), domain_id, range);
        card.insert(name.to_string(), c);
        return Ok(());
    }
    Err("schema line must be type_def or pred_def".into())
}

fn apply_node(cat: &mut Catalog, n: &Value) -> Result<(), String> {
    let id = n["id"].as_u64().ok_or("node.id missing")?;
    if let Some(t) = n["type"].as_str() {
        let tid = cat.field_id(t).ok_or(format!("unknown type: {t}"))?;
        cat.set_node_type(id, tid);
    }
    if let Some(l) = n["label"].as_u64() {
        cat.set_node_label(id, l as u8);
    }
    Ok(())
}

fn obj_key(v: &Value) -> Result<ObjKey, String> {
    if let Some(n) = v.get("node").and_then(|x| x.as_u64()) {
        return Ok(ObjKey::Node(n));
    }
    if let Some(i) = v.get("int").and_then(|x| x.as_i64()) {
        return Ok(ObjKey::Int(i));
    }
    if let Some(f) = v.get("float").and_then(|x| x.as_f64()) {
        return Ok(ObjKey::Float((f as f32 as f64).to_bits()));
    }
    if let Some(t) = v.get("text").and_then(|x| x.as_str()) {
        return Ok(ObjKey::Text(t.to_string()));
    }
    if let Some(b) = v.get("bool").and_then(|x| x.as_bool()) {
        return Ok(ObjKey::Bool(b));
    }
    Err("object must be one of {node|int|float|text|bool}".into())
}

fn cmd_init(dir: &Path) {
    fs::create_dir_all(dir).unwrap_or_else(|e| die(&format!("mkdir: {e}")));
    if dir.join("wal.log").exists() {
        die("database already exists");
    }
    let eng =
        Engine::open(dir.join("wal.log"), 8_000_000).unwrap_or_else(|e| die(&format!("init: {e}")));
    drop(eng);
    fs::write(dir.join("meta.json"), "{}\n").unwrap();
    println!("initialized stroma database at {}", dir.display());
}

fn cmd_ingest(dir: &Path, file: &str) {
    let mut db = Db::open(dir);
    let source: u32 = 0;
    let mut batch: Vec<(u32, WriteKind)> = Vec::new();
    let (mut n_defs, mut n_nodes, mut n_facts, mut n_retracts) = (0u64, 0u64, 0u64, 0u64);
    for line in read_lines(Path::new(file)) {
        let v: Value =
            serde_json::from_str(&line).unwrap_or_else(|e| die(&format!("bad json: {e}: {line}")));
        if v.get("type_def").is_some() || v.get("pred_def").is_some() {
            apply_def(&mut db.cat, &mut db.cardinality, &v).unwrap_or_else(|e| die(&e));
            db.append("schema.jsonl", &line);
            n_defs += 1;
        } else if v.get("node").is_some() {
            apply_node(&mut db.cat, &v["node"]).unwrap_or_else(|e| die(&e));
            db.append("nodes.jsonl", &line);
            n_nodes += 1;
        } else if let Some(f) = v.get("fact") {
            let subject = f["subject"]
                .as_u64()
                .unwrap_or_else(|| die("fact.subject missing"));
            let pname = f["predicate"]
                .as_str()
                .unwrap_or_else(|| die("fact.predicate missing"));
            db.cat
                .field_id(pname)
                .unwrap_or_else(|| die(&format!("unknown predicate: {pname}")));
            let predicate = db.cat.field_id(pname).unwrap();
            let object = obj_key(&f["object"]).unwrap_or_else(|e| die(&e));
            let valid_from = f["valid_from"].as_i64().unwrap_or(0);
            let kind = match db.cardinality.get(pname) {
                Some(Cardinality::One) => WriteKind::SetOne {
                    subject,
                    predicate,
                    object,
                    valid_from,
                },
                _ => WriteKind::AddMany {
                    subject,
                    predicate,
                    object,
                },
            };
            batch.push((source, kind));
            n_facts += 1;
            if batch.len() >= 10_000 {
                db.eng
                    .write_batch(std::mem::take(&mut batch))
                    .unwrap_or_else(|e| die(&format!("backpressure: {e:?}")));
                db.eng
                    .sync()
                    .unwrap_or_else(|e| die(&format!("fsync: {e}")));
                db.eng.materialize();
            }
        } else if let Some(r) = v.get("retract") {
            // flush pending facts so the retract observes them
            if !batch.is_empty() {
                db.eng
                    .write_batch(std::mem::take(&mut batch))
                    .unwrap_or_else(|e| die(&format!("backpressure: {e:?}")));
                db.eng
                    .sync()
                    .unwrap_or_else(|e| die(&format!("fsync: {e}")));
                db.eng.materialize();
            }
            let subject = r["subject"]
                .as_u64()
                .unwrap_or_else(|| die("retract.subject missing"));
            let pname = r["predicate"]
                .as_str()
                .unwrap_or_else(|| die("retract.predicate missing"));
            let predicate = db
                .cat
                .field_id(pname)
                .unwrap_or_else(|| die(&format!("unknown predicate: {pname}")));
            let object = obj_key(&r["object"]).unwrap_or_else(|e| die(&e));
            db.eng
                .retract_edge(source, subject, predicate, object)
                .unwrap_or_else(|e| die(&format!("backpressure: {e:?}")));
            n_retracts += 1;
        } else {
            die(&format!("unrecognized record: {line}"));
        }
    }
    if !batch.is_empty() {
        db.eng
            .write_batch(batch)
            .unwrap_or_else(|e| die(&format!("backpressure: {e:?}")));
    }
    db.eng
        .sync()
        .unwrap_or_else(|e| die(&format!("fsync: {e}")));
    db.eng.materialize();
    println!(
        "ingested: {n_defs} defs, {n_nodes} nodes, {n_facts} facts, {n_retracts} retracts (durable_head={})",
        db.eng.durable_head()
    );
}

fn cmd_embed(dir: &Path, file: &str) {
    let db = Db::open(dir);
    let mut dim = db.dim;
    let mut bin = OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("embeddings.bin"))
        .unwrap();
    let mut ids = OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("embeddings.ids"))
        .unwrap();
    let mut n = 0u64;
    for line in read_lines(Path::new(file)) {
        let v: Value =
            serde_json::from_str(&line).unwrap_or_else(|e| die(&format!("bad json: {e}")));
        let node = v["node"]
            .as_u64()
            .unwrap_or_else(|| die("embed.node missing"));
        let vecv: Vec<f32> = v["vector"]
            .as_array()
            .unwrap_or_else(|| die("embed.vector missing"))
            .iter()
            .map(|x| x.as_f64().unwrap_or(0.0) as f32)
            .collect();
        if dim == 0 {
            dim = vecv.len();
            fs::write(dir.join("meta.json"), json!({ "dim": dim }).to_string()).unwrap();
        }
        if vecv.len() != dim {
            die(&format!(
                "dimension mismatch: expected {dim}, got {}",
                vecv.len()
            ));
        }
        for x in &vecv {
            bin.write_all(&x.to_le_bytes()).unwrap();
        }
        ids.write_all(&node.to_le_bytes()).unwrap();
        n += 1;
    }
    bin.sync_all().unwrap();
    ids.sync_all().unwrap();
    println!("embedded: {n} vectors (dim={dim})");
}

fn parse_flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn cmd_query(dir: &Path, args: &[String]) {
    let db = Db::open(dir);
    let snap = db.eng.snapshot_arc();
    let sub = args.first().map(|s| s.as_str()).unwrap_or("");
    match sub {
        "point" => {
            let subject: u64 = args
                .get(1)
                .and_then(|a| a.parse().ok())
                .unwrap_or_else(|| die("usage: query point <subject> <predicate>"));
            let pname = args
                .get(2)
                .unwrap_or_else(|| die("usage: query point <subject> <predicate>"));
            let pid = db
                .cat
                .field_id(pname)
                .unwrap_or_else(|| die(&format!("unknown predicate: {pname}")));
            match db.cardinality.get(pname.as_str()) {
                Some(Cardinality::One) => {
                    println!(
                        "{}",
                        json!({ "one": query::point_one(&snap, subject, pid).map(fmt_obj) })
                    );
                }
                _ => {
                    let many: Vec<Value> = query::point_many(&snap, subject, pid)
                        .into_iter()
                        .map(fmt_obj)
                        .collect();
                    println!("{}", json!({ "many": many }));
                }
            }
        }
        "expand" => {
            let subject: u64 = args
                .get(1)
                .and_then(|a| a.parse().ok())
                .unwrap_or_else(|| die("usage: query expand <subject> <predicate>"));
            let pname = args
                .get(2)
                .unwrap_or_else(|| die("usage: query expand <subject> <predicate>"));
            let pid = db
                .cat
                .field_id(pname)
                .unwrap_or_else(|| die(&format!("unknown predicate: {pname}")));
            let out: Vec<u64> = query::expand(&snap, subject, pid).into_iter().collect();
            println!("{}", json!({ "nodes": out }));
        }
        "search" => {
            let ty = parse_flag(args, "--type")
                .unwrap_or_else(|| die("search requires --type <TypeName>"));
            let k: usize = parse_flag(args, "--k")
                .and_then(|s| s.parse().ok())
                .unwrap_or(10);
            let labels: u32 = parse_flag(args, "--allowed-labels")
                .and_then(|s| s.parse().ok())
                .unwrap_or(u32::MAX);
            let mode = match parse_flag(args, "--mode").as_deref() {
                Some("strict") => ReadMode::Strict,
                _ => ReadMode::Fresh,
            };
            let expand_pred = parse_flag(args, "--expand");
            let vec_file = parse_flag(args, "--vector-file")
                .unwrap_or_else(|| die("search requires --vector-file <json array of floats>"));
            let qv: Vec<f32> = serde_json::from_str::<Vec<f64>>(
                &fs::read_to_string(&vec_file)
                    .unwrap_or_else(|e| die(&format!("read {vec_file}: {e}"))),
            )
            .unwrap_or_else(|e| die(&format!("vector json: {e}")))
            .into_iter()
            .map(|x| x as f32)
            .collect();
            let tid = db
                .cat
                .field_id(&ty)
                .unwrap_or_else(|| die(&format!("unknown type: {ty}")));
            let idx = db
                .index()
                .unwrap_or_else(|| die("no embeddings ingested (run `stroma embed` first)"));
            let mut transforms = Vec::new();
            if let Some(p) = expand_pred {
                let pid = db
                    .cat
                    .field_id(&p)
                    .unwrap_or_else(|| die(&format!("unknown predicate: {p}")));
                transforms.push(Transform::Expand { predicate: pid });
            }
            let pipeline = Pipeline {
                source: Source::TypeAnn {
                    q: qv,
                    target_type: tid,
                    k,
                },
                transforms,
                max_nodes: 100,
                mode,
            };
            let vv = VersionVector::new(db.eng.durable_head(), db.emb_ids.len() as u64);
            let t = run(
                &snap,
                &db.cat,
                &idx,
                &pipeline,
                &Principal {
                    allowed_labels: labels,
                },
                vv,
            );
            println!(
                "{}",
                json!({ "ids": t.ids, "scores": t.scores, "as_of": { "changelog": vv.changelog_seqno, "vector": vv.vector_watermark } })
            );
        }
        _ => die("usage: stroma query <point|expand|search> ..."),
    }
}

fn fmt_obj(o: ObjKey) -> Value {
    match o {
        ObjKey::Node(n) => json!({ "node": n }),
        ObjKey::Int(i) => json!({ "int": i }),
        ObjKey::Float(b) => json!({ "float": f64::from_bits(b) }),
        ObjKey::Text(t) => json!({ "text": t }),
        ObjKey::Bool(b) => json!({ "bool": b }),
    }
}

fn cmd_stats(dir: &Path) {
    let db = Db::open(dir);
    let wal_bytes = fs::metadata(dir.join("wal.log"))
        .map(|m| m.len())
        .unwrap_or(0);
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "facts": { "durable_head": db.eng.durable_head(), "unmerged": db.eng.unmerged() },
            "schema": { "defs": read_lines(&dir.join("schema.jsonl")).len(), "nodes": read_lines(&dir.join("nodes.jsonl")).len() },
            "embeddings": { "count": db.emb_ids.len(), "dim": db.dim },
            "storage": { "wal_bytes": wal_bytes, "embeddings_bytes": db.emb.len() * 4 },
        }))
        .unwrap()
    );
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
        "init" => cmd_init(dir),
        "ingest" => cmd_ingest(
            dir,
            rest.first()
                .unwrap_or_else(|| die("usage: stroma ingest <file.jsonl> --db <dir>")),
        ),
        "embed" => cmd_embed(
            dir,
            rest.first()
                .unwrap_or_else(|| die("usage: stroma embed <file.jsonl> --db <dir>")),
        ),
        "query" => cmd_query(dir, &rest),
        "stats" => cmd_stats(dir),
        "serve" => die("`stroma serve` (HTTP + MCP) is tracked separately and not yet implemented"),
        _ => die(usage),
    }
}
