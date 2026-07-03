//! Directory-backed StromaDB — the shared database abstraction behind the `stroma` CLI and the
//! `stroma-serve` HTTP/MCP surface. Owns the on-disk layout, replay-on-open, a cached vector index,
//! and a single JSON dispatch for queries so both frontends speak the same contract.
//!
//! Directory layout (authoritative inputs only; derived stores rebuild on open — the DR design):
//!   wal.log          append-only changelog (facts; crash-sound, group-commit fsync)
//!   schema.jsonl     type/predicate definitions, replayed in order (Field-ID interning is
//!                    order-deterministic, so ids are stable across restarts)
//!   nodes.jsonl      node type/label assignments, replayed
//!   embeddings.bin   received embeddings, flat f32 LE; embeddings.ids = u64 LE per row
//!   meta.json        { "dim": N }
//!
//! Record formats (JSONL) — ingest: type_def / pred_def / node / fact / retract; embed: {node,vector}.
//! Query request (JSON): {"op":"point"|"expand"|"search", ...} — see [`Db::query`].

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use stroma_core::catalog::{Cardinality, Catalog, Range, RelProps, ValueType};
use stroma_core::changelog::WriteKind;
use stroma_core::engine::Engine;
use stroma_core::fold::ObjKey;
use stroma_core::ir::{Pipeline, Principal, Source, Transform, run};
use stroma_core::ivf::IvfPq;
use stroma_core::query;
use stroma_core::version::{ReadMode, VersionVector};

pub type DbResult<T> = Result<T, String>;

/// Counts from an ingest batch.
#[derive(Debug, Default, Clone, Copy)]
pub struct IngestStats {
    pub defs: u64,
    pub nodes: u64,
    pub facts: u64,
    pub retracts: u64,
    pub durable_head: u64,
}

/// A directory-backed database: durable engine + typed catalog + cached vector index.
pub struct Db {
    dir: PathBuf,
    eng: Engine,
    cat: Catalog,
    cardinality: HashMap<String, Cardinality>,
    emb_ids: Vec<u64>,
    emb: Vec<f32>,
    dim: usize,
    index: Option<IvfPq>,
}

impl Db {
    /// Create an empty database directory (errors if one already exists).
    pub fn init(dir: &Path) -> DbResult<()> {
        fs::create_dir_all(dir).map_err(|e| format!("mkdir: {e}"))?;
        if dir.join("wal.log").exists() {
            return Err("database already exists".into());
        }
        Engine::open(dir.join("wal.log"), DEFAULT_N_MAX).map_err(|e| format!("init: {e}"))?;
        fs::write(dir.join("meta.json"), "{}\n").map_err(|e| format!("meta.json: {e}"))?;
        Ok(())
    }

    /// Open an existing database: recover the WAL, replay the catalog, load embeddings, build the
    /// vector index. Uses [`DEFAULT_N_MAX`] for the backlog bound.
    pub fn open(dir: &Path) -> DbResult<Db> {
        Self::open_with(dir, DEFAULT_N_MAX)
    }

    /// Like [`Db::open`] with an explicit un-merged backlog bound (`n_max`): the read-merge tail
    /// length allowed before writes hit backpressure — larger = more RAM headroom, smaller = earlier
    /// backpressure. Not persisted; it is a per-process property of the in-memory changelog.
    pub fn open_with(dir: &Path, n_max: usize) -> DbResult<Db> {
        if !dir.join("wal.log").exists() {
            return Err(format!(
                "{} is not a stroma database (run init first)",
                dir.display()
            ));
        }
        let eng = Engine::open(dir.join("wal.log"), n_max).map_err(|e| format!("open wal: {e}"))?;
        let mut cat = Catalog::new();
        let mut cardinality = HashMap::new();
        for line in read_lines(&dir.join("schema.jsonl")) {
            let v: Value = serde_json::from_str(&line).map_err(|e| format!("schema.jsonl: {e}"))?;
            apply_def(&mut cat, &mut cardinality, &v)?;
        }
        for line in read_lines(&dir.join("nodes.jsonl")) {
            let v: Value = serde_json::from_str(&line).map_err(|e| format!("nodes.jsonl: {e}"))?;
            apply_node(&mut cat, &v["node"])?;
        }
        let meta: Value = fs::read_to_string(dir.join("meta.json"))
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(json!({}));
        let dim = meta["dim"].as_u64().unwrap_or(0) as usize;
        let emb = read_f32(&dir.join("embeddings.bin"));
        let emb_ids = read_u64(&dir.join("embeddings.ids"));
        if dim > 0 && emb.len() != emb_ids.len() * dim {
            return Err("embeddings.bin / embeddings.ids length mismatch".into());
        }
        let mut db = Db {
            dir: dir.to_path_buf(),
            eng,
            cat,
            cardinality,
            emb_ids,
            emb,
            dim,
            index: None,
        };
        db.rebuild_index();
        Ok(db)
    }

    /// Open the database, first creating an empty one if the directory has no WAL yet — the
    /// container-friendly entrypoint (a fresh volume just works).
    pub fn open_or_init(dir: &Path) -> DbResult<Db> {
        Self::open_or_init_with(dir, DEFAULT_N_MAX)
    }

    /// [`Db::open_or_init`] with an explicit backlog bound (see [`Db::open_with`]).
    pub fn open_or_init_with(dir: &Path, n_max: usize) -> DbResult<Db> {
        if !dir.join("wal.log").exists() {
            Self::init(dir)?;
        }
        Self::open_with(dir, n_max)
    }

    fn append_line(&self, file: &str, line: &str) -> DbResult<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(file))
            .map_err(|e| format!("open {file}: {e}"))?;
        writeln!(f, "{line}").map_err(|e| format!("write {file}: {e}"))
    }

    fn rebuild_index(&mut self) {
        if self.emb_ids.is_empty() || self.dim == 0 {
            self.index = None;
            return;
        }
        let vecs: Vec<Vec<f32>> = (0..self.emb_ids.len())
            .map(|i| self.emb[i * self.dim..(i + 1) * self.dim].to_vec())
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
                    (
                        id,
                        i as u64,
                        v.clone(),
                        self.cat.node_label(id).unwrap_or(0) as u32,
                    )
                })
                .collect(),
        );
        self.index = Some(idx);
    }

    /// Ingest a JSONL batch (type_def / pred_def / node / fact / retract). Durable on return.
    pub fn ingest_str(&mut self, jsonl: &str) -> DbResult<IngestStats> {
        let mut s = IngestStats::default();
        let mut batch: Vec<(u32, WriteKind)> = Vec::new();
        let mut touched_nodes = false;
        for line in jsonl.lines().filter(|l| !l.trim().is_empty()) {
            let v: Value =
                serde_json::from_str(line).map_err(|e| format!("bad json: {e}: {line}"))?;
            if v.get("type_def").is_some() || v.get("pred_def").is_some() {
                apply_def(&mut self.cat, &mut self.cardinality, &v)?;
                self.append_line("schema.jsonl", line)?;
                s.defs += 1;
            } else if v.get("node").is_some() {
                apply_node(&mut self.cat, &v["node"])?;
                self.append_line("nodes.jsonl", line)?;
                touched_nodes = true;
                s.nodes += 1;
            } else if let Some(f) = v.get("fact") {
                let subject = f["subject"].as_u64().ok_or("fact.subject missing")?;
                let pname = f["predicate"].as_str().ok_or("fact.predicate missing")?;
                let predicate = self
                    .cat
                    .field_id(pname)
                    .ok_or(format!("unknown predicate: {pname}"))?;
                let object = obj_key(&f["object"])?;
                let valid_from = f["valid_from"].as_i64().unwrap_or(0);
                let kind = match self.cardinality.get(pname) {
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
                batch.push((0, kind));
                s.facts += 1;
                if batch.len() >= 10_000 {
                    self.flush(&mut batch)?;
                }
            } else if let Some(r) = v.get("retract") {
                self.flush(&mut batch)?; // retract must observe prior writes
                let subject = r["subject"].as_u64().ok_or("retract.subject missing")?;
                let pname = r["predicate"].as_str().ok_or("retract.predicate missing")?;
                let predicate = self
                    .cat
                    .field_id(pname)
                    .ok_or(format!("unknown predicate: {pname}"))?;
                let object = obj_key(&r["object"])?;
                self.eng
                    .retract_edge(0, subject, predicate, object)
                    .map_err(|e| format!("backpressure: {e:?}"))?;
                s.retracts += 1;
            } else {
                return Err(format!("unrecognized record: {line}"));
            }
        }
        self.flush(&mut batch)?;
        self.eng.sync().map_err(|e| format!("fsync: {e}"))?;
        self.eng.materialize();
        s.durable_head = self.eng.durable_head();
        if touched_nodes {
            self.rebuild_index();
        }
        Ok(s)
    }

    fn flush(&mut self, batch: &mut Vec<(u32, WriteKind)>) -> DbResult<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.eng
            .write_batch(std::mem::take(batch))
            .map_err(|e| format!("backpressure: {e:?}"))?;
        self.eng.sync().map_err(|e| format!("fsync: {e}"))?;
        self.eng.materialize();
        Ok(())
    }

    /// Append received embeddings ({"node":N,"vector":[...]} per line) and rebuild the index.
    pub fn embed_str(&mut self, jsonl: &str) -> DbResult<usize> {
        let mut n = 0usize;
        for line in jsonl.lines().filter(|l| !l.trim().is_empty()) {
            let v: Value = serde_json::from_str(line).map_err(|e| format!("bad json: {e}"))?;
            let node = v["node"].as_u64().ok_or("embed.node missing")?;
            let vecv: Vec<f32> = v["vector"]
                .as_array()
                .ok_or("embed.vector missing")?
                .iter()
                .map(|x| x.as_f64().unwrap_or(0.0) as f32)
                .collect();
            if self.dim == 0 {
                self.dim = vecv.len();
                fs::write(
                    self.dir.join("meta.json"),
                    json!({ "dim": self.dim }).to_string(),
                )
                .map_err(|e| format!("meta.json: {e}"))?;
            }
            if vecv.len() != self.dim {
                return Err(format!(
                    "dimension mismatch: expected {}, got {}",
                    self.dim,
                    vecv.len()
                ));
            }
            self.emb.extend_from_slice(&vecv);
            self.emb_ids.push(node);
            n += 1;
        }
        // persist (append)
        let mut bin = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("embeddings.bin"))
            .map_err(|e| format!("{e}"))?;
        let start = self.emb.len() - n * self.dim;
        for &x in &self.emb[start..] {
            bin.write_all(&x.to_le_bytes())
                .map_err(|e| format!("{e}"))?;
        }
        let mut ids = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("embeddings.ids"))
            .map_err(|e| format!("{e}"))?;
        for &id in &self.emb_ids[self.emb_ids.len() - n..] {
            ids.write_all(&id.to_le_bytes())
                .map_err(|e| format!("{e}"))?;
        }
        self.rebuild_index();
        Ok(n)
    }

    /// Run a JSON query request and return a JSON result.
    ///
    /// - `{"op":"point","subject":N,"predicate":"name"}` → `{"one":..}` or `{"many":[..]}`
    /// - `{"op":"expand","subject":N,"predicate":"name"}` → `{"nodes":[..]}`
    /// - `{"op":"search","type":"T","vector":[..],"k":K,"allowed_labels":M,"expand":"pred","mode":"fresh|strict"}`
    ///   → `{"ids":[..],"scores":[..],"as_of":{..}}`
    pub fn query(&self, req: &Value) -> DbResult<Value> {
        let snap = self.eng.snapshot_arc();
        match req["op"].as_str().ok_or("missing op")? {
            "point" => {
                let subject = req["subject"].as_u64().ok_or("point.subject missing")?;
                let pname = req["predicate"].as_str().ok_or("point.predicate missing")?;
                let pid = self
                    .cat
                    .field_id(pname)
                    .ok_or(format!("unknown predicate: {pname}"))?;
                Ok(match self.cardinality.get(pname) {
                    Some(Cardinality::One) => {
                        json!({ "one": query::point_one(&snap, subject, pid).map(fmt_obj) })
                    }
                    _ => {
                        json!({ "many": query::point_many(&snap, subject, pid).into_iter().map(fmt_obj).collect::<Vec<_>>() })
                    }
                })
            }
            "expand" => {
                let subject = req["subject"].as_u64().ok_or("expand.subject missing")?;
                let pname = req["predicate"]
                    .as_str()
                    .ok_or("expand.predicate missing")?;
                let pid = self
                    .cat
                    .field_id(pname)
                    .ok_or(format!("unknown predicate: {pname}"))?;
                Ok(
                    json!({ "nodes": query::expand(&snap, subject, pid).into_iter().collect::<Vec<_>>() }),
                )
            }
            "search" => {
                let ty = req["type"].as_str().ok_or("search.type missing")?;
                let tid = self.cat.field_id(ty).ok_or(format!("unknown type: {ty}"))?;
                let k = req["k"].as_u64().unwrap_or(10) as usize;
                let labels = req["allowed_labels"]
                    .as_u64()
                    .map(|m| m as u32)
                    .unwrap_or(u32::MAX);
                let mode = if req["mode"].as_str() == Some("strict") {
                    ReadMode::Strict
                } else {
                    ReadMode::Fresh
                };
                let qv: Vec<f32> = req["vector"]
                    .as_array()
                    .ok_or("search.vector missing")?
                    .iter()
                    .map(|x| x.as_f64().unwrap_or(0.0) as f32)
                    .collect();
                let idx = self.index.as_ref().ok_or("no embeddings ingested")?;
                let mut transforms = Vec::new();
                if let Some(p) = req["expand"].as_str() {
                    let pid = self
                        .cat
                        .field_id(p)
                        .ok_or(format!("unknown predicate: {p}"))?;
                    transforms.push(Transform::Expand { predicate: pid });
                }
                let pipeline = Pipeline {
                    source: Source::TypeAnn {
                        q: qv,
                        target_type: tid,
                        k,
                    },
                    transforms,
                    max_nodes: req["max_nodes"].as_u64().unwrap_or(100) as usize,
                    mode,
                };
                // embeddings arrive on a separate channel here (not seqno-stamped from the changelog),
                // so clamp the vector watermark to the changelog head to keep the version-vector
                // invariant (vector <= changelog). Fresh mode (default) ignores the watermark anyway.
                let head = self.eng.durable_head();
                let vw = (self.emb_ids.len() as u64).min(head);
                let vv = VersionVector::new(head, vw);
                let t = run(
                    &snap,
                    &self.cat,
                    idx,
                    &pipeline,
                    &Principal {
                        allowed_labels: labels,
                    },
                    vv,
                );
                Ok(
                    json!({ "ids": t.ids, "scores": t.scores, "as_of": { "changelog": vv.changelog_seqno, "vector": vv.vector_watermark } }),
                )
            }
            other => Err(format!("unknown op: {other}")),
        }
    }

    pub fn cardinality_of(&self, predicate: &str) -> Option<Cardinality> {
        self.cardinality.get(predicate).copied()
    }

    pub fn stats(&self) -> Value {
        let wal_bytes = fs::metadata(self.dir.join("wal.log"))
            .map(|m| m.len())
            .unwrap_or(0);
        json!({
            "facts": { "durable_head": self.eng.durable_head(), "unmerged": self.eng.unmerged() },
            "schema": { "defs": read_lines(&self.dir.join("schema.jsonl")).len(), "nodes": read_lines(&self.dir.join("nodes.jsonl")).len() },
            "embeddings": { "count": self.emb_ids.len(), "dim": self.dim },
            "storage": { "wal_bytes": wal_bytes, "embeddings_bytes": self.emb.len() * 4 },
        })
    }
}

/// Default un-merged backlog bound — the read-merge tail length before backpressure.
pub const DEFAULT_N_MAX: usize = 8_000_000;

fn pick_m(dim: usize) -> usize {
    for m in (1..=96.min(dim)).rev() {
        if dim.is_multiple_of(m) && dim / m >= 4 {
            return m;
        }
    }
    1
}

fn read_lines(p: &Path) -> Vec<String> {
    fs::read_to_string(p)
        .map(|s| {
            s.lines()
                .filter(|l| !l.trim().is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
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
) -> DbResult<()> {
    if let Some(t) = v.get("type_def") {
        cat.register_type(t["name"].as_str().ok_or("type_def.name missing")?);
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

fn apply_node(cat: &mut Catalog, n: &Value) -> DbResult<()> {
    let id = n["id"].as_u64().ok_or("node.id missing")?;
    if let Some(t) = n["type"].as_str() {
        cat.set_node_type(id, cat.field_id(t).ok_or(format!("unknown type: {t}"))?);
    }
    if let Some(l) = n["label"].as_u64() {
        cat.set_node_label(id, l as u8);
    }
    Ok(())
}

fn obj_key(v: &Value) -> DbResult<ObjKey> {
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

fn fmt_obj(o: ObjKey) -> Value {
    match o {
        ObjKey::Node(n) => json!({ "node": n }),
        ObjKey::Int(i) => json!({ "int": i }),
        ObjKey::Float(b) => json!({ "float": f64::from_bits(b) }),
        ObjKey::Text(t) => json!({ "text": t }),
        ObjKey::Bool(b) => json!({ "bool": b }),
    }
}
