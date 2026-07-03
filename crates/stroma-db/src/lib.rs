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

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use stroma_core::calendar::Calendar;
use stroma_core::catalog::{Cardinality, Catalog, Range, RelProps, ValueType};
use stroma_core::changelog::WriteKind;
use stroma_core::engine::Engine;
use stroma_core::fold::{ObjKey, Snapshot};
use stroma_core::ir::{Pipeline, Principal, Source, Transform, Traverser, run};
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
                let t = self.run_hybrid(req, &snap)?;
                Ok(
                    json!({ "ids": t.ids, "scores": t.scores, "as_of": { "changelog": t.as_of.changelog_seqno, "vector": t.as_of.vector_watermark } }),
                )
            }
            "retrieve_context" => self.retrieve_context(req, &snap),
            "neighborhood" => self.neighborhood(req, &snap),
            "node" => self.node_detail(req, &snap),
            "graph" => self.graph(req, &snap),
            other => Err(format!("unknown op: {other}")),
        }
    }

    /// Distance-bounded subgraph around a focal node: BFS out to `hops` (default 2), following a
    /// given `predicate` or *all* node-valued edges (ontology view), authz-scoped, capped at
    /// `max_nodes` (default 3000). Returns `{nodes:[{id,depth}], edges:[[a,b]]}` — the primitive the
    /// UI's "distance from a node" filter renders.
    fn neighborhood(&self, req: &Value, snap: &Snapshot) -> DbResult<Value> {
        let focus = req["subject"].as_u64().ok_or("subject required")?;
        let hops = req["hops"].as_u64().unwrap_or(2) as usize;
        let cap = req["max_nodes"].as_u64().unwrap_or(3000) as usize;
        let labels = req["allowed_labels"]
            .as_u64()
            .map(|m| m as u32)
            .unwrap_or(u32::MAX);
        let pred = match req["predicate"].as_str() {
            Some(p) => Some(
                self.cat
                    .field_id(p)
                    .ok_or(format!("unknown predicate: {p}"))?,
            ),
            None => None,
        };
        let visible = |n: u64| {
            self.cat
                .node_label(n)
                .is_none_or(|l| (labels >> l) & 1 == 1)
        };

        let mut depth: HashMap<u64, usize> = HashMap::new();
        let mut edges: BTreeSet<(u64, u64)> = BTreeSet::new();
        if !visible(focus) {
            return Ok(json!({ "nodes": [], "edges": [] }));
        }
        depth.insert(focus, 0);
        // undirected adjacency: reach both what the focus points to and what points at it.
        let adj = query::undirected_adjacency(snap, pred);
        let mut frontier = vec![focus];
        for d in 0..hops {
            if frontier.is_empty() || depth.len() >= cap {
                break;
            }
            let mut next = Vec::new();
            for &u in &frontier {
                for &v in adj.get(&u).into_iter().flatten() {
                    if !visible(v) {
                        continue;
                    }
                    edges.insert(if u < v { (u, v) } else { (v, u) });
                    if !depth.contains_key(&v) && depth.len() < cap {
                        depth.insert(v, d + 1);
                        next.push(v);
                    }
                }
            }
            frontier = next;
        }
        let nodes: Vec<Value> = depth
            .iter()
            .map(|(&id, &d)| json!({ "id": id, "depth": d, "name": self.display_name(snap, id) }))
            .collect();
        let strengths = query::edge_strengths(snap, pred);
        let edges: Vec<Value> = edges
            .iter()
            .filter(|(a, b)| depth.contains_key(a) && depth.contains_key(b))
            .map(|(a, b)| json!([a, b, strengths.get(&(*a, *b)).copied().unwrap_or(1)]))
            .collect();
        Ok(json!({ "nodes": nodes, "edges": edges, "focus": focus }))
    }

    /// Full detail of a single node: its type, label, and every stored `(predicate, value)`
    /// assertion (One current value + Many present set), predicate names resolved via the catalog.
    /// Post-authz: if `allowed_labels` is given and the node's label is not permitted, returns
    /// `{id, denied:true}` rather than leaking its properties. Powers the UI's node-inspect panel.
    fn node_detail(&self, req: &Value, snap: &Snapshot) -> DbResult<Value> {
        let subject = req["subject"].as_u64().ok_or("subject required")?;
        let labels = req["allowed_labels"]
            .as_u64()
            .map(|m| m as u32)
            .unwrap_or(u32::MAX);
        let visible = self
            .cat
            .node_label(subject)
            .is_none_or(|l| (labels >> l) & 1 == 1);
        if !visible {
            return Ok(json!({ "id": subject, "denied": true }));
        }
        let (ones, manys) = query::describe(snap, subject);
        let mut props = Vec::with_capacity(ones.len() + manys.len());
        for (p, ok) in ones {
            let name = self.cat.name(p).unwrap_or("?");
            props.push(json!({ "predicate": name, "card": "one", "value": fmt_obj(ok) }));
        }
        for (p, set) in manys {
            let name = self.cat.name(p).unwrap_or("?");
            let vals: Vec<Value> = set.into_iter().map(fmt_obj).collect();
            props.push(json!({ "predicate": name, "card": "many", "values": vals }));
        }
        let ty = self
            .cat
            .node_type(subject)
            .and_then(|t| self.cat.name(t))
            .map(|s| s.to_string());
        Ok(
            json!({ "id": subject, "type": ty, "label": self.cat.node_label(subject), "props": props }),
        )
    }

    /// Whole-graph view: every declared node and its node-valued edges, authz-scoped and capped at
    /// `max_nodes` (default 3000). Unlike `neighborhood` there is no focal distance — the result is
    /// the entire visible graph (or a `truncated` prefix when it exceeds the cap). Same
    /// `{nodes:[{id,depth}], edges:[[a,b]]}` shape so the UI renders it identically.
    fn graph(&self, req: &Value, snap: &Snapshot) -> DbResult<Value> {
        let cap = req["max_nodes"].as_u64().unwrap_or(3000) as usize;
        let labels = req["allowed_labels"]
            .as_u64()
            .map(|m| m as u32)
            .unwrap_or(u32::MAX);
        let visible = |n: u64| {
            self.cat
                .node_label(n)
                .is_none_or(|l| (labels >> l) & 1 == 1)
        };

        let all: Vec<u64> = self
            .cat
            .node_ids()
            .into_iter()
            .filter(|&n| visible(n))
            .collect();
        let truncated = all.len() > cap;
        let keep: BTreeSet<u64> = all.into_iter().take(cap).collect();

        let mut edges: BTreeSet<(u64, u64)> = BTreeSet::new();
        for &u in &keep {
            for v in query::neighbors(snap, u) {
                if keep.contains(&v) {
                    edges.insert(if u < v { (u, v) } else { (v, u) });
                }
            }
        }
        let nodes: Vec<Value> = keep
            .iter()
            .map(|&id| json!({ "id": id, "depth": 0, "name": self.display_name(snap, id) }))
            .collect();
        let strengths = query::edge_strengths(snap, None);
        let edges: Vec<Value> = edges
            .iter()
            .map(|(a, b)| json!([a, b, strengths.get(&(*a, *b)).copied().unwrap_or(1)]))
            .collect();
        Ok(json!({ "nodes": nodes, "edges": edges, "truncated": truncated }))
    }

    /// A node's display name — the value of its first `name`/`title`/`display_name`/`full_name`
    /// text predicate, if any. Used to label nodes in graph/neighbourhood results.
    fn display_name(&self, snap: &Snapshot, id: u64) -> Option<String> {
        const NAME_PREDS: [&str; 4] = ["name", "title", "display_name", "full_name"];
        snap.one
            .range((id, u32::MIN)..=(id, u32::MAX))
            .find_map(|(&(_, p), v)| match v {
                Some(ObjKey::Text(s))
                    if self.cat.name(p).is_some_and(|n| NAME_PREDS.contains(&n)) =>
                {
                    Some(s.clone())
                }
                _ => None,
            })
    }

    /// Shared type-aware hybrid search: builds the pipeline from a JSON request (`type`, `vector`,
    /// `k`, `allowed_labels`, `expand`, `mode`, `max_nodes`) and evaluates it, authz-scoped.
    fn run_hybrid(&self, req: &Value, snap: &Snapshot) -> DbResult<Traverser> {
        let ty = req["type"].as_str().ok_or("type missing")?;
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
            .ok_or("vector missing")?
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
        // embeddings arrive on a separate channel (not seqno-stamped from the changelog), so clamp
        // the vector watermark to the changelog head to keep the version-vector invariant.
        let head = self.eng.durable_head();
        let vw = (self.emb_ids.len() as u64).min(head);
        let vv = VersionVector::new(head, vw);
        Ok(run(
            snap,
            &self.cat,
            idx,
            &pipeline,
            &Principal {
                allowed_labels: labels,
            },
            vv,
        ))
    }

    /// Assemble LLM-ready context from a hybrid search: for each hit, the *current* value of the
    /// `content` predicate plus a calendar-framed stamp of its `date` predicate (weekday, days
    /// relative to `as_of`, business-hours, fiscal quarter), ordered oldest→newest. An optional
    /// calendar frame (`tz_offset_min`, `business_start_min`, `business_end_min`,
    /// `fiscal_year_start_month`) shapes the stamps. Returns `{context, hits, as_of}`.
    fn retrieve_context(&self, req: &Value, snap: &Snapshot) -> DbResult<Value> {
        let content_p = self
            .cat
            .field_id(
                req["content"]
                    .as_str()
                    .ok_or("content predicate required")?,
            )
            .ok_or("unknown content predicate")?;
        let date_p = match req["date"].as_str() {
            Some(d) => Some(self.cat.field_id(d).ok_or("unknown date predicate")?),
            None => None,
        };
        let cal = Calendar {
            utc_offset_min: req["tz_offset_min"].as_i64().unwrap_or(0) as i32,
            business_start_min: req["business_start_min"].as_u64().unwrap_or(540) as u32,
            business_end_min: req["business_end_min"].as_u64().unwrap_or(1080) as u32,
            fiscal_year_start_month: req["fiscal_year_start_month"].as_u64().unwrap_or(1) as u32,
        };

        let t = self.run_hybrid(req, snap)?;
        // gather (node, score, date, content) — current values (fold LWW resolves supersession)
        let mut rows: Vec<(u64, f32, Option<i64>, String)> = t
            .ids
            .iter()
            .zip(&t.scores)
            .map(|(&n, &sc)| {
                let content = match query::point_one(snap, n, content_p) {
                    Some(ObjKey::Text(s)) => s,
                    _ => String::new(),
                };
                let date = date_p.and_then(|dp| match query::point_one(snap, n, dp) {
                    Some(ObjKey::Int(v)) => Some(v),
                    _ => None,
                });
                (n, sc, date, content)
            })
            .collect();

        // as_of = request value, else the most recent hit date, else 0
        let as_of = req["as_of"]
            .as_i64()
            .unwrap_or_else(|| rows.iter().filter_map(|r| r.2).max().unwrap_or(0));

        // chronological order (oldest first); undated last, then by score
        rows.sort_by(|a, b| {
            a.2.unwrap_or(i64::MAX)
                .cmp(&b.2.unwrap_or(i64::MAX))
                .then(b.1.partial_cmp(&a.1).unwrap())
        });

        let mut lines = Vec::with_capacity(rows.len());
        let hits: Vec<Value> = rows
            .iter()
            .map(|(n, sc, date, content)| {
                let stamp = date.map(|d| cal.tag(d, as_of));
                lines.push(match &stamp {
                    Some(s) => format!("- [{s}] {content}"),
                    None => format!("- {content}"),
                });
                json!({ "node": n, "score": sc, "date": date, "stamp": stamp, "content": content })
            })
            .collect();
        let context = format!("(excerpts oldest→newest)\n{}", lines.join("\n"));
        Ok(json!({ "context": context, "hits": hits, "as_of": as_of }))
    }

    pub fn cardinality_of(&self, predicate: &str) -> Option<Cardinality> {
        self.cardinality.get(predicate).copied()
    }

    /// Current durable changelog head — a cheap in-memory monotonic counter used by the console's
    /// live-update poll to detect that the database has advanced.
    pub fn durable_head(&self) -> u64 {
        self.eng.durable_head()
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
