//! Directory-backed StromaDB — the shared database abstraction behind the `stroma` CLI and the
//! `stroma-serve` HTTP/MCP surface. Owns the on-disk layout, replay-on-open, a cached vector index,
//! and a single JSON dispatch for queries so both frontends speak the same contract.
//!
//! Concurrency (lock-free reads during writes): [`Db`] splits into a write authority
//! ([`WriteState`], behind a `Mutex`) and an immutable pinned read view ([`ReadState`], behind an
//! `RwLock<Arc<ReadState>>`). A read clones the current `Arc<ReadState>` under a momentary lock and
//! then runs entirely on that pinned state with no lock held; a write holds the write mutex for the
//! whole ETL and, on completion, swaps in a fresh `Arc<ReadState>`. So a long write never blocks a
//! read, and a read is snapshot-isolated against writes that land after it pins.
//!
//! Directory layout (authoritative inputs only; derived stores rebuild on open — the DR design):
//!   wal.log          append-only changelog (facts + node type/label ops; crash-sound, group-commit)
//!   schema.jsonl     type/predicate definitions, replayed in order (Field-ID interning is
//!                    order-deterministic, so ids are stable across restarts)
//!   rules.jsonl      named conformance rules (`rule_def`), replayed in order into the rule registry
//!   nodes.jsonl      node type/label assignments (audit mirror of what was ingested; the authority
//!                    is the WAL ops, which the recovered snapshot carries — not replayed)
//!   embeddings.bin   received embeddings, flat f32 LE; embeddings.ids = u64 LE per row
//!   meta.json        { "dim": N }
//!
//! Record formats (JSONL) — ingest: type_def / pred_def / rule_def / node / fact / retract / close;
//! embed: {node,vector}.
//! Query request (JSON): {"op":"point"|"expand"|"search", ...} — see [`Db::query`].

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use serde_json::{Value, json};
use stroma_core::calendar::Calendar;
use stroma_core::catalog::{Cardinality, Catalog, Range, RelProps, ValueType};
use stroma_core::changelog::WriteKind;
use stroma_core::completeness;
use stroma_core::conformance;
use stroma_core::engine::Engine;
use stroma_core::fact::{FieldId, NodeId};
use stroma_core::fold::{ObjKey, Snapshot};
use stroma_core::ir::{Filter, NoAnn, Pipeline, Principal, Source, Transform, Traverser, run};
use stroma_core::ivf::IvfPq;
use stroma_core::query;
use stroma_core::version::{ReadMode, VersionVector};

pub type DbResult<T> = Result<T, String>;

/// Counts from an ingest batch. `retracts` counts only retracts that removed a present edge (an
/// absent-edge retract is a no-op); `closes` counts `close` records (each is a changelog write).
#[derive(Debug, Default, Clone, Copy)]
pub struct IngestStats {
    pub defs: u64,
    pub nodes: u64,
    pub facts: u64,
    pub retracts: u64,
    pub closes: u64,
    pub durable_head: u64,
}

/// The schema-level catalog authority: the interner + registered types/predicates plus each
/// predicate's cardinality, and the durable registry of named conformance rules. Cloneable and
/// `Arc`-shared into the read view; rebuilt (copy-on-write) only when a `type_def`/`pred_def`/
/// `rule_def` arrives, so the frequent node/fact writes never re-clone it.
#[derive(Clone, Default)]
struct Schema {
    cat: Catalog,
    cardinality: HashMap<String, Cardinality>,
    /// Named conformance rules declared once (`rule_def`) and evaluated by `rule_name`. Parsed at
    /// declaration; names are resolved against the catalog only at evaluation.
    rules: HashMap<String, conformance::Rule>,
}

/// A directory-backed database. Reads are lock-free over a pinned [`ReadState`]; writes hold the
/// `write` mutex for the ETL and then publish a fresh read view.
pub struct Db {
    write: Mutex<WriteState>,
    read: RwLock<Arc<ReadState>>,
}

/// Everything mutated during a write. Held behind `Db::write`.
struct WriteState {
    dir: PathBuf,
    eng: Engine,
    /// Schema authority, `Arc`-shared with the current read view (copy-on-write on def changes).
    schema: Arc<Schema>,
    /// Write-side node→label map — the index-build authority (labels ride the ANN posting lists).
    /// Node types reach readers via the snapshot's `node_types` (folded from `SetNodeType` ops).
    node_label_w: HashMap<NodeId, u8>,
    /// Received embeddings, `Arc`-shared with the read view; appended (copy-on-write) by `embed`.
    emb_ids: Arc<Vec<u64>>,
    emb: Arc<Vec<f32>>,
    dim: usize,
    index: Arc<Option<IvfPq>>,
    n_max: usize,
}

/// An immutable, pinned read view. A read clones the `Arc<ReadState>` then runs entirely on it with
/// no lock held, so it is isolated from any write that publishes a newer view afterwards.
pub struct ReadState {
    /// Graph + node type/label maps, pinned at publish time (from the engine's shared snapshot).
    snap: Arc<Snapshot>,
    /// Schema-level catalog, `Arc`-shared; rebuilt only on `type_def`/`pred_def`.
    schema: Arc<Schema>,
    index: Arc<Option<IvfPq>>,
    emb_ids: Arc<Vec<u64>>,
    emb: Arc<Vec<f32>>,
    dim: usize,
    /// The durable changelog head this view was pinned at — the `as_of` for the version vector.
    durable_head: u64,
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

    /// Open an existing database: recover the WAL (facts + node ops), replay the schema catalog, load
    /// embeddings, build the vector index. Uses [`DEFAULT_N_MAX`] for the backlog bound.
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
        let mut schema = Schema::default();
        for line in read_lines(&dir.join("schema.jsonl")) {
            let v: Value = serde_json::from_str(&line).map_err(|e| format!("schema.jsonl: {e}"))?;
            apply_def(&mut schema, &v)?;
        }
        // Named conformance rules replay after the catalog (names are resolved only at evaluation,
        // so rule order relative to the defs it references does not matter here).
        for line in read_lines(&dir.join("rules.jsonl")) {
            let v: Value = serde_json::from_str(&line).map_err(|e| format!("rules.jsonl: {e}"))?;
            apply_rule_def(&mut schema, &v)?;
        }
        // Node type/label attributes live in the WAL now (SetNodeType/SetNodeLabel ops), so the
        // recovered snapshot already carries them — nodes.jsonl is kept only for counts, not replayed.
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
        // Reconstruct the write-side label map from the recovered snapshot (its single source now).
        let snap = eng.snapshot_arc();
        let node_label_w: HashMap<NodeId, u8> =
            snap.node_labels.iter().map(|(&k, &v)| (k, v)).collect();
        let mut w = WriteState {
            dir: dir.to_path_buf(),
            eng,
            schema: Arc::new(schema),
            node_label_w,
            emb_ids: Arc::new(emb_ids),
            emb: Arc::new(emb),
            dim,
            index: Arc::new(None),
            n_max,
        };
        w.rebuild_index();
        let rs = Arc::new(w.build_read_state());
        Ok(Db {
            write: Mutex::new(w),
            read: RwLock::new(rs),
        })
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

    /// Clear the database to empty: remove the authoritative inputs (changelog, schema/node
    /// assignments, received embeddings) and re-open a fresh engine, then publish an empty read view.
    /// **Destructive** — every fact is gone. Intended for tests and dev/demo resets; the
    /// `stroma-serve` endpoint that exposes it is opt-in and off by default.
    pub fn reset(&self) -> DbResult<()> {
        let mut w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        for f in [
            "wal.log",
            "schema.jsonl",
            "rules.jsonl",
            "nodes.jsonl",
            "embeddings.bin",
            "embeddings.ids",
            "meta.json",
        ] {
            let p = w.dir.join(f);
            if p.exists() {
                fs::remove_file(&p).map_err(|e| format!("reset: remove {f}: {e}"))?;
            }
        }
        Self::init(&w.dir)?;
        let eng = Engine::open(w.dir.join("wal.log"), w.n_max)
            .map_err(|e| format!("reset: open: {e}"))?;
        w.eng = eng;
        w.schema = Arc::new(Schema::default());
        w.node_label_w.clear();
        w.emb_ids = Arc::new(Vec::new());
        w.emb = Arc::new(Vec::new());
        w.dim = 0;
        w.index = Arc::new(None);
        self.publish(&w);
        Ok(())
    }

    /// Ingest a JSONL batch (type_def / pred_def / rule_def / node / fact / retract / close).
    /// Durable on return; the updated read view is published atomically before this returns.
    pub fn ingest_str(&self, jsonl: &str) -> DbResult<IngestStats> {
        let mut w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        let s = w.ingest(jsonl)?;
        self.publish(&w);
        Ok(s)
    }

    /// Append received embeddings ({"node":N,"vector":[...]} per line), rebuild the index, and publish.
    pub fn embed_str(&self, jsonl: &str) -> DbResult<usize> {
        let mut w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        let n = w.embed(jsonl)?;
        self.publish(&w);
        Ok(n)
    }

    /// Pin the current read view and run a JSON query on it with no lock held (lock-free read).
    ///
    /// - `{"op":"point","subject":N,"predicate":"name"[,"valid_at":T][,"now":T,"max_age":A]}` →
    ///   `{"one":..}` or `{"many":[..]}` (`valid_at` = valid-time as-of read for a One-predicate: the
    ///   value in effect at instant `T`). A *current* One answer also carries the winning version's
    ///   `"valid_from"` and an additive `"confidence"` `{tier, corroboration, sources[, age]}` — a
    ///   coarse tier plus its raw signals; both omitted for an as-of / absent read. `now`/`max_age`
    ///   supply the freshness reference (`age = now - valid_from`; stale when `age > max_age`).
    ///   A current One answer whose winning version is a *close* carries `"closed_from"` (the
    ///   close's `valid_from`) next to `"one": null`; never for an as-of read or a never-written key.
    /// - `{"op":"expand","subject":N,"predicate":"name"[,"max_depth":D]}` → `{"nodes":[..]}`
    ///   (honors the predicate's declared props — symmetric / inverse / transitive; `max_depth`
    ///   bounds the transitive closure, default 16)
    /// - `{"op":"edge_props","subject":N,"predicate":"name","object":{..}}` → `{"props":{k:v,..}}`
    ///   (properties on the edge `(subject, predicate, object)`; set at ingest via a fact's `props`)
    /// - `{"op":"search","type":"T","vector":[..],"k":K,"allowed_labels":M,"expand":"pred","mode":"fresh|strict"}`
    ///   → `{"ids":[..],"scores":[..],"as_of":{..}}`
    pub fn query(&self, req: &Value) -> DbResult<Value> {
        let rs = self.read_state();
        rs.query(req)
    }

    /// Pin and return the current read view (an `Arc<ReadState>`). Cheap — a momentary lock + an
    /// `Arc` clone. The returned view is stable: writes that publish afterwards do not affect it.
    pub fn read_state(&self) -> Arc<ReadState> {
        self.read.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Publish a fresh read view built from the (locked) write state.
    fn publish(&self, w: &WriteState) {
        *self.read.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(w.build_read_state());
    }

    pub fn cardinality_of(&self, predicate: &str) -> Option<Cardinality> {
        self.read
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .schema
            .cardinality
            .get(predicate)
            .copied()
    }

    /// Current durable changelog head — a cheap in-memory monotonic counter used by the console's
    /// live-update poll to detect that the database has advanced (read off the pinned view).
    pub fn durable_head(&self) -> u64 {
        self.read
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .durable_head
    }

    pub fn stats(&self) -> Value {
        let w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        let wal_bytes = fs::metadata(w.dir.join("wal.log"))
            .map(|m| m.len())
            .unwrap_or(0);
        json!({
            "facts": { "durable_head": w.eng.durable_head(), "unmerged": w.eng.unmerged() },
            // Catalog size, not lines processed: connectors legitimately re-send their schema with
            // every self-contained batch, so counting the persisted def/node lines reads as
            // unbounded growth on a dashboard while the catalog holds a few dozen entries.
            "schema": {
                "types": w.schema.cat.types_len(),
                "predicates": w.schema.cat.predicates().count(),
                "nodes": node_ids(&w.eng.snapshot_arc()).len(),
                "rules": w.schema.rules.len(),
            },
            "embeddings": { "count": w.emb_ids.len(), "dim": w.dim },
            "storage": { "wal_bytes": wal_bytes, "embeddings_bytes": w.emb.len() * 4 },
        })
    }
}

impl WriteState {
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
            self.index = Arc::new(None);
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
                        self.node_label_w.get(&id).copied().unwrap_or(0) as u32,
                    )
                })
                .collect(),
        );
        self.index = Arc::new(Some(idx));
    }

    /// Snapshot the current write state into an immutable read view.
    fn build_read_state(&self) -> ReadState {
        ReadState {
            snap: self.eng.snapshot_arc(),
            schema: Arc::clone(&self.schema),
            index: Arc::clone(&self.index),
            emb_ids: Arc::clone(&self.emb_ids),
            emb: Arc::clone(&self.emb),
            dim: self.dim,
            durable_head: self.eng.durable_head(),
        }
    }

    /// Resolve a fact's optional `source` name to the interned Field-ID stamped on the write's
    /// `OrderKey` (its provenance). Absent source → `0`, the "unset"/unknown sentinel. A source name
    /// is just another interned string: an already-known name (a prior source, or a type/predicate of
    /// the same spelling) is a cheap lookup that never touches the shared schema; a genuinely new name
    /// is interned copy-on-write (like a def — at most one Arc rebuild per batch) and persisted as a
    /// `source_def` line so replay re-interns it in the same order and reproduces the exact id. The id
    /// space lives entirely in `schema.jsonl`, so the numeric `source` the WAL stores stays resolvable
    /// across a reopen.
    fn source_id(&mut self, name: Option<&str>) -> DbResult<FieldId> {
        let Some(n) = name else { return Ok(0) };
        if let Some(id) = self.schema.cat.field_id(n) {
            return Ok(id);
        }
        let id = Arc::make_mut(&mut self.schema).cat.intern_ref(n);
        self.append_line(
            "schema.jsonl",
            &json!({ "source_def": { "name": n } }).to_string(),
        )?;
        Ok(id)
    }

    /// The write-side of ingest: parse the batch, emit ops to the engine, persist inputs, fsync,
    /// materialize. Node type/label lines emit `SetNodeType`/`SetNodeLabel` ops through the engine so
    /// the snapshot carries them, and mirror the label into `node_label_w` for the index build.
    fn ingest(&mut self, jsonl: &str) -> DbResult<IngestStats> {
        let mut s = IngestStats::default();
        let mut batch: Vec<(u32, WriteKind)> = Vec::new();
        let mut touched_nodes = false;
        for line in jsonl.lines().filter(|l| !l.trim().is_empty()) {
            let v: Value =
                serde_json::from_str(line).map_err(|e| format!("bad json: {e}: {line}"))?;
            if v.get("type_def").is_some() || v.get("pred_def").is_some() {
                apply_def(Arc::make_mut(&mut self.schema), &v)?;
                self.append_line("schema.jsonl", line)?;
                s.defs += 1;
            } else if v.get("rule_def").is_some() {
                // A named conformance rule: parse + store (names are validated at evaluation, not
                // here — the referenced predicates may be declared later), persist for replay.
                apply_rule_def(Arc::make_mut(&mut self.schema), &v)?;
                self.append_line("rules.jsonl", line)?;
            } else if let Some(n) = v.get("node") {
                self.apply_node(n)?;
                self.append_line("nodes.jsonl", line)?;
                touched_nodes = true;
                s.nodes += 1;
            } else if let Some(f) = v.get("fact") {
                let subject = f["subject"].as_u64().ok_or("fact.subject missing")?;
                let pname = f["predicate"].as_str().ok_or("fact.predicate missing")?;
                let predicate = self
                    .schema
                    .cat
                    .field_id(pname)
                    .ok_or(format!("unknown predicate: {pname}"))?;
                let object = obj_key(&f["object"])?;
                let valid_from = f["valid_from"].as_i64().unwrap_or(0);
                let valid_to = f["valid_to"].as_i64();
                // per-fact provenance: intern the optional source name to its stable Field-ID (absent
                // → 0). Interned once and reused for this fact's edge-property writes too.
                let source = self.source_id(f.get("source").and_then(|x| x.as_str()))?;
                let kind = match self.schema.cardinality.get(pname) {
                    Some(Cardinality::One) => WriteKind::SetOne {
                        subject,
                        predicate,
                        object: object.clone(),
                        valid_from,
                        valid_to,
                    },
                    _ => WriteKind::AddMany {
                        subject,
                        predicate,
                        object: object.clone(),
                    },
                };
                batch.push((source, kind));
                s.facts += 1;
                // optional edge properties on this fact's edge (subject, predicate, object)
                if let Some(props) = f.get("props").and_then(|p| p.as_object()) {
                    for (key, val) in props {
                        batch.push((
                            source,
                            WriteKind::SetEdgeProp {
                                subject,
                                predicate,
                                object: object.clone(),
                                key: key.clone(),
                                value: value_key(val)?,
                            },
                        ));
                    }
                }
                if batch.len() >= 10_000 {
                    self.flush(&mut batch)?;
                }
            } else if let Some(c) = v.get("close") {
                // Close a cardinality-one value: no successor — the head becomes absent and as-of
                // reads at or after `valid_from` return nothing. Maps to `CloseOne` in the changelog
                // (a versioned row with no object), so it replays and merges like any other write.
                let subject = c["subject"].as_u64().ok_or("close.subject missing")?;
                let pname = c["predicate"].as_str().ok_or("close.predicate missing")?;
                let predicate = self
                    .schema
                    .cat
                    .field_id(pname)
                    .ok_or(format!("unknown predicate: {pname}"))?;
                match self.schema.cardinality.get(pname) {
                    Some(Cardinality::One) => {}
                    Some(Cardinality::Many) => {
                        return Err(format!(
                            "cannot close '{pname}' (cardinality-many): use a retract record to remove an edge"
                        ));
                    }
                    None => return Err(format!("unknown predicate: {pname}")),
                }
                let valid_from = c["valid_from"].as_i64().unwrap_or(0);
                let source = self.source_id(c.get("source").and_then(|x| x.as_str()))?;
                batch.push((
                    source,
                    WriteKind::CloseOne {
                        subject,
                        predicate,
                        valid_from,
                    },
                ));
                s.closes += 1;
                if batch.len() >= 10_000 {
                    self.flush(&mut batch)?;
                }
            } else if let Some(r) = v.get("retract") {
                self.flush(&mut batch)?; // retract must observe prior writes
                let subject = r["subject"].as_u64().ok_or("retract.subject missing")?;
                let pname = r["predicate"].as_str().ok_or("retract.predicate missing")?;
                let predicate = self
                    .schema
                    .cat
                    .field_id(pname)
                    .ok_or(format!("unknown predicate: {pname}"))?;
                // Retract resolves OR-Set observed tags — a many-only mechanism. A one-predicate has
                // no tags, so a retract on it would be a silent no-op: reject it and point at `close`.
                if self.schema.cardinality.get(pname) == Some(&Cardinality::One) {
                    return Err(format!(
                        "cannot retract '{pname}' (cardinality-one): use a close record to end its value"
                    ));
                }
                let object = obj_key(&r["object"])?;
                let source = self.source_id(r.get("source").and_then(|x| x.as_str()))?;
                let removed = self
                    .eng
                    .retract_edge(source, subject, predicate, object)
                    .map_err(|e| format!("backpressure: {e:?}"))?;
                // count only retracts that removed a present edge (absent edge → no-op, not counted)
                if removed.is_some() {
                    s.retracts += 1;
                }
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

    /// A node record: emit its type/label as engine ops (so the snapshot carries them) and mirror the
    /// label into the write-side index-build map.
    fn apply_node(&mut self, n: &Value) -> DbResult<()> {
        let id = n["id"].as_u64().ok_or("node.id missing")?;
        if let Some(t) = n["type"].as_str() {
            let tid = self
                .schema
                .cat
                .field_id(t)
                .ok_or(format!("unknown type: {t}"))?;
            self.eng
                .write(
                    0,
                    WriteKind::SetNodeType {
                        node: id,
                        type_id: tid,
                    },
                )
                .map_err(|e| format!("backpressure: {e:?}"))?;
        }
        if let Some(l) = n["label"].as_u64() {
            let label = l as u8;
            self.node_label_w.insert(id, label);
            self.eng
                .write(0, WriteKind::SetNodeLabel { node: id, label })
                .map_err(|e| format!("backpressure: {e:?}"))?;
        }
        Ok(())
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

    fn embed(&mut self, jsonl: &str) -> DbResult<usize> {
        // Parse + validate the whole batch first (so a mid-batch dimension error persists nothing).
        let mut vectors: Vec<(u64, Vec<f32>)> = Vec::new();
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
            vectors.push((node, vecv));
        }
        let n = vectors.len();
        if n == 0 {
            return Ok(0);
        }
        // persist (append) then update the in-memory buffers (copy-on-write so readers keep their Arc)
        let mut bin = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("embeddings.bin"))
            .map_err(|e| format!("{e}"))?;
        let mut ids = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("embeddings.ids"))
            .map_err(|e| format!("{e}"))?;
        let emb = Arc::make_mut(&mut self.emb);
        let emb_ids = Arc::make_mut(&mut self.emb_ids);
        for (node, vecv) in &vectors {
            for &x in vecv {
                bin.write_all(&x.to_le_bytes())
                    .map_err(|e| format!("{e}"))?;
            }
            ids.write_all(&node.to_le_bytes())
                .map_err(|e| format!("{e}"))?;
            emb.extend_from_slice(vecv);
            emb_ids.push(*node);
        }
        self.rebuild_index();
        Ok(n)
    }
}

impl ReadState {
    /// Run a JSON query request against this pinned read view.
    pub fn query(&self, req: &Value) -> DbResult<Value> {
        match req["op"].as_str().ok_or("missing op")? {
            "point" => {
                let subject = req["subject"].as_u64().ok_or("point.subject missing")?;
                let pname = req["predicate"].as_str().ok_or("point.predicate missing")?;
                let pid = self
                    .schema
                    .cat
                    .field_id(pname)
                    .ok_or(format!("unknown predicate: {pname}"))?;
                // optional valid-time as-of: `"valid_at": T` returns the One-value in effect at T
                // (respecting the [valid_from, valid_to) interval); absent = current functional value.
                let valid_at = req["valid_at"].as_i64();
                Ok(match self.schema.cardinality.get(pname) {
                    Some(Cardinality::One) => {
                        let obj = match valid_at {
                            Some(at) => query::point_one_asof(&self.snap, subject, pid, at),
                            None => query::point_one(&self.snap, subject, pid),
                        };
                        // Provenance of the current functional value: the winning version's source
                        // name (omitted when unset, or for an as-of/historical read). Additive — the
                        // `one` shape is unchanged.
                        let provenance: Option<String> = (valid_at.is_none() && obj.is_some())
                            .then(|| {
                                query::point_one_source(&self.snap, subject, pid)
                                    .filter(|&src| src != 0)
                                    .and_then(|src| self.schema.cat.name(src))
                                    .map(str::to_string)
                            })
                            .flatten();
                        // A *current* One value (not an as-of read, value present) carries the
                        // additive confidence signals below; `obj` is consumed building `resp`.
                        let is_current = valid_at.is_none() && obj.is_some();
                        // A current read that came back absent — the only case that may carry
                        // `closed_from` below.
                        let is_current_absent = valid_at.is_none() && obj.is_none();
                        let mut resp = json!({ "one": obj.map(fmt_obj) });
                        if let Some(p) = provenance {
                            resp["provenance"] = json!(p);
                        }
                        // The winning version's valid_from (additive; a current value only — an
                        // ingest guard compares it against an incoming event's valid_from to detect
                        // late arrivals before writing).
                        if is_current
                            && let Some(vf) = query::point_one_valid_from(&self.snap, subject, pid)
                        {
                            resp["valid_from"] = json!(vf);
                        }
                        // The close boundary when the winning version is a close (additive; a
                        // current absent value only — never for an as-of read, and a never-written
                        // key stays exactly `{"one": null}`). Distinguishes "ended" from "never
                        // written" so a writer can defend the close during late-arrival repair.
                        if is_current_absent
                            && let Some(vf) = query::point_one_closed_from(&self.snap, subject, pid)
                        {
                            resp["closed_from"] = json!(vf);
                        }
                        // Coarse confidence for a *current* One value (additive; omitted for an
                        // as-of / absent read, so the shape is then identical to before). The raw
                        // signals (corroboration, sources, age) accompany the engine's default tier
                        // so a caller/policy layer can derive its own.
                        if is_current {
                            let now = req["now"].as_i64();
                            let max_age = req["max_age"].as_i64();
                            let c =
                                query::confidence_signals(&self.snap, subject, pid, now, max_age);
                            let mut conf = json!({
                                "tier": c.tier.as_str(),
                                "corroboration": c.corroboration,
                                "sources": c.corroboration,
                            });
                            if let Some(age) = c.age {
                                conf["age"] = json!(age);
                            }
                            resp["confidence"] = conf;
                        }
                        resp
                    }
                    _ => {
                        json!({ "many": query::point_many(&self.snap, subject, pid).into_iter().map(fmt_obj).collect::<Vec<_>>() })
                    }
                })
            }
            "expand" => {
                let subject = req["subject"].as_u64().ok_or("expand.subject missing")?;
                let pname = req["predicate"]
                    .as_str()
                    .ok_or("expand.predicate missing")?;
                let pid = self
                    .schema
                    .cat
                    .field_id(pname)
                    .ok_or(format!("unknown predicate: {pname}"))?;
                // Honor the predicate's declared relationship properties (symmetric / inverse /
                // transitive); `max_depth` bounds the transitive closure (default 16).
                let max_depth = req["max_depth"].as_u64().map(|d| d as usize).unwrap_or(16);
                Ok(
                    json!({ "nodes": query::expand_rel(&self.snap, &self.schema.cat, subject, pid, max_depth).into_iter().collect::<Vec<_>>() }),
                )
            }
            "search" => {
                let t = self.run_hybrid(req)?;
                Ok(
                    json!({ "ids": t.ids, "scores": t.scores, "as_of": { "changelog": t.as_of.changelog_seqno, "vector": t.as_of.vector_watermark } }),
                )
            }
            "edge_props" => {
                let subject = req["subject"]
                    .as_u64()
                    .ok_or("edge_props.subject missing")?;
                let pname = req["predicate"]
                    .as_str()
                    .ok_or("edge_props.predicate missing")?;
                let pid = self
                    .schema
                    .cat
                    .field_id(pname)
                    .ok_or(format!("unknown predicate: {pname}"))?;
                let object = obj_key(&req["object"])?;
                let props = query::edge_props(&self.snap, subject, pid, &object)
                    .map(|m| {
                        m.iter()
                            .map(|(k, v)| (k.clone(), fmt_obj(v.clone())))
                            .collect::<serde_json::Map<_, _>>()
                    })
                    .unwrap_or_default();
                Ok(json!({ "props": props }))
            }
            "retrieve_context" => self.retrieve_context(req),
            "neighborhood" => self.neighborhood(req),
            "node" => self.node_detail(req),
            "graph" => self.graph(req),
            "overview" => self.overview(req),
            "schema" => Ok(self.schema_view()),
            "pipeline" => self.pipeline(req),
            "conformance" => self.conformance(req),
            "completeness" => self.completeness(req),
            other => Err(format!("unknown op: {other}")),
        }
    }

    /// Distance-bounded subgraph around a focal node: BFS out to `hops` (default 2), following a
    /// given `predicate` or *all* node-valued edges (ontology view), authz-scoped, capped at
    /// `max_nodes` (default 3000). Returns `{nodes:[{id,depth}], edges:[[a,b]]}` — the primitive the
    /// UI's "distance from a node" filter renders.
    fn neighborhood(&self, req: &Value) -> DbResult<Value> {
        let focus = req["subject"].as_u64().ok_or("subject required")?;
        let hops = req["hops"].as_u64().unwrap_or(2) as usize;
        let cap = req["max_nodes"].as_u64().unwrap_or(3000) as usize;
        let labels = req["allowed_labels"]
            .as_u64()
            .map(|m| m as u32)
            .unwrap_or(u32::MAX);
        let pred = match req["predicate"].as_str() {
            Some(p) => Some(
                self.schema
                    .cat
                    .field_id(p)
                    .ok_or(format!("unknown predicate: {p}"))?,
            ),
            None => None,
        };
        let visible = |n: u64| {
            self.snap
                .node_labels
                .get(&n)
                .is_none_or(|&l| (labels >> l) & 1 == 1)
        };

        let mut depth: HashMap<u64, usize> = HashMap::new();
        let mut edges: BTreeSet<(u64, u64)> = BTreeSet::new();
        if !visible(focus) {
            return Ok(json!({ "nodes": [], "edges": [] }));
        }
        depth.insert(focus, 0);
        // undirected adjacency: reach both what the focus points to and what points at it.
        let adj = query::undirected_adjacency(&self.snap, pred);
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
            .map(|(&id, &d)| json!({ "id": id, "depth": d, "name": self.display_name(id) }))
            .collect();
        let strengths = query::edge_strengths(&self.snap, pred);
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
    fn node_detail(&self, req: &Value) -> DbResult<Value> {
        let subject = req["subject"].as_u64().ok_or("subject required")?;
        let labels = req["allowed_labels"]
            .as_u64()
            .map(|m| m as u32)
            .unwrap_or(u32::MAX);
        let visible = self
            .snap
            .node_labels
            .get(&subject)
            .is_none_or(|&l| (labels >> l) & 1 == 1);
        if !visible {
            return Ok(json!({ "id": subject, "denied": true }));
        }
        let (ones, manys) = query::describe(&self.snap, subject);
        let mut props = Vec::with_capacity(ones.len() + manys.len());
        for (p, ok) in ones {
            let name = self.schema.cat.name(p).unwrap_or("?");
            // provenance of this One value: the winning version's source name (omitted when unset)
            let source: Option<String> = query::point_one_source(&self.snap, subject, p)
                .filter(|&src| src != 0)
                .and_then(|src| self.schema.cat.name(src))
                .map(str::to_string);
            let mut prop = json!({ "predicate": name, "card": "one", "value": fmt_obj(ok) });
            if let Some(src) = source {
                prop["source"] = json!(src);
            }
            props.push(prop);
        }
        for (p, set) in manys {
            let name = self.schema.cat.name(p).unwrap_or("?");
            let vals: Vec<Value> = set.into_iter().map(fmt_obj).collect();
            props.push(json!({ "predicate": name, "card": "many", "values": vals }));
        }
        let ty = self
            .snap
            .node_types
            .get(&subject)
            .and_then(|&t| self.schema.cat.name(t))
            .map(|s| s.to_string());
        // the node's stored embedding, if any (so the console can show it carries a vector)
        let embedding: Option<Vec<f32>> = self
            .emb_ids
            .iter()
            .position(|&id| id == subject)
            .map(|i| self.emb[i * self.dim..(i + 1) * self.dim].to_vec());
        Ok(json!({
            "id": subject,
            "type": ty,
            "label": self.snap.node_labels.get(&subject).copied(),
            "props": props,
            "embedding": embedding,
            "dim": self.dim,
        }))
    }

    /// The schema vocabulary: registered predicates (name, cardinality, domain/range) and the set of
    /// node labels actually in use — so a client can discover what is queryable and which sensitivity
    /// labels exist, instead of guessing predicate names or bitmask values.
    fn schema_view(&self) -> Value {
        let mut preds: Vec<Value> = self
            .schema
            .cat
            .predicates()
            .map(|p| {
                let name = self.schema.cat.name(p.id).unwrap_or("?");
                let card = match p.cardinality {
                    Cardinality::One => "one",
                    Cardinality::Many => "many",
                };
                let domain = self.schema.cat.name(p.domain).map(|s| s.to_string());
                let range = match p.range {
                    Range::Type(t) => json!({ "type": self.schema.cat.name(t) }),
                    Range::Value(v) => json!({ "value": match v {
                        ValueType::Int => "int",
                        ValueType::Float => "float",
                        ValueType::Text => "text",
                        ValueType::Bool => "bool",
                    } }),
                };
                json!({ "name": name, "card": card, "domain": domain, "range": range })
            })
            .collect();
        preds.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        json!({ "predicates": preds, "labels": labels_in_use(&self.snap) })
    }

    /// Whole-graph view: every declared node and its node-valued edges, authz-scoped and capped at
    /// `max_nodes` (default 3000). Unlike `neighborhood` there is no focal distance — the result is
    /// the entire visible graph (or a `truncated` prefix when it exceeds the cap). Same
    /// `{nodes:[{id,depth}], edges:[[a,b]]}` shape so the UI renders it identically.
    fn graph(&self, req: &Value) -> DbResult<Value> {
        let cap = req["max_nodes"].as_u64().unwrap_or(3000) as usize;
        let labels = req["allowed_labels"]
            .as_u64()
            .map(|m| m as u32)
            .unwrap_or(u32::MAX);
        let visible = |n: u64| {
            self.snap
                .node_labels
                .get(&n)
                .is_none_or(|&l| (labels >> l) & 1 == 1)
        };

        let all: Vec<u64> = node_ids(&self.snap)
            .into_iter()
            .filter(|&n| visible(n))
            .collect();
        let truncated = all.len() > cap;
        let keep: BTreeSet<u64> = all.into_iter().take(cap).collect();

        let mut edges: BTreeSet<(u64, u64)> = BTreeSet::new();
        for &u in &keep {
            for v in query::neighbors(&self.snap, u) {
                if keep.contains(&v) {
                    edges.insert(if u < v { (u, v) } else { (v, u) });
                }
            }
        }
        let nodes: Vec<Value> = keep
            .iter()
            .map(|&id| json!({ "id": id, "depth": 0, "name": self.display_name(id) }))
            .collect();
        let strengths = query::edge_strengths(&self.snap, None);
        let edges: Vec<Value> = edges
            .iter()
            .map(|(a, b)| json!([a, b, strengths.get(&(*a, *b)).copied().unwrap_or(1)]))
            .collect();
        Ok(json!({ "nodes": nodes, "edges": edges, "truncated": truncated }))
    }

    /// Structural overview ("map"): one super-node per entity type — its member count and a sample
    /// member id — plus inter-type edges (how many node-valued edges cross each type pair). A cheap
    /// orientation view for graphs too large to render node-by-node; the UI sizes super-nodes by count
    /// and drills into a type by re-centring on its sample. Authz-scoped. Super-node id = the type's
    /// field id (unique within this response); `sample` carries the real node id to drill to.
    fn overview(&self, req: &Value) -> DbResult<Value> {
        const UNTYPED: u32 = u32::MAX;
        let labels = req["allowed_labels"]
            .as_u64()
            .map(|m| m as u32)
            .unwrap_or(u32::MAX);
        let visible = |n: u64| {
            self.snap
                .node_labels
                .get(&n)
                .is_none_or(|&l| (labels >> l) & 1 == 1)
        };
        let type_of = |n: u64| self.snap.node_types.get(&n).copied().unwrap_or(UNTYPED);

        let mut count: HashMap<u32, u64> = HashMap::new();
        let mut sample: HashMap<u32, u64> = HashMap::new();
        for n in node_ids(&self.snap).into_iter().filter(|&n| visible(n)) {
            let t = type_of(n);
            *count.entry(t).or_default() += 1;
            sample
                .entry(t)
                .and_modify(|s| *s = (*s).min(n))
                .or_insert(n);
        }

        // inter-type edge weights: count node-valued edges whose endpoints are different types
        let mut ew: HashMap<(u32, u32), u64> = HashMap::new();
        let bump = |a: u64, b: u64, ew: &mut HashMap<(u32, u32), u64>| {
            if !visible(a) || !visible(b) {
                return;
            }
            let (ta, tb) = (type_of(a), type_of(b));
            if ta != tb {
                let key = if ta < tb { (ta, tb) } else { (tb, ta) };
                *ew.entry(key).or_default() += 1;
            }
        };
        for (&(s, _), v) in self.snap.one.iter() {
            if let Some(ObjKey::Node(o)) = v {
                bump(s, *o, &mut ew);
            }
        }
        for (&(s, _), set) in self.snap.many.iter() {
            for o in set {
                if let ObjKey::Node(o) = o {
                    bump(s, *o, &mut ew);
                }
            }
        }

        let nodes: Vec<Value> = count
            .iter()
            .map(|(&t, &c)| {
                let name = if t == UNTYPED {
                    "(untyped)"
                } else {
                    self.schema.cat.name(t).unwrap_or("?")
                };
                json!({ "id": t, "depth": 0, "name": name, "count": c, "sample": sample[&t] })
            })
            .collect();
        let edges: Vec<Value> = ew.iter().map(|(&(a, b), &w)| json!([a, b, w])).collect();
        Ok(json!({ "nodes": nodes, "edges": edges, "overview": true }))
    }

    /// A node's display name — the value of its first `name`/`title`/`display_name`/`full_name`
    /// text predicate, if any. Used to label nodes in graph/neighbourhood results.
    fn display_name(&self, id: u64) -> Option<String> {
        const NAME_PREDS: [&str; 4] = ["name", "title", "display_name", "full_name"];
        self.snap
            .one
            .range((id, u32::MIN)..=(id, u32::MAX))
            .find_map(|(&(_, p), v)| match v {
                Some(ObjKey::Text(s))
                    if self
                        .schema
                        .cat
                        .name(p)
                        .is_some_and(|n| NAME_PREDS.contains(&n)) =>
                {
                    Some(s.clone())
                }
                _ => None,
            })
    }

    /// Shared type-aware hybrid search: builds the pipeline from a JSON request (`type`, `vector`,
    /// `k`, `allowed_labels`, `expand`, `mode`, `max_nodes`) and evaluates it, authz-scoped.
    fn run_hybrid(&self, req: &Value) -> DbResult<Traverser> {
        let ty = req["type"].as_str().ok_or("type missing")?;
        let tid = self
            .schema
            .cat
            .field_id(ty)
            .ok_or(format!("unknown type: {ty}"))?;
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
        let idx = (*self.index).as_ref().ok_or("no embeddings ingested")?;
        let mut transforms = Vec::new();
        if let Some(p) = req["expand"].as_str() {
            let pid = self
                .schema
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
        let head = self.durable_head;
        let vw = (self.emb_ids.len() as u64).min(head);
        let vv = VersionVector::new(head, vw);
        Ok(run(
            &self.snap,
            idx,
            &pipeline,
            &Principal {
                allowed_labels: labels,
            },
            vv,
        ))
    }

    /// Composable pipeline: surfaces the query IR as `source → steps → top-k`, so the console can let
    /// a user *chain* primitives. Source is `{nodes:[..]}` (identity), `{similar:{node,k}}` (that
    /// node's embedding as a type-ANN seed), or `{type_ann:{type,vector,k}}`. Steps are
    /// `{expand:"pred"}` (follow a predicate) or `{filter_type:"T"}` (keep a type). Authz-scoped.
    /// Returns `{ids, names}`.
    fn pipeline(&self, req: &Value) -> DbResult<Value> {
        let labels = req["allowed_labels"]
            .as_u64()
            .map(|m| m as u32)
            .unwrap_or(u32::MAX);
        // ---- source ----
        let src = &req["source"];
        let (source, needs_ann) = if let Some(nodes) = src["nodes"].as_array() {
            let subjects = nodes.iter().filter_map(|n| n.as_u64()).collect();
            (Source::Point { subjects }, false)
        } else if let Some(sim) = src.get("similar").filter(|v| !v.is_null()) {
            // seed from a node's own embedding (search within its type)
            let node = sim["node"].as_u64().ok_or("similar.node required")?;
            let k = sim["k"].as_u64().unwrap_or(10) as usize;
            let ty = self
                .snap
                .node_types
                .get(&node)
                .copied()
                .ok_or("that node has no type to search within")?;
            let pos = self
                .emb_ids
                .iter()
                .position(|&id| id == node)
                .ok_or("that node has no embedding to search by")?;
            let q = self.emb[pos * self.dim..(pos + 1) * self.dim].to_vec();
            (
                Source::TypeAnn {
                    q,
                    target_type: ty,
                    k,
                },
                true,
            )
        } else if let Some(ta) = src.get("type_ann").filter(|v| !v.is_null()) {
            let ty = ta["type"].as_str().ok_or("type_ann.type required")?;
            let tid = self
                .schema
                .cat
                .field_id(ty)
                .ok_or(format!("unknown type: {ty}"))?;
            let k = ta["k"].as_u64().unwrap_or(10) as usize;
            let q = ta["vector"]
                .as_array()
                .ok_or("type_ann.vector required")?
                .iter()
                .map(|x| x.as_f64().unwrap_or(0.0) as f32)
                .collect();
            (
                Source::TypeAnn {
                    q,
                    target_type: tid,
                    k,
                },
                true,
            )
        } else {
            return Err("source must be one of {nodes|similar|type_ann}".into());
        };
        // ---- steps ----
        let mut transforms = Vec::new();
        for step in req["steps"].as_array().into_iter().flatten() {
            if let Some(p) = step["expand"].as_str() {
                let pid = self
                    .schema
                    .cat
                    .field_id(p)
                    .ok_or(format!("unknown predicate: {p}"))?;
                transforms.push(Transform::Expand { predicate: pid });
            } else if let Some(t) = step["filter_type"].as_str() {
                let tid = self
                    .schema
                    .cat
                    .field_id(t)
                    .ok_or(format!("unknown type: {t}"))?;
                transforms.push(Transform::Filter(Filter::HasType { ty: tid }));
            } else {
                return Err("step must be {expand:..} or {filter_type:..}".into());
            }
        }
        let pipeline = Pipeline {
            source,
            transforms,
            max_nodes: req["max_nodes"].as_u64().unwrap_or(3000) as usize,
            mode: ReadMode::Fresh,
        };
        let head = self.durable_head;
        let vw = (self.emb_ids.len() as u64).min(head);
        let vv = VersionVector::new(head, vw);
        let principal = Principal {
            allowed_labels: labels,
        };
        let t = if needs_ann {
            let idx = (*self.index).as_ref().ok_or("no embeddings ingested")?;
            run(&self.snap, idx, &pipeline, &principal, vv)
        } else {
            run(&self.snap, &NoAnn, &pipeline, &principal, vv)
        };
        let nodes: Vec<Value> = t
            .ids
            .iter()
            .map(|&id| json!({ "id": id, "name": self.display_name(id) }))
            .collect();
        Ok(json!({ "ids": t.ids, "nodes": nodes }))
    }

    /// Evaluate a declared conformance rule into deterministic per-subject verdicts. The rule is given
    /// either inline as `req["rule"]` or by `req["rule_name"]` (a rule declared once via `rule_def` and
    /// stored in the registry) — exactly one is required. Predicate/type names are resolved against the
    /// catalog (unknown names are a clear error), then [`conformance::evaluate`] composes the existing
    /// read primitives into `OK | ABSENT | MISMATCH | NOT_APPLICABLE` verdicts, authz-scoped by
    /// `allowed_labels` (default all). A `MISMATCH` carries a `kind` of `"stale"` or `"wrong"`. Returns
    /// `{ "verdicts": [ { subject, verdict, kind, required, actual, as_of }, .. ] }`.
    fn conformance(&self, req: &Value) -> DbResult<Value> {
        let rule = if let Some(name) = req["rule_name"].as_str() {
            self.schema
                .rules
                .get(name)
                .cloned()
                .ok_or(format!("unknown rule_name: {name}"))?
        } else if !req["rule"].is_null() {
            parse_conformance_rule(&req["rule"])?
        } else {
            return Err(
                "conformance requires either 'rule' (inline) or 'rule_name' (stored)".into(),
            );
        };
        let missing = conformance::unresolved_names(&rule, &self.schema.cat);
        if !missing.is_empty() {
            return Err(format!(
                "unknown name(s) in conformance rule: {}",
                missing.join(", ")
            ));
        }
        let labels = req["allowed_labels"]
            .as_u64()
            .map(|m| m as u32)
            .unwrap_or(u32::MAX);
        let verdicts = conformance::evaluate(&self.snap, &self.schema.cat, &rule, labels);
        let out: Vec<Value> = verdicts
            .into_iter()
            .map(|v| {
                json!({
                    "subject": v.subject,
                    "verdict": v.verdict.as_str(),
                    "kind": v.mismatch_kind.map(|k| k.as_str()),
                    "required": v.required.map(fmt_obj),
                    "actual": v.actual.map(fmt_obj),
                    "as_of": v.as_of,
                })
            })
            .collect();
        Ok(json!({ "verdicts": out }))
    }

    /// Report, per node of a type, the schema-required predicates that are *absent* — the
    /// "expected-but-absent" completeness check. The `required` set is an explicit list of predicate
    /// names given in the request (`{"op":"completeness","type":"Issue","required":["assigned-to",..]}`);
    /// type + predicate names are resolved against the catalog (unknown names are a clear error), then
    /// [`completeness::evaluate`] reports, for each node of `type`, the required predicates with no
    /// value — deterministic (sorted by node id, missing list in request order) and authz-scoped by
    /// `allowed_labels` (default all). Nodes with every required predicate present are omitted. Returns
    /// `{ "incomplete": [ { "node": N, "missing": ["P", ..] }, .. ] }`.
    fn completeness(&self, req: &Value) -> DbResult<Value> {
        let type_name = req["type"].as_str().ok_or("completeness.type missing")?;
        let required_v = req["required"]
            .as_array()
            .ok_or("completeness.required must be an array of predicate names")?;
        let mut required: Vec<String> = Vec::with_capacity(required_v.len());
        for p in required_v {
            required.push(
                p.as_str()
                    .ok_or("completeness.required entries must be strings")?
                    .to_string(),
            );
        }
        let missing = completeness::unresolved_names(type_name, &required, &self.schema.cat);
        if !missing.is_empty() {
            return Err(format!(
                "unknown name(s) in completeness request: {}",
                missing.join(", ")
            ));
        }
        let labels = req["allowed_labels"]
            .as_u64()
            .map(|m| m as u32)
            .unwrap_or(u32::MAX);
        let incomplete: Vec<Value> =
            completeness::evaluate(&self.snap, &self.schema.cat, type_name, &required, labels)
                .into_iter()
                .map(|i| json!({ "node": i.node, "missing": i.missing }))
                .collect();
        Ok(json!({ "incomplete": incomplete }))
    }

    /// Assemble LLM-ready context from a hybrid search: for each hit, the *current* value of the
    /// `content` predicate plus a calendar-framed stamp of its `date` predicate (weekday, days
    /// relative to `as_of`, business-hours, fiscal quarter), ordered oldest→newest. An optional
    /// calendar frame (`tz_offset_min`, `business_start_min`, `business_end_min`,
    /// `fiscal_year_start_month`) shapes the stamps. Returns `{context, hits, as_of}`.
    fn retrieve_context(&self, req: &Value) -> DbResult<Value> {
        let content_p = self
            .schema
            .cat
            .field_id(
                req["content"]
                    .as_str()
                    .ok_or("content predicate required")?,
            )
            .ok_or("unknown content predicate")?;
        let date_p = match req["date"].as_str() {
            Some(d) => Some(
                self.schema
                    .cat
                    .field_id(d)
                    .ok_or("unknown date predicate")?,
            ),
            None => None,
        };
        let cal = Calendar {
            utc_offset_min: req["tz_offset_min"].as_i64().unwrap_or(0) as i32,
            business_start_min: req["business_start_min"].as_u64().unwrap_or(540) as u32,
            business_end_min: req["business_end_min"].as_u64().unwrap_or(1080) as u32,
            fiscal_year_start_month: req["fiscal_year_start_month"].as_u64().unwrap_or(1) as u32,
        };

        let t = self.run_hybrid(req)?;
        // gather (node, score, date, content) — current values (fold LWW resolves supersession)
        let mut rows: Vec<(u64, f32, Option<i64>, String)> = t
            .ids
            .iter()
            .zip(&t.scores)
            .map(|(&n, &sc)| {
                let content = match query::point_one(&self.snap, n, content_p) {
                    Some(ObjKey::Text(s)) => s,
                    _ => String::new(),
                };
                let date = date_p.and_then(|dp| match query::point_one(&self.snap, n, dp) {
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

/// All declared node ids (union of typed and labelled nodes), sorted ascending — the source for a
/// whole-graph view. Sorted because a `HashMap`'s iteration order is unspecified, but the
/// graph/overview views expect a stable, ordered node set.
fn node_ids(snap: &Snapshot) -> Vec<NodeId> {
    let mut s: BTreeSet<NodeId> = snap.node_types.keys().copied().collect();
    s.extend(snap.node_labels.keys().copied());
    s.into_iter().collect()
}

/// The distinct ABAC sensitivity labels actually assigned to nodes, sorted ascending.
fn labels_in_use(snap: &Snapshot) -> Vec<u8> {
    snap.node_labels
        .values()
        .copied()
        .collect::<BTreeSet<u8>>()
        .into_iter()
        .collect()
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

fn apply_def(schema: &mut Schema, v: &Value) -> DbResult<()> {
    // A `source_def` records a provenance source name interned during fact ingest (never sent by a
    // client — `WriteState::source_id` appends it). Replaying it here, interleaved with the type/pred
    // defs in `schema.jsonl`, re-interns it in the same order so the numeric `source` the WAL stores
    // resolves back to this name after a reopen.
    if let Some(sd) = v.get("source_def") {
        schema
            .cat
            .intern_ref(sd["name"].as_str().ok_or("source_def.name missing")?);
        return Ok(());
    }
    if let Some(t) = v.get("type_def") {
        schema
            .cat
            .register_type(t["name"].as_str().ok_or("type_def.name missing")?);
        return Ok(());
    }
    if let Some(p) = v.get("pred_def") {
        let name = p["name"].as_str().ok_or("pred_def.name missing")?;
        let c = match p["cardinality"].as_str().unwrap_or("many") {
            "one" => Cardinality::One,
            _ => Cardinality::Many,
        };
        // A predicate's cardinality is load-bearing: existing facts were folded as One or Many under
        // it. Redefining it with a different cardinality would make later writes conflict with the
        // folded state, so reject it here with a clear error rather than letting the fold panic.
        // Re-sending the same definition (same cardinality) is idempotent and allowed.
        if let Some(&existing) = schema.cardinality.get(name)
            && existing != c
        {
            return Err(format!(
                "predicate '{name}' is already defined with cardinality {existing:?}; it cannot be redefined as {c:?}"
            ));
        }
        let domain = p["domain"].as_str().ok_or("pred_def.domain missing")?;
        let domain_id = schema
            .cat
            .field_id(domain)
            .ok_or(format!("unknown domain type: {domain}"))?;
        let range = if let Some(rt) = p["range"].as_str() {
            Range::Type(
                schema
                    .cat
                    .field_id(rt)
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
        // Declared relationship properties, evaluated at query time by `expand` (never materialized).
        // `inverse` names another predicate; intern it to a stable Field-ID even if that predicate's
        // own pred_def has not arrived yet (a forward reference), so declaration order does not matter.
        let symmetric = p["symmetric"].as_bool().unwrap_or(false);
        let transitive = p["transitive"].as_bool().unwrap_or(false);
        let inverse = p
            .get("inverse")
            .and_then(|x| x.as_str())
            .map(|inv| schema.cat.intern_ref(inv));
        let props = RelProps {
            symmetric,
            transitive,
            inverse,
        };
        schema
            .cat
            .register_predicate(name, c, props, domain_id, range);
        schema.cardinality.insert(name.to_string(), c);
        return Ok(());
    }
    Err("schema line must be type_def, pred_def, or source_def".into())
}

/// Register a named conformance rule from a `{"rule_def":{"name":..,"rule":{..}}}` line into the
/// registry. The rule is parsed structurally here (via [`parse_conformance_rule`]); its predicate/
/// type names are resolved against the catalog only at evaluation, so a rule may be declared before
/// the predicates it references. Re-declaring a name replaces the stored rule.
fn apply_rule_def(schema: &mut Schema, v: &Value) -> DbResult<()> {
    let rd = v.get("rule_def").ok_or("rule line must be a rule_def")?;
    let name = rd["name"]
        .as_str()
        .ok_or("rule_def.name missing")?
        .to_string();
    let rule = parse_conformance_rule(&rd["rule"])?;
    schema.rules.insert(name, rule);
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

/// A bare JSON scalar → value ObjKey, for edge-property values (`{"level": 5, "role": "lead"}`).
fn value_key(v: &Value) -> DbResult<ObjKey> {
    match v {
        Value::Bool(b) => Ok(ObjKey::Bool(*b)),
        Value::String(s) => Ok(ObjKey::Text(s.clone())),
        Value::Number(n) if n.is_i64() => Ok(ObjKey::Int(n.as_i64().unwrap())),
        Value::Number(n) if n.is_u64() => Ok(ObjKey::Int(n.as_u64().unwrap() as i64)),
        Value::Number(n) => Ok(ObjKey::Float((n.as_f64().unwrap() as f32 as f64).to_bits())),
        _ => Err("edge-property value must be a number, string, or bool".into()),
    }
}

/// Parse a conformance rule from its JSON declaration into the name-based [`conformance::Rule`]
/// (names are resolved to field ids later, at evaluation, via the catalog).
fn parse_conformance_rule(v: &Value) -> DbResult<conformance::Rule> {
    let subject_type = v["subject_type"]
        .as_str()
        .ok_or("rule.subject_type missing")?
        .to_string();
    let actual = v["actual"]
        .as_str()
        .ok_or("rule.actual missing")?
        .to_string();
    let hops_v = v["required"]["hops"]
        .as_array()
        .ok_or("rule.required.hops missing")?;
    let mut required = Vec::with_capacity(hops_v.len());
    for h in hops_v {
        let predicate = h["predicate"]
            .as_str()
            .ok_or("rule.required hop predicate missing")?
            .to_string();
        let as_of = h.get("as_of").and_then(|a| a.as_str()).map(str::to_string);
        required.push(conformance::Hop { predicate, as_of });
    }
    Ok(conformance::Rule {
        subject_type,
        scope: parse_conformance_cond(&v["scope"])?,
        required,
        actual,
        absent_when: parse_conformance_cond(&v["absent_when"])?,
    })
}

/// Parse an optional `{ "predicate": name, "equals": scalar }` condition (absent/null → `None`).
fn parse_conformance_cond(v: &Value) -> DbResult<Option<conformance::Cond>> {
    if v.is_null() {
        return Ok(None);
    }
    let predicate = v["predicate"]
        .as_str()
        .ok_or("conformance condition predicate missing")?
        .to_string();
    let equals = value_key(&v["equals"])?;
    Ok(Some(conformance::Cond { predicate, equals }))
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
