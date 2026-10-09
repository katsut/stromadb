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
//!   wal.log.snap     compaction snapshot: the full fold state (superseded rows included — as-of
//!                    reads keep answering) as of a covered seqno; cold-start = snapshot + tail
//!   wal.log.archive-<S>  the WAL prefix a compaction truncated (kept for audit; uncompressed v1)
//!   schema.jsonl     type/predicate definitions, replayed in order (Field-ID interning is
//!                    order-deterministic, so ids are stable across restarts)
//!   rules.jsonl      named conformance rules (`rule_def`), replayed in order into the rule registry
//!   nodes.jsonl      node type/label assignments (audit mirror of what was ingested; the authority
//!                    is the WAL ops, which the recovered snapshot carries — not replayed)
//!   embeddings.bin   received embeddings, flat f32 LE; embeddings.ids = u64 LE per row
//!   meta.json        { "dim": N }
//!   counts.json      { "nodes": N, "facts": H } — derived listing hint (not an input): rewritten
//!                    on open, close, compaction, reset and after each ingest; read by
//!                    [`persisted_counts`] to report a database without opening it
//!   LOCK             advisory lock file guarding the directory against concurrent opens; holds
//!                    the owning pid (informational — the flock, not the content, is the guard)
//!
//! Record formats (JSONL) — ingest: type_def / pred_def / rule_def / node / fact / retract / close;
//! embed: {node,vector}.
//! Query request (JSON): {"op":"point"|"expand"|"search", ...} — see [`Db::query`].

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::Instant;

/// Process-lifetime anchor for `/stats` uptime — touched on the first `Db::open` so uptime tracks
/// how long the database has been serving, not when stats was first asked.
static START: LazyLock<Instant> = LazyLock::new(Instant::now);

use serde_json::{Value, json};
use stromadb_core::calendar::Calendar;
use stromadb_core::catalog::{Cardinality, Catalog, Range, RelProps, ValueType};
use stromadb_core::changelog::WriteKind;
use stromadb_core::completeness;
use stromadb_core::conformance;
use stromadb_core::engine::Engine;
use stromadb_core::fact::{FieldId, NodeId};
use stromadb_core::fold::{ObjKey, Snapshot};
use stromadb_core::incremental::{MaintainedConformance, VerdictDiff};
use stromadb_core::ir::{Filter, NoAnn, Pipeline, Principal, Source, Transform, Traverser, run};
use stromadb_core::ivf::IvfPq;
use stromadb_core::mask::{self, FactMask, Facts, Masked};
use stromadb_core::query;
use stromadb_core::version::{ReadMode, VersionVector};

/// Shared MCP tool schemas + JSON-RPC dispatch (used by the stdio binary and the serve endpoint).
pub mod mcp;

/// The change feed: a bounded journal of what each durable batch touched, read after authz.
pub mod feed;
pub use feed::{Feed, VerdictFeed};

pub type DbResult<T> = Result<T, String>;

/// What a compaction did — the covered seqno and the resulting on-disk footprint.
#[derive(Debug, Clone, Copy)]
pub struct CompactionStats {
    pub covered: u64,
    pub wal_bytes: u64,
    pub snapshot_bytes: u64,
}

/// Counts from an ingest batch. `facts` counts fact writes actually appended — a re-assertion
/// identical to current state is skipped and counted in `suppressed` instead. `retracts` counts
/// only retracts that removed a present edge (an absent-edge retract is a no-op); `closes` counts
/// `close` records appended (a duplicate close is suppressed); `suppressed` counts incoming writes
/// (facts, closes, edge-prop sets) skipped as no-ops.
///
/// `head_before` is the durable head when the batch took the write lock and `durable_head` the
/// head after it. Batches are serialized, so `(head_before, durable_head]` is exactly the range of
/// heads this batch wrote — the range a client bounds `conformance_changes` with to attribute
/// verdict changes to this batch.
#[derive(Debug, Default, Clone, Copy)]
pub struct IngestStats {
    pub defs: u64,
    pub nodes: u64,
    pub facts: u64,
    pub retracts: u64,
    pub closes: u64,
    pub suppressed: u64,
    pub head_before: u64,
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
    rules: HashMap<String, StoredRule>,
    /// Provenance source ids registered via `source_def` — the inventory of writers that have ever
    /// stamped facts (surfaced by `/stats`; the interner alone cannot tell sources from other names).
    sources: BTreeSet<FieldId>,
}

/// A stored conformance rule: the parsed form the evaluator runs, plus the declaration exactly as
/// it was given in its `rule_def` line, which the `rule` op returns.
#[derive(Clone)]
struct StoredRule {
    rule: conformance::Rule,
    definition: Value,
}

/// A directory-backed database. Reads are lock-free over a pinned [`ReadState`]; writes hold the
/// `write` mutex for the ETL and then publish a fresh read view.
pub struct Db {
    write: Mutex<WriteState>,
    read: RwLock<Arc<ReadState>>,
    /// The change feed journal, shared with the write path that fills it. Its own lock is held
    /// only to append a batch or copy out a read, so a feed read never waits for a write.
    feed: Arc<Mutex<feed::Journal>>,
    /// Exclusive advisory lock on `<dir>/LOCK`, held for the lifetime of this handle so a second
    /// process (or a second open in this process) cannot replay/append the same directory. The OS
    /// releases the lock when the handle is dropped or the process exits.
    _lock: DirLock,
}

/// Holder of the data directory's `LOCK` file (see [`Db`]). On non-unix targets the guard is a
/// no-op — the field only exists to keep the file (and its flock) alive on unix.
struct DirLock {
    #[cfg(unix)]
    _file: fs::File,
}

/// Take a non-blocking exclusive advisory lock on `<dir>/LOCK` and stamp the current pid into it.
/// Fails fast with the holder's pid when another process already owns the directory. MSRV 1.88
/// predates `File::try_lock` (stabilized 1.89), so this calls `flock` through libc on unix; other
/// targets get a no-op fallback.
fn lock_dir(dir: &Path) -> DbResult<DirLock> {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let path = dir.join("LOCK");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| format!("open LOCK: {e}"))?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            // Best effort: the holder wrote its pid into the file on acquiring the lock.
            let holder = fs::read_to_string(&path)
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok());
            return Err(match holder {
                Some(pid) => format!(
                    "data directory {} is in use by another process (pid {pid})",
                    dir.display()
                ),
                None => format!(
                    "data directory {} is in use by another process",
                    dir.display()
                ),
            });
        }
        file.set_len(0).map_err(|e| format!("LOCK: {e}"))?;
        writeln!(&file, "{}", std::process::id()).map_err(|e| format!("LOCK: {e}"))?;
        Ok(DirLock { _file: file })
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(DirLock {})
    }
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
    /// Write-side node→type mirror, kept solely so a re-sent node record with unchanged type and
    /// label can be suppressed without consulting the (possibly stale) read snapshot.
    node_type_w: HashMap<NodeId, FieldId>,
    /// Since-boot total of suppressed no-op writes (facts, closes, props, nodes) — /stats observability.
    suppressed_total: u64,
    /// Maintained headline counters, kept equal to the published snapshot by every node write so
    /// `/stats` never scans: distinct nodes carrying a type or a label (`|types ∪ labels|`), and
    /// the node count per ABAC label. Node attributes are never removed, so both only grow or move.
    node_count: u64,
    label_counts: BTreeMap<u8, u64>,
    /// What `counts.json` last recorded (see [`WriteState::persist_counts`]).
    counts_persisted: Option<Counts>,
    /// Received embeddings, `Arc`-shared with the read view; appended (copy-on-write) by `embed`.
    emb_ids: Arc<Vec<u64>>,
    emb: Arc<Vec<f32>>,
    dim: usize,
    index: Arc<Option<IvfPq>>,
    /// Whether the last index build retrained the quantizers (vs reusing them) — /stats observability.
    index_retrained: bool,
    /// Stored rules under live maintenance (`conformance_watch`): the maintained verdict map plus a
    /// bounded diff journal, fed by every tail drain (see [`WriteState::materialize_live`]).
    live_rules: HashMap<String, LiveRule>,
    /// Per-batch default provenance ([`Db::ingest_str_as`]): stamped on fact/retract/close lines
    /// that carry no `source` of their own. Set for the duration of one ingest call, then cleared.
    default_source: Option<String>,
    /// The change feed journal (shared with [`Db`]) and what this batch noted for it so far.
    feed: Arc<Mutex<feed::Journal>>,
    feed_pending: feed::Pending,
    n_max: usize,
}

/// Diff-journal entries retained per watched rule; a cursor older than the retained window gets a
/// `resync` answer instead of silently missing changes.
const LIVE_LOG_CAP: usize = 1024;

/// What [`Db::rule_changes`] read: the visible verdict changes of the rules whose journal covers
/// the cursor, and the names of those it does not.
struct RuleChanges {
    /// The durable head at the time of the read.
    head: u64,
    /// The head the answer is complete up to (`until` capped at `head`).
    upper: u64,
    changes: Vec<Value>,
    lagging: Vec<String>,
}

/// A stored conformance rule under live maintenance.
struct LiveRule {
    maintained: MaintainedConformance,
    /// `(durable head after the batch, that batch's verdict changes)`, oldest first.
    log: VecDeque<(u64, Vec<VerdictDiff>)>,
    /// Heads at or below this may have been dropped from the journal — cursors behind it resync.
    truncated_to: u64,
}

impl LiveRule {
    fn push(&mut self, head: u64, diffs: Vec<VerdictDiff>) {
        if self.log.len() >= LIVE_LOG_CAP
            && let Some((h, _)) = self.log.pop_front()
        {
            self.truncated_to = h;
        }
        self.log.push_back((head, diffs));
    }
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
    /// `/stats` counters captured at publish time, so a stats read is O(1) and never takes the
    /// write lock (it would otherwise wait out a whole ingest batch, index rebuild included).
    node_count: u64,
    label_counts: BTreeMap<u8, u64>,
    unmerged: usize,
    suppressed_total: u64,
    index_retrained: bool,
    wal_bytes: u64,
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
        // Guard the directory before touching any of its files: exactly one live handle (across
        // processes) may replay the WAL and append to it.
        let lock = lock_dir(dir)?;
        LazyLock::force(&START); // anchor /stats uptime at first open
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
        let node_type_w: HashMap<NodeId, FieldId> =
            snap.node_types.iter().map(|(&k, &v)| (k, v)).collect();
        // seed the maintained counters once from the recovered state; writes keep them current
        let node_count = node_ids(&snap).len() as u64;
        let mut label_counts: BTreeMap<u8, u64> = BTreeMap::new();
        for &l in node_label_w.values() {
            *label_counts.entry(l).or_insert(0) += 1;
        }
        let journal = Arc::new(Mutex::new(feed::Journal::new(eng.durable_head())));
        let mut w = WriteState {
            dir: dir.to_path_buf(),
            eng,
            schema: Arc::new(schema),
            node_label_w,
            node_type_w,
            suppressed_total: 0,
            node_count,
            label_counts,
            counts_persisted: None,
            emb_ids: Arc::new(emb_ids),
            emb: Arc::new(emb),
            dim,
            index: Arc::new(None),
            index_retrained: false,
            live_rules: HashMap::new(),
            default_source: None,
            feed: journal.clone(),
            feed_pending: feed::Pending::default(),
            n_max,
        };
        w.rebuild_index();
        w.persist_counts();
        let rs = Arc::new(w.build_read_state());
        Ok(Db {
            write: Mutex::new(w),
            read: RwLock::new(rs),
            feed: journal,
            _lock: lock,
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
        // compaction siblings too — a surviving snapshot/archive would resurrect the old state on
        // the next open (the snapshot IS authoritative input once the WAL prefix is truncated)
        if let Ok(entries) = fs::read_dir(&w.dir) {
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                if name.starts_with("wal.log.") {
                    fs::remove_file(e.path())
                        .map_err(|err| format!("reset: remove {name}: {err}"))?;
                }
            }
        }
        Self::init(&w.dir)?;
        let eng = Engine::open(w.dir.join("wal.log"), w.n_max)
            .map_err(|e| format!("reset: open: {e}"))?;
        w.eng = eng;
        w.schema = Arc::new(Schema::default());
        w.node_label_w.clear();
        w.node_type_w.clear();
        w.node_count = 0;
        w.label_counts.clear();
        w.emb_ids = Arc::new(Vec::new());
        w.emb = Arc::new(Vec::new());
        w.dim = 0;
        w.index = Arc::new(None);
        w.index_retrained = false;
        w.live_rules.clear();
        w.feed_pending.clear();
        w.feed.lock().unwrap_or_else(|e| e.into_inner()).reset();
        w.persist_counts();
        self.publish(&w);
        Ok(())
    }

    /// Snapshot + truncate the changelog (see `Engine::compact`): the full fold state — superseded
    /// rows included, as-of reads keep answering across the boundary — is persisted as
    /// `wal.log.snap`, the covered WAL is archived, and cold-start replay becomes snapshot-load +
    /// tail. Non-destructive (unlike reset) but heavyweight; explicitly invoked, no automatic
    /// trigger. Returns the covered seqno plus the resulting file sizes for observability.
    pub fn compact(&self) -> DbResult<CompactionStats> {
        let mut w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        // engine compact drains the tail internally — route any remaining tail through the live
        // feed first so watched rules never miss a touch
        w.materialize_live();
        let covered = w.eng.compact().map_err(|e| format!("compact: {e}"))?;
        let wal = w.dir.join("wal.log");
        let size = |p: &std::path::Path| fs::metadata(p).map(|m| m.len()).unwrap_or(0);
        let stats = CompactionStats {
            covered,
            wal_bytes: size(&wal),
            snapshot_bytes: size(&w.dir.join("wal.log.snap")),
        };
        // the WAL shrank: republish so the cached storage counter reflects it
        w.persist_counts();
        self.publish(&w);
        Ok(stats)
    }

    /// Ingest a JSONL batch (type_def / pred_def / rule_def / node / fact / retract / close).
    /// Durable on return; the updated read view is published atomically before this returns.
    pub fn ingest_str(&self, jsonl: &str) -> DbResult<IngestStats> {
        self.ingest_str_as(jsonl, None)
    }

    /// [`Db::ingest_str`] with a default provenance: facts (and retracts/closes) whose lines carry
    /// no `source` are stamped with `default_source` — how a serving layer records *which client*
    /// asserted a write. An explicit per-line `source` always wins; `None` = today's behavior.
    pub fn ingest_str_as(
        &self,
        jsonl: &str,
        default_source: Option<&str>,
    ) -> DbResult<IngestStats> {
        let mut w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        w.default_source = default_source.map(str::to_string);
        let s = w.ingest(jsonl);
        w.default_source = None;
        let s = s?;
        w.persist_counts();
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
    ///   `{"one":..}` or `{"many":[..]}` (`valid_at` = valid-time as-of read: for a One-predicate
    ///   the value in effect at instant `T`; for a Many-predicate the elements in effect at `T`,
    ///   with `"valid_at"` echoed so a client can detect the capability). A *current* One answer
    ///   also carries the winning version's `"valid_from"` and an additive `"confidence"`
    ///   `{tier, corroboration, sources[, age]}` — a coarse tier plus its raw signals; both omitted
    ///   for an as-of / absent read. `now`/`max_age` supply the freshness reference
    ///   (`age = now - valid_from`; stale when `age > max_age`). A current One answer whose winning
    ///   version is a *close* carries `"closed_from"` (the close's `valid_from`) next to
    ///   `"one": null`; never for an as-of read or a never-written key.
    /// - `{"op":"expand","subject":N,"predicate":"name"[,"max_depth":D][,"valid_at":T]}` →
    ///   `{"nodes":[..]}` (honors the predicate's declared props — symmetric / inverse /
    ///   transitive; `max_depth` bounds the transitive closure, default 16; with `valid_at` every
    ///   hop answers from the state in effect at `T`, echoed back)
    /// - `{"op":"edge_props","subject":N,"predicate":"name","object":{..}}` → `{"props":{k:v,..}}`
    ///   (properties on the edge `(subject, predicate, object)`; set at ingest via a fact's `props`)
    /// - `{"op":"search","type":"T","vector":[..],"k":K,"allowed_labels":M,"expand":"pred","mode":"fresh|strict"}`
    ///   → `{"ids":[..],"scores":[..],"as_of":{..}}`
    /// - `{"op":"lookup","predicate":"name","value":V[,"type":"T"][,"limit":L][,"valid_at":T]}` →
    ///   `{"nodes":[{id,type,display}],"truncated":bool}` (exact match on a one-cardinality value;
    ///   the external key → node id read); batched as `"queries":[{"value":V[,"valid_at":T]},..]`
    ///   or `"values":[V,..]` → `{"results":[..]}`, one single-form answer per item in order
    pub fn query(&self, req: &Value) -> DbResult<Value> {
        // The two live-maintenance ops run against the write-side registry (they mutate / read the
        // maintained state under the write lock); everything else is a lock-free read on the
        // pinned view.
        match req["op"].as_str() {
            Some("conformance_watch") => return self.conformance_watch(req),
            Some("conformance_changes") => return self.conformance_changes(req),
            _ => {}
        }
        let rs = self.read_state();
        rs.query(req)
    }

    /// Put a stored rule under live maintenance (idempotent) and return its full current verdicts
    /// plus a `cursor`. From then on every ingest keeps the verdict map current incrementally
    /// (O(touched), not O(subjects)) and journals the changes; poll them with
    /// `conformance_changes`. The watch is in-memory: re-watch after a reopen.
    fn conformance_watch(&self, req: &Value) -> DbResult<Value> {
        let name = req["rule_name"]
            .as_str()
            .ok_or("conformance_watch.rule_name missing (only stored rules can be watched)")?;
        if !req["assume"].is_null() {
            return Err(
                "conformance_watch maintains graph-only verdicts; evaluate 'assume' with the conformance op"
                    .into(),
            );
        }
        let labels = req_labels(req);
        let mut w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        w.materialize_live(); // seed from a fully drained state
        let rule = w
            .schema
            .rules
            .get(name)
            .map(|s| s.rule.clone())
            .ok_or(format!("unknown rule_name: {name}"))?;
        let missing = conformance::unresolved_names(&rule, &w.schema.cat);
        if !missing.is_empty() {
            return Err(format!(
                "unknown name(s) in conformance rule: {}",
                missing.join(", ")
            ));
        }
        check_rule_paths(&rule, &w.schema.cat)?;
        let head = w.eng.durable_head();
        if !w.live_rules.contains_key(name) {
            let snap = w.eng.snapshot_arc();
            let maintained = MaintainedConformance::new(rule, &snap, &w.schema.cat);
            w.live_rules.insert(
                name.to_string(),
                LiveRule {
                    maintained,
                    log: VecDeque::new(),
                    truncated_to: head,
                },
            );
        }
        let snap = w.eng.snapshot_arc();
        // maintenance is unfiltered: the caller's node labels drop subjects, and its fact labels
        // withhold verdicts whose support set touches a hidden fact (`hidden_by_label`)
        let mask = FactMask::new(&w.schema.cat, labels);
        let masking = mask.hides_any(&snap);
        let maintained = &w.live_rules[name].maintained;
        let verdicts: Vec<Value> = maintained
            .verdicts()
            .values()
            .filter(|v| label_visible(&snap, v.subject, labels))
            .map(|v| {
                let v = if masking {
                    let support = maintained.support(v.subject);
                    conformance::mask_verdict(v.clone(), &support, &mask)
                } else {
                    v.clone()
                };
                verdict_json(&conformance::mask_missing_nodes(v, &snap, labels))
            })
            .collect();
        Ok(json!({ "verdicts": verdicts, "cursor": head }))
    }

    /// The verdict changes of a watched rule since `cursor` (a head returned by `conformance_watch`
    /// or a previous call), plus the new cursor. When the cursor has fallen behind the bounded
    /// journal, answers `{"resync": true}` — re-watch (or take the full verdicts) instead of
    /// trusting a gap.
    ///
    /// Every change carries the `head` of the batch that caused it. An optional `until` (inclusive)
    /// bounds the answer to `cursor < head <= until` and becomes the returned cursor (capped at the
    /// current head), so a client can read exactly one ingest's changes with
    /// `cursor = head_before, until = durable_head`. Without `rule_name` the answer covers every
    /// watched rule, ordered by head then rule name, and each change names its `rule`; it resyncs
    /// when the cursor is behind any of their journals.
    fn conformance_changes(&self, req: &Value) -> DbResult<Value> {
        let name = req["rule_name"].as_str();
        let cursor = req["cursor"]
            .as_u64()
            .ok_or("conformance_changes.cursor missing")?;
        let until = match &req["until"] {
            Value::Null => None,
            v => Some(
                v.as_u64()
                    .ok_or("conformance_changes.until must be a head (unsigned integer)")?,
            ),
        };
        if let Some(u) = until
            && u < cursor
        {
            return Err(format!(
                "conformance_changes.until ({u}) is below cursor ({cursor})"
            ));
        }
        let r = self.rule_changes(name, cursor, until, req_labels(req))?;
        if !r.lagging.is_empty() {
            return Ok(json!({ "resync": true, "cursor": r.head }));
        }
        Ok(json!({ "changes": r.changes, "cursor": r.upper }))
    }

    /// The verdict changes of every watched rule with `since < head <= until`, in head order (rules
    /// by name within a head), as a principal with `allowed_labels` sees them — the same rows and
    /// masking as `conformance_changes` without a rule name. A rule whose journal no longer reaches
    /// back to `since` is named in `resync` and contributes no changes; the other rules still do.
    pub fn verdict_changes(
        &self,
        since: u64,
        until: u64,
        allowed_labels: u32,
    ) -> DbResult<VerdictFeed> {
        if until < since {
            return Err(format!("until ({until}) is below since ({since})"));
        }
        let r = self.rule_changes(None, since, Some(until), allowed_labels)?;
        Ok(VerdictFeed {
            changes: r.changes,
            resync: r.lagging,
        })
    }

    /// The shared read behind `conformance_changes` and [`Db::verdict_changes`]: the changes of
    /// the watched rule `name` (every watched rule when `None`) in `(cursor, upper]`, where
    /// `upper` is `until` capped at the durable head. Rules whose journal starts after `cursor`
    /// are listed in `lagging` and skipped.
    fn rule_changes(
        &self,
        name: Option<&str>,
        cursor: u64,
        until: Option<u64>,
        labels: u32,
    ) -> DbResult<RuleChanges> {
        let mut w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        w.materialize_live(); // usually a no-op: ingest drains on return
        let head = w.eng.durable_head();
        let upper = until.map_or(head, |u| u.min(head));
        let snap = w.eng.snapshot_arc();
        let rules: Vec<(&str, &LiveRule)> = match name {
            Some(name) => {
                let Some(lr) = w.live_rules.get(name) else {
                    return Err(format!(
                        "rule not watched: {name} — call conformance_watch first"
                    ));
                };
                vec![(name, lr)]
            }
            None => {
                let mut all: Vec<(&str, &LiveRule)> = w
                    .live_rules
                    .iter()
                    .map(|(n, lr)| (n.as_str(), lr))
                    .collect();
                all.sort_unstable_by_key(|(n, _)| *n);
                all
            }
        };
        let (lagging, rules): (Vec<_>, Vec<_>) = rules
            .into_iter()
            .partition(|(_, lr)| cursor < lr.truncated_to);
        let lagging: Vec<String> = lagging.into_iter().map(|(n, _)| n.to_string()).collect();
        // Each side of a change is masked with the support it was judged from, labels as of that
        // judgment. The journal also holds entries whose verdict did not change but whose read
        // rows changed labels; an entry that looks the same on both sides through this caller's
        // mask is dropped.
        let mask = FactMask::new(&w.schema.cat, labels);
        let masking = mask.hides_any(&snap);
        let seen = |v: &Option<conformance::Verdict>, support: &conformance::Support| {
            v.clone().map(|v| {
                let v = if masking {
                    conformance::mask_verdict(v, support, &mask)
                } else {
                    v
                };
                conformance::mask_missing_nodes(v, &snap, labels)
            })
        };
        let mut entries: Vec<(u64, &str, &VerdictDiff)> = rules
            .iter()
            .flat_map(|(rule, lr)| {
                lr.log
                    .iter()
                    .filter(|(h, _)| *h > cursor && *h <= upper)
                    .flat_map(move |(h, diffs)| diffs.iter().map(move |d| (*h, *rule, d)))
            })
            .collect();
        // stable: within one head, rules stay in name order and a rule's diffs in journal order
        entries.sort_by_key(|(h, _, _)| *h);
        let changes: Vec<Value> = entries
            .into_iter()
            .filter(|(_, _, d)| label_visible(&snap, d.subject, labels))
            .filter_map(|(h, rule, d)| {
                let (old, new) = (seen(&d.old, &d.old_support), seen(&d.new, &d.new_support));
                (old != new).then(|| {
                    let mut c = json!({
                        "subject": d.subject,
                        "old": old.as_ref().map(verdict_json),
                        "new": new.as_ref().map(verdict_json),
                        "head": h,
                    });
                    if name.is_none() {
                        c["rule"] = json!(rule);
                    }
                    c
                })
            })
            .collect();
        Ok(RuleChanges {
            head,
            upper,
            changes,
            lagging,
        })
    }

    /// The change feed after cursor `since` (a durable head) as a principal with `allowed_labels`
    /// sees it: per batch, the nodes it touched with their type and the visible predicates written
    /// on them, and whether each node is new. Answers up to the current read view's head, with
    /// node labels and per-fact labels (D35) applied to that view; never values. A cursor the
    /// bounded journal cannot answer exactly gets `resync` (see [`Feed`]). Lock-free with respect
    /// to writes: it pins the read view and holds the journal lock only while copying.
    pub fn changes_since(&self, since: u64, allowed_labels: u32) -> Feed {
        let rs = self.read_state();
        self.feed.lock().unwrap_or_else(|e| e.into_inner()).read(
            since,
            rs.durable_head,
            &rs.snap,
            &rs.schema.cat,
            allowed_labels,
        )
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

    /// Headline node and fact counts (`schema.nodes`, `facts.durable_head` of [`Db::stats`]) off
    /// the pinned view — O(1), no write lock.
    pub fn counts(&self) -> Counts {
        let rs = self.read_state();
        Counts {
            nodes: rs.node_count,
            facts: rs.durable_head,
        }
    }

    /// Engine/schema/embedding/storage counters. Reads only the pinned view: every figure is a
    /// counter captured at publish time (or a small catalog walk), so this is O(1) in the graph
    /// size and never waits for an in-flight write.
    pub fn stats(&self) -> Value {
        let rs = self.read_state();
        // ABAC label distribution — the graph's sensitivity-tier composition, keyed by label value.
        let labels: serde_json::Map<String, Value> = rs
            .label_counts
            .iter()
            .map(|(l, n)| (l.to_string(), json!(n)))
            .collect();
        // Provenance inventory: every source name that has ever stamped a fact, sorted by name.
        let mut sources: Vec<&str> = rs
            .schema
            .sources
            .iter()
            .filter_map(|&s| rs.schema.cat.name(s))
            .collect();
        sources.sort_unstable();
        json!({
            "server": { "version": env!("CARGO_PKG_VERSION"), "uptime_seconds": START.elapsed().as_secs() },
            // suppressed_since_boot is process-lifetime observability (like uptime), not durable
            // state: how much observation noise the ingest boundary has absorbed.
            "facts": { "durable_head": rs.durable_head, "unmerged": rs.unmerged, "suppressed_since_boot": rs.suppressed_total },
            // Catalog size, not lines processed: connectors legitimately re-send their schema with
            // every self-contained batch, so counting the persisted def/node lines reads as
            // unbounded growth on a dashboard while the catalog holds a few dozen entries.
            "schema": {
                "types": rs.schema.cat.types_len(),
                "predicates": rs.schema.cat.predicates().count(),
                "nodes": rs.node_count,
                "rules": rs.schema.rules.len(),
            },
            "labels": labels,
            // stored fact rows per access label (live rows only; a predicate floor is not counted)
            "fact_labels": rs.snap.fact_label_counts.iter().map(|(l, n)| (l.to_string(), json!(n))).collect::<serde_json::Map<String, Value>>(),
            "sources": sources,
            "embeddings": { "count": rs.emb_ids.len(), "dim": rs.dim },
            // Quantizer fit (drift observability): live/trained assignment error and whether the
            // last rebuild had to retrain. A drift_ratio creeping past ~1.5 is the "recall is
            // silently degrading" signal this block exists to surface.
            "index": rs.index.as_ref().as_ref().map(|i| {
                let f = i.fit();
                json!({
                    "nlist": i.nlist(),
                    "trained_fit": f.trained,
                    "live_fit": f.live,
                    "drift_ratio": f.ratio,
                    "retrained_last_build": rs.index_retrained,
                })
            }),
            "storage": { "wal_bytes": rs.wal_bytes, "embeddings_bytes": rs.emb.len() * 4 },
        })
    }
}

impl Drop for Db {
    /// Leave an exact `counts.json` behind so a listing can report this database without opening it.
    fn drop(&mut self) {
        self.write
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .persist_counts();
    }
}

/// Headline counts of a database: distinct nodes and the durable changelog head (facts).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    pub nodes: u64,
    pub facts: u64,
}

/// The counts a database directory last persisted to `counts.json` (on open, after each ingest,
/// compaction, reset and close), read without opening the database or taking its lock. `None`
/// when the file is absent or unreadable (e.g. a directory last written by an older version).
pub fn persisted_counts(dir: &Path) -> Option<Counts> {
    let v: Value = serde_json::from_str(&fs::read_to_string(dir.join("counts.json")).ok()?).ok()?;
    Some(Counts {
        nodes: v["nodes"].as_u64()?,
        facts: v["facts"].as_u64()?,
    })
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

    /// Rebuild the vector index over the current embeddings — reuse-or-retrain. The trained
    /// quantizers are kept (skipping k-means, the dominant build cost) while they still describe the
    /// corpus; a full retrain happens only when the fit ratio crosses [`INDEX_DRIFT_RATIO_MAX`] or
    /// the corpus outgrows the trained `nlist` by ≥2× (the cell-imbalance driver). Runs under the
    /// write lock; the fresh index reaches readers via the usual atomic view publish, and either
    /// path carries the identical posting set (nodes/seqnos/labels), so watermark semantics are
    /// unaffected by which one ran.
    fn rebuild_index(&mut self) {
        if self.emb_ids.is_empty() || self.dim == 0 {
            self.index = Arc::new(None);
            self.index_retrained = false;
            return;
        }
        let n = self.emb_ids.len();
        let vecs: Vec<Vec<f32>> = (0..n)
            .map(|i| self.emb[i * self.dim..(i + 1) * self.dim].to_vec())
            .collect();
        let items = |vecs: &[Vec<f32>]| -> Vec<(u64, u64, Vec<f32>, u32)> {
            self.emb_ids
                .iter()
                .zip(vecs)
                .enumerate()
                .map(|(i, (&id, v))| {
                    (
                        id,
                        i as u64,
                        v.clone(),
                        self.node_label_w.get(&id).copied().unwrap_or(0) as u32,
                    )
                })
                .collect()
        };
        // Reuse path: re-add everything against the existing quantizers, then let the fit ratio —
        // accumulated over exactly the vectors just added — judge whether they still fit.
        let reused: Option<IvfPq> = match self.index.as_ref().as_ref() {
            Some(old) if old.dim() == self.dim && IvfPq::suggested_nlist(n) < old.nlist() * 2 => {
                Some(old.fresh_like())
            }
            _ => None,
        };
        if let Some(mut idx) = reused {
            idx.add_batch(items(&vecs));
            if idx.fit().ratio <= INDEX_DRIFT_RATIO_MAX {
                self.index = Arc::new(Some(idx));
                self.index_retrained = false;
                return;
            }
            // drifted — fall through and pay for k-means on the actual corpus
        }
        // Retrain path: a deterministic stride sample spanning the WHOLE corpus. A prefix sample
        // (`vecs[..20_000]`) would lock the quantizers to the oldest distribution forever.
        let cap = INDEX_TRAIN_SAMPLE.min(n);
        let sample: Vec<Vec<f32>> = (0..cap).map(|i| vecs[i * n / cap].clone()).collect();
        let mut idx = IvfPq::new(self.dim, IvfPq::suggested_nlist(n), pick_m(self.dim));
        idx.train(&sample);
        idx.add_batch(items(&vecs));
        self.index = Arc::new(Some(idx));
        self.index_retrained = true;
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
            node_count: self.node_count,
            label_counts: self.label_counts.clone(),
            unmerged: self.eng.unmerged(),
            suppressed_total: self.suppressed_total,
            index_retrained: self.index_retrained,
            // one stat per publish, off the read path
            wal_bytes: fs::metadata(self.dir.join("wal.log"))
                .map(|m| m.len())
                .unwrap_or(0),
        }
    }

    /// Write `counts.json` ({"nodes","facts"}) via tmp + rename, so [`persisted_counts`] can list
    /// this database without opening it. Called on open, after every ingest, compaction, reset and
    /// on close; skipped when the counts did not change. Not fsynced — one small write + rename
    /// next to the batch's WAL fsync — so it survives a killed process, not necessarily a power
    /// loss. Best effort: the file is a listing hint, never an input, so a failure is logged.
    fn persist_counts(&mut self) {
        let counts = Counts {
            nodes: self.node_count,
            facts: self.eng.durable_head(),
        };
        if self.counts_persisted == Some(counts) {
            return;
        }
        let body = json!({ "nodes": counts.nodes, "facts": counts.facts }).to_string();
        let tmp = self.dir.join("counts.json.tmp");
        let res =
            fs::write(&tmp, body).and_then(|()| fs::rename(&tmp, self.dir.join("counts.json")));
        match res {
            Ok(()) => self.counts_persisted = Some(counts),
            Err(e) => eprintln!("stromadb: write {}/counts.json: {e}", self.dir.display()),
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
            // an already-known name may still be NEW as a source (e.g. it was first interned as a
            // type or predicate); record it in the inventory and persist the source_def once
            if !self.schema.sources.contains(&id) {
                Arc::make_mut(&mut self.schema).sources.insert(id);
                self.append_line(
                    "schema.jsonl",
                    &json!({ "source_def": { "name": n } }).to_string(),
                )?;
            }
            return Ok(id);
        }
        let schema = Arc::make_mut(&mut self.schema);
        let id = schema.cat.intern_ref(n);
        schema.sources.insert(id);
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
        let mut s = IngestStats {
            head_before: self.eng.durable_head(),
            ..IngestStats::default()
        };
        let mut batch: Vec<(u32, WriteKind, Option<u8>)> = Vec::new();
        let mut touched_nodes = false;
        // Keys this call has written (pending batch entries + un-materialized retracts). The no-op
        // suppression below compares an incoming write against the engine's materialized state,
        // which is current for a key exactly when the key is NOT in this set — `flush` materializes
        // everything appended so far, so the set is cleared at every flush. A dirty key skips
        // suppression and appends (the pre-suppression behavior, always safe).
        let mut dirty: HashSet<(NodeId, FieldId)> = HashSet::new();
        for line in jsonl.lines().filter(|l| !l.trim().is_empty()) {
            let v: Value =
                serde_json::from_str(line).map_err(|e| format!("bad json: {e}: {line}"))?;
            if v.get("type_def").is_some() || v.get("pred_def").is_some() {
                let floor_of = |schema: &Schema| {
                    v["pred_def"]["name"]
                        .as_str()
                        .and_then(|n| schema.cat.field_id(n))
                        .and_then(|p| schema.cat.predicate(p))
                        .and_then(|d| d.label_floor)
                };
                let floor_before = floor_of(&self.schema);
                apply_def(Arc::make_mut(&mut self.schema), &v)?;
                self.append_line("schema.jsonl", line)?;
                s.defs += 1;
                // A floor change re-labels existing facts for every reader, but no fact key is
                // written, so watched rules would not journal it: invalidate every watch, exactly
                // as re-declaring a rule invalidates its own.
                if floor_of(&self.schema) != floor_before {
                    self.live_rules.clear();
                }
            } else if v.get("source_def").is_some() {
                // A client-declared provenance source (SPEC §2) — same registration a fact's own
                // `source` would trigger, so re-sending one a fact already interned is a no-op line
                // in schema.jsonl either way (apply_def re-interns idempotently on replay).
                apply_def(Arc::make_mut(&mut self.schema), &v)?;
                self.append_line("schema.jsonl", line)?;
                s.defs += 1;
            } else if v.get("rule_def").is_some() {
                // A named conformance rule: parse + store (names are validated at evaluation, not
                // here — the referenced predicates may be declared later), persist for replay.
                apply_rule_def(Arc::make_mut(&mut self.schema), &v)?;
                // a re-declaration invalidates any live watch on the old declaration — the watcher
                // re-registers (and re-seeds) against the new rule
                if let Some(name) = v["rule_def"]["name"].as_str() {
                    self.live_rules.remove(name);
                }
                self.append_line("rules.jsonl", line)?;
            } else if let Some(n) = v.get("node") {
                if self.apply_node(n)? {
                    self.append_line("nodes.jsonl", line)?;
                    touched_nodes = true;
                    s.nodes += 1;
                } else {
                    s.suppressed += 1;
                }
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
                // optional per-fact access label, stored on this fact's row
                let label = record_label(f, "fact")?;
                // per-fact provenance: intern the optional source name to its stable Field-ID
                // (absent → the batch's default_source if set, else 0). Interned once and reused
                // for this fact's edge-property writes too.
                let src_name = f
                    .get("source")
                    .and_then(|x| x.as_str())
                    .map(str::to_string)
                    .or_else(|| self.default_source.clone());
                let source = self.source_id(src_name.as_deref())?;
                // No-op suppression (append-on-change): a re-assertion identical to current state is
                // skipped, so the changelog grows with change, not with observation frequency (a
                // connector re-sync re-emits unchanged facts wholesale). One head read per incoming
                // fact against the materialized state — trusted only for a clean key (see `dirty`).
                // A same-value fact from a DIFFERENT source always appends: distinct agreeing
                // sources are per-row corroboration evidence.
                let clean = !dirty.contains(&(subject, predicate));
                let snap = self.eng.snapshot_arc();
                let kind = match self.schema.cardinality.get(pname) {
                    Some(Cardinality::One) => {
                        // Suppress iff the CURRENT HEAD row matches on object, valid interval,
                        // source, and access label. Head-only comparison keeps arrival-order
                        // semantics intact: a re-send equal to an OLDER row still appends — it
                        // legitimately moves the head, which the upstream late-arrival guard
                        // relies on. A re-send with a different label appends: it relabels.
                        let dup = clean
                            && query::point_one_head(&*snap, subject, predicate).is_some_and(
                                |(ok, obj, vf, vt)| {
                                    obj.as_ref() == Some(&object)
                                        && vf == valid_from
                                        && vt == valid_to
                                        && ok.source == source
                                        && row_label(&snap, subject, predicate, &ok) == label
                                },
                            );
                        (!dup).then(|| WriteKind::SetOne {
                            subject,
                            predicate,
                            object: object.clone(),
                            valid_from,
                            valid_to,
                        })
                    }
                    _ => {
                        // Suppress iff the element is currently PRESENT with a live add row
                        // matching (source, valid interval) exactly — a different source is
                        // corroboration, a changed interval a correction, and a re-grant after a
                        // close a re-open; all of those append.
                        let dup = clean
                            && self.eng.many_live_asserted(
                                subject, predicate, &object, source, valid_from, valid_to, label,
                            );
                        (!dup).then(|| WriteKind::AddMany {
                            subject,
                            predicate,
                            object: object.clone(),
                            valid_from,
                            valid_to,
                        })
                    }
                };
                match kind {
                    Some(kind) => {
                        batch.push((source, kind, label));
                        dirty.insert((subject, predicate));
                        self.feed_pending.note((subject, predicate), label);
                        s.facts += 1; // appended fact writes only — a suppressed no-op is not a fact
                    }
                    None => s.suppressed += 1,
                }
                // optional edge properties on this fact's edge (subject, predicate, object); each
                // prop equal to its current value is skipped independently — a suppressed fact body
                // with a changed prop still appends just the prop.
                if let Some(props) = f.get("props").and_then(|p| p.as_object()) {
                    for (key, val) in props {
                        let value = value_key(val)?;
                        if clean
                            && query::edge_prop(&snap, subject, predicate, &object, key).as_ref()
                                == Some(&value)
                        {
                            s.suppressed += 1;
                            continue;
                        }
                        batch.push((
                            source,
                            WriteKind::SetEdgeProp {
                                subject,
                                predicate,
                                object: object.clone(),
                                key: key.clone(),
                                value,
                            },
                            None,
                        ));
                        dirty.insert((subject, predicate));
                        // a property follows its edge: visible to whoever sees this fact's row
                        self.feed_pending.note((subject, predicate), label);
                    }
                }
                if batch.len() >= 10_000 {
                    self.flush(&mut batch)?;
                    dirty.clear();
                }
            } else if let Some(c) = v.get("close") {
                // Close a value's valid-time interval: no successor — reads at or after
                // `valid_from` return nothing. Cardinality-one closes the key's single value
                // (`CloseOne`); cardinality-many requires an `object` and closes THAT element's
                // interval (`CloseMany`) — the temporal end of one grant, unlike `retract` which
                // erases the element's history outright. Both are versioned rows with no object,
                // so they replay and merge like any other write.
                let subject = c["subject"].as_u64().ok_or("close.subject missing")?;
                let pname = c["predicate"].as_str().ok_or("close.predicate missing")?;
                let predicate = self
                    .schema
                    .cat
                    .field_id(pname)
                    .ok_or(format!("unknown predicate: {pname}"))?;
                let many = match self.schema.cardinality.get(pname) {
                    Some(Cardinality::One) => {
                        if c.get("object").is_some() {
                            return Err(format!(
                                "close on '{pname}' (cardinality-one) takes no object — the key has a single value"
                            ));
                        }
                        false
                    }
                    Some(Cardinality::Many) => {
                        if c.get("object").is_none() {
                            return Err(format!(
                                "close on '{pname}' (cardinality-many) requires an object — a close ends ONE element's interval (use retract to erase an edge without history)"
                            ));
                        }
                        true
                    }
                    None => return Err(format!("unknown predicate: {pname}")),
                };
                let valid_from = c["valid_from"].as_i64().unwrap_or(0);
                // a close is a row like any other and may carry an access label (a close of a
                // hidden value is then hidden with it)
                let label = record_label(c, "close")?;
                let src_name = c
                    .get("source")
                    .and_then(|x| x.as_str())
                    .map(str::to_string)
                    .or_else(|| self.default_source.clone());
                let source = self.source_id(src_name.as_deref())?;
                let clean = !dirty.contains(&(subject, predicate));
                // No-op suppression: the (element's) winner is already a close at the same
                // boundary with the same label.
                let snap = self.eng.snapshot_arc();
                let closed_dup = |head: Option<stromadb_core::fold::VersionRow>| {
                    head.is_some_and(|(ok, obj, vf, _)| {
                        obj.is_none()
                            && vf == valid_from
                            && row_label(&snap, subject, predicate, &ok) == label
                    })
                };
                let kind = if many {
                    let object = obj_key(&c["object"])?;
                    let dup =
                        clean && closed_dup(query::many_head(&*snap, subject, predicate, &object));
                    (!dup).then_some(WriteKind::CloseMany {
                        subject,
                        predicate,
                        object,
                        valid_from,
                    })
                } else {
                    let dup =
                        clean && closed_dup(query::point_one_head(&*snap, subject, predicate));
                    (!dup).then_some(WriteKind::CloseOne {
                        subject,
                        predicate,
                        valid_from,
                    })
                };
                match kind {
                    Some(kind) => {
                        batch.push((source, kind, label));
                        dirty.insert((subject, predicate));
                        self.feed_pending.note((subject, predicate), label);
                        s.closes += 1;
                    }
                    None => s.suppressed += 1,
                }
                if batch.len() >= 10_000 {
                    self.flush(&mut batch)?;
                    dirty.clear();
                }
            } else if let Some(r) = v.get("retract") {
                self.flush(&mut batch)?; // retract must observe prior writes
                dirty.clear();
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
                let src_name = r
                    .get("source")
                    .and_then(|x| x.as_str())
                    .map(str::to_string)
                    .or_else(|| self.default_source.clone());
                let source = self.source_id(src_name.as_deref())?;
                // the feed shows a retract to whoever could see the element: note the labels of
                // the element's rows (the state is materialized, flushed just above)
                let element_labels: Vec<Option<u8>> = {
                    let snap = self.eng.snapshot_arc();
                    snap.many_history
                        .get(&(subject, predicate))
                        .and_then(|m| m.get(&object))
                        .map(|rows| {
                            rows.iter()
                                .map(|(ok, ..)| row_label(&snap, subject, predicate, ok))
                                .collect()
                        })
                        .unwrap_or_default()
                };
                let removed = self
                    .eng
                    .retract_edge(source, subject, predicate, object)
                    .map_err(|e| format!("backpressure: {e:?}"))?;
                // count only retracts that removed a present edge (absent edge → no-op, not counted)
                if removed.is_some() {
                    s.retracts += 1;
                    for l in element_labels {
                        self.feed_pending.note((subject, predicate), l);
                    }
                    // the remove sits in the un-materialized tail until the next flush, so the
                    // materialized state is stale for this key — mark it dirty
                    dirty.insert((subject, predicate));
                }
            } else {
                return Err(format!("unrecognized record: {line}"));
            }
        }
        self.flush(&mut batch)?;
        self.eng.sync().map_err(|e| format!("fsync: {e}"))?;
        self.materialize_live();
        s.durable_head = self.eng.durable_head();
        self.suppressed_total += s.suppressed;
        if touched_nodes {
            self.rebuild_index();
        }
        Ok(s)
    }

    /// A node record: emit its type/label as engine ops (so the snapshot carries them) and mirror the
    /// label into the write-side index-build map.
    /// Returns whether anything was written — a re-sent record whose type and label both match
    /// the write-side mirrors is a no-op the caller suppresses (no changelog write, no jsonl line).
    fn apply_node(&mut self, n: &Value) -> DbResult<bool> {
        let id = n["id"].as_u64().ok_or("node.id missing")?;
        let mut wrote = false;
        // The mirrors (and the maintained counters with them) move only after the engine accepted
        // the op, so they never run ahead of what the snapshot will hold.
        let mut counted = self.node_type_w.contains_key(&id) || self.node_label_w.contains_key(&id);
        if let Some(t) = n["type"].as_str() {
            let tid = self
                .schema
                .cat
                .field_id(t)
                .ok_or(format!("unknown type: {t}"))?;
            if self.node_type_w.get(&id) != Some(&tid) {
                self.eng
                    .write(
                        0,
                        WriteKind::SetNodeType {
                            node: id,
                            type_id: tid,
                        },
                    )
                    .map_err(|e| format!("backpressure: {e:?}"))?;
                self.node_type_w.insert(id, tid);
                if !counted {
                    self.node_count += 1;
                    self.feed_pending.note_new(id);
                    counted = true;
                }
                wrote = true;
            }
        }
        if let Some(l) = n["label"].as_u64() {
            let label = l as u8;
            if self.node_label_w.get(&id) != Some(&label) {
                self.eng
                    .write(0, WriteKind::SetNodeLabel { node: id, label })
                    .map_err(|e| format!("backpressure: {e:?}"))?;
                if let Some(old) = self.node_label_w.insert(id, label)
                    && let Some(c) = self.label_counts.get_mut(&old)
                {
                    *c -= 1;
                    if *c == 0 {
                        self.label_counts.remove(&old);
                    }
                }
                *self.label_counts.entry(label).or_insert(0) += 1;
                if !counted {
                    self.node_count += 1;
                    self.feed_pending.note_new(id);
                }
                wrote = true;
            }
        }
        Ok(wrote)
    }

    fn flush(&mut self, batch: &mut Vec<(u32, WriteKind, Option<u8>)>) -> DbResult<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.eng
            .write_batch_labeled(std::mem::take(batch))
            .map_err(|e| format!("backpressure: {e:?}"))?;
        self.eng.sync().map_err(|e| format!("fsync: {e}"))?;
        self.materialize_live();
        Ok(())
    }

    /// Drain the engine tail, journal what it touched for the change feed, and keep every
    /// watched rule current. Every tail drain on the write path MUST go through here — a drain
    /// that bypassed it would silently detach the maintained verdict maps from the graph (their
    /// support keys would never fire again) and leave a gap in the change feed.
    fn materialize_live(&mut self) {
        let (keys, nodes) = self.eng.materialize_tracked_with_nodes();
        if keys.is_empty() && nodes.is_empty() {
            self.feed_pending.clear();
            return;
        }
        let head = self.eng.durable_head();
        self.feed.lock().unwrap_or_else(|e| e.into_inner()).record(
            head,
            &keys,
            &nodes,
            &mut self.feed_pending,
        );
        if self.live_rules.is_empty() {
            return;
        }
        let snap = self.eng.snapshot_arc();
        for lr in self.live_rules.values_mut() {
            let diffs = lr.maintained.apply(&snap, &self.schema.cat, &keys, &nodes);
            if !diffs.is_empty() {
                lr.push(head, diffs);
            }
        }
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
    /// The fact view a read with `allowed_labels` runs on — the single choke point of per-fact
    /// label masking. Every read op below reads facts only through the view this returns (node
    /// labels still gate whole nodes, via [`label_visible`]). It borrows the pinned snapshot, so
    /// reads stay lock-free; when the mask hides nothing that exists, every access is a direct
    /// borrow of the snapshot.
    fn facts(&self, allowed_labels: u32) -> Masked<'_> {
        Masked::new(&self.snap, &self.schema.cat, allowed_labels)
    }

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
                // post-authz: a subject outside the caller's labels answers `denied` (same contract
                // as the node view); a node-valued answer outside them reads as absent.
                let labels = req_labels(req);
                let facts = self.facts(labels);
                if !label_visible(&self.snap, subject, labels) {
                    return Ok(json!({ "denied": true }));
                }
                let node_ok = |o: &ObjKey| !matches!(o, ObjKey::Node(n) if !label_visible(&self.snap, *n, labels));
                // optional valid-time as-of: `"valid_at": T` returns the One-value in effect at T
                // (respecting the [valid_from, valid_to) interval); absent = current functional value.
                let valid_at = req["valid_at"].as_i64();
                Ok(match self.schema.cardinality.get(pname) {
                    Some(Cardinality::One) => {
                        let obj = match valid_at {
                            Some(at) => query::point_one_asof(&facts, subject, pid, at),
                            None => query::point_one(&facts, subject, pid),
                        }
                        .filter(node_ok);
                        // Provenance of the current functional value: the winning version's source
                        // name (omitted when unset, or for an as-of/historical read). Additive — the
                        // `one` shape is unchanged.
                        let provenance: Option<String> = (valid_at.is_none() && obj.is_some())
                            .then(|| {
                                query::point_one_source(&facts, subject, pid)
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
                            && let Some(vf) = query::point_one_valid_from(&facts, subject, pid)
                        {
                            resp["valid_from"] = json!(vf);
                        }
                        // The close boundary when the winning version is a close (additive; a
                        // current absent value only — never for an as-of read, and a never-written
                        // key stays exactly `{"one": null}`). Distinguishes "ended" from "never
                        // written" so a writer can defend the close during late-arrival repair.
                        if is_current_absent
                            && let Some(vf) = query::point_one_closed_from(&facts, subject, pid)
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
                            let c = query::confidence_signals(&facts, subject, pid, now, max_age);
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
                    _ => match valid_at {
                        // As-of Many read: the elements in effect at T. The response echoes
                        // `valid_at` so a client can tell a supporting server from an older one
                        // that would silently answer with the current set.
                        Some(at) => {
                            json!({ "many": query::point_many_asof(&facts, subject, pid, at).into_iter().filter(node_ok).map(fmt_obj).collect::<Vec<_>>(), "valid_at": at })
                        }
                        None => {
                            json!({ "many": query::point_many(&facts, subject, pid).into_iter().filter(node_ok).map(fmt_obj).collect::<Vec<_>>() })
                        }
                    },
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
                // post-authz, same contract as point: an out-of-labels subject is denied, and
                // out-of-labels nodes drop from the result set.
                let labels = req_labels(req);
                let facts = self.facts(labels);
                if !label_visible(&self.snap, subject, labels) {
                    return Ok(json!({ "denied": true }));
                }
                let vis = |n: &u64| label_visible(&self.snap, *n, labels);
                // Honor the predicate's declared relationship properties (symmetric / inverse /
                // transitive); `max_depth` bounds the transitive closure (default 16). Optional
                // `valid_at` answers every hop from the state in effect at T (echoed back, same
                // capability-detection contract as the Many as-of point read).
                let max_depth = req["max_depth"].as_u64().map(|d| d as usize).unwrap_or(16);
                Ok(match req["valid_at"].as_i64() {
                    Some(at) => {
                        json!({ "nodes": query::expand_rel_asof(&facts, &self.schema.cat, subject, pid, max_depth, at).into_iter().filter(vis).collect::<Vec<_>>(), "valid_at": at })
                    }
                    None => {
                        json!({ "nodes": query::expand_rel(&facts, &self.schema.cat, subject, pid, max_depth).into_iter().filter(vis).collect::<Vec<_>>() })
                    }
                })
            }
            // The "over which intervals" read: the full valid-time timeline of a value reached
            // through a chain of one-cardinality hops (a single-element chain = one predicate's own
            // timeline). The interval form of composing `point … valid_at` — for any T inside a
            // returned segment the point as-of composition returns that segment's value; instants
            // no segment covers read as absent.
            "timeline" => {
                let subject = req["subject"].as_u64().ok_or("timeline.subject missing")?;
                let hops_v = req["hops"]
                    .as_array()
                    .ok_or("timeline.hops must be an array of predicate names")?;
                if hops_v.is_empty() {
                    return Err("timeline.hops must not be empty".into());
                }
                let mut hops = Vec::with_capacity(hops_v.len());
                for h in hops_v {
                    let name = h
                        .as_str()
                        .ok_or("timeline.hops entries must be predicate names")?;
                    hops.push(
                        self.schema
                            .cat
                            .field_id(name)
                            .ok_or(format!("unknown predicate: {name}"))?,
                    );
                }
                // post-authz, same contract as point: an out-of-labels subject answers empty, and
                // segments whose value is an out-of-labels node are dropped (an interval naming a
                // hidden node would leak its existence).
                let labels = req_labels(req);
                let facts = self.facts(labels);
                if !label_visible(&self.snap, subject, labels) {
                    return Ok(json!({ "segments": [] }));
                }
                // Trace the walk's support set: every (node, predicate) history it reads. The
                // answer's confidence is the weakest link over those supports (min tier), so a
                // three-hop answer resting on one source-less hop reads as low even when the other
                // hops corroborate.
                let mut supports: BTreeSet<(u64, u32)> = BTreeSet::new();
                let segs = query::derived_timeline_traced(&facts, subject, &hops, &mut |n, p| {
                    supports.insert((n, p));
                });
                let segments: Vec<Value> = segs
                    .into_iter()
                    .filter(|s| !matches!(&s.value, ObjKey::Node(n) if !label_visible(&self.snap, *n, labels)))
                    .map(|s| {
                        json!({
                            "value": fmt_obj(s.value),
                            "valid_from": s.valid_from,
                            "valid_to": s.valid_to,
                        })
                    })
                    .collect();
                let mut resp = json!({ "segments": segments });
                // Additive confidence, same omission rule as `point`: an absent answer (no
                // segments) carries none, keeping the shape unchanged. The weakest support's raw
                // signals travel with the tier, plus a pointer to *which* hop is the bottleneck.
                if !resp["segments"].as_array().unwrap().is_empty() {
                    let now = req["now"].as_i64();
                    let max_age = req["max_age"].as_i64();
                    let weakest = supports
                        .iter()
                        .map(|&(n, p)| {
                            (query::confidence_signals(&facts, n, p, now, max_age), n, p)
                        })
                        .min_by_key(|(c, ..)| c.tier)
                        .expect("hops is non-empty, so at least one support was recorded");
                    let (c, n, p) = weakest;
                    let mut conf = json!({
                        "tier": c.tier.as_str(),
                        "corroboration": c.corroboration,
                        "sources": c.corroboration,
                        "weakest": {
                            "node": n,
                            "predicate": self.schema.cat.name(p),
                        },
                    });
                    if let Some(age) = c.age {
                        conf["age"] = json!(age);
                    }
                    resp["confidence"] = conf;
                }
                Ok(resp)
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
                // post-authz like point: a subject outside the caller's node labels is denied, and
                // the props of an edge whose rows are all hidden by fact labels are absent
                let labels = req_labels(req);
                if !label_visible(&self.snap, subject, labels) {
                    return Ok(json!({ "denied": true }));
                }
                let facts = self.facts(labels);
                let props = query::edge_props(&facts, subject, pid, &object)
                    .map(|m| {
                        m.iter()
                            .map(|(k, v)| (k.clone(), fmt_obj(v.clone())))
                            .collect::<serde_json::Map<_, _>>()
                    })
                    .unwrap_or_default();
                Ok(json!({ "props": props }))
            }
            "retrieve_context" => self.retrieve_context(req),
            "find" => self.find(req),
            "type_nodes" => self.type_nodes(req),
            "lookup" => self.lookup(req),
            "neighborhood" => self.neighborhood(req),
            "node" => self.node_detail(req),
            "graph" => self.graph(req),
            "overview" => self.overview(req),
            "schema" => Ok(self.schema_view()),
            "rule" => self.rule_definition(req),
            "pipeline" => self.pipeline(req),
            "conformance" => self.conformance(req),
            "completeness" => self.completeness(req),
            other => Err(format!("unknown op: {other}")),
        }
    }

    /// Distance-bounded subgraph around a focal node: BFS out to `hops` (default 2), following a
    /// given `predicate` or *all* node-valued edges (ontology view), authz-scoped, capped at
    /// `max_nodes` (default 3000). Returns `{nodes:[{id,depth}], edges:[[a,b]]}` — the primitive the
    /// UI's "distance from a node" filter renders. When the cap cuts a hop level, `order:"recent"`
    /// keeps the nodes created latest (see [`first_seen`]); the default keeps adjacency order.
    /// One node of a `graph` / `neighborhood` answer: id, hop depth, display name and type name
    /// (`null` for an untyped node), and `placeholder`: true when the node exists only because
    /// another node's fact points at it, i.e. it has no facts of its own as subject.
    fn graph_node(&self, facts: &Masked, id: u64, depth: usize) -> Value {
        let ty = self
            .snap
            .node_types
            .get(&id)
            .and_then(|&t| self.schema.cat.name(t));
        let (ones, manys) = query::describe(facts, id);
        let placeholder = ones.is_empty() && manys.is_empty();
        json!({ "id": id, "depth": depth, "name": self.display_name(facts, id), "type": ty, "placeholder": placeholder })
    }

    /// One edge of a `graph` / `neighborhood` answer: `[a, b, strength, [predicate names]]`, where
    /// strength is the number of distinct predicates connecting the pair.
    fn graph_edge(&self, preds: &HashMap<(u64, u64), BTreeSet<FieldId>>, a: u64, b: u64) -> Value {
        let mut names: Vec<&str> = preds
            .get(&(a, b))
            .into_iter()
            .flatten()
            .filter_map(|&p| self.schema.cat.name(p))
            .collect();
        names.sort_unstable();
        json!([a, b, names.len().max(1), names])
    }

    fn neighborhood(&self, req: &Value) -> DbResult<Value> {
        let focus = req["subject"].as_u64().ok_or("subject required")?;
        let hops = req["hops"].as_u64().unwrap_or(2) as usize;
        let cap = req["max_nodes"].as_u64().unwrap_or(3000) as usize;
        let labels = req_labels(req);
        let facts = self.facts(labels);
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
        let adj = query::undirected_adjacency(&facts, pred);
        let seen = is_recent(req).then(|| first_seen(&self.snap));
        let mut frontier = vec![focus];
        for d in 0..hops {
            if frontier.is_empty() || depth.len() >= cap {
                break;
            }
            let mut found: Vec<u64> = Vec::new();
            let mut found_set: HashSet<u64> = HashSet::new();
            for &u in &frontier {
                for &v in adj.get(&u).into_iter().flatten() {
                    if !visible(v) {
                        continue;
                    }
                    edges.insert(if u < v { (u, v) } else { (v, u) });
                    if !depth.contains_key(&v) && found_set.insert(v) {
                        found.push(v);
                    }
                }
            }
            // within one hop level the cap keeps the newest nodes when `order` is "recent"
            if let Some(seen) = &seen {
                sort_recent(&mut found, seen);
            }
            found.truncate(cap.saturating_sub(depth.len()));
            for &v in &found {
                depth.insert(v, d + 1);
            }
            frontier = found;
        }
        let nodes: Vec<Value> = depth
            .iter()
            .map(|(&id, &d)| self.graph_node(&facts, id, d))
            .collect();
        let preds = query::edge_predicates(&facts, pred);
        let edges: Vec<Value> = edges
            .iter()
            .filter(|(a, b)| depth.contains_key(a) && depth.contains_key(b))
            .map(|(a, b)| self.graph_edge(&preds, *a, *b))
            .collect();
        Ok(json!({ "nodes": nodes, "edges": edges, "focus": focus }))
    }

    /// Full detail of a single node: its type, label, and every stored `(predicate, value)`
    /// assertion (One current value + Many present set), predicate names resolved via the catalog.
    /// Post-authz: if `allowed_labels` is given and the node's label is not permitted, returns
    /// `{id, denied:true}` rather than leaking its properties. Powers the UI's node-inspect panel.
    fn node_detail(&self, req: &Value) -> DbResult<Value> {
        let subject = req["subject"].as_u64().ok_or("subject required")?;
        let labels = req_labels(req);
        let facts = self.facts(labels);
        let visible = self
            .snap
            .node_labels
            .get(&subject)
            .is_none_or(|&l| (labels >> l) & 1 == 1);
        if !visible {
            return Ok(json!({ "id": subject, "denied": true }));
        }
        let (ones, manys) = query::describe(&facts, subject);
        let mut props = Vec::with_capacity(ones.len() + manys.len());
        for (p, ok) in ones {
            let name = self.schema.cat.name(p).unwrap_or("?");
            // provenance of this One value: the winning version's source name (omitted when unset)
            let source: Option<String> = query::point_one_source(&facts, subject, p)
                .filter(|&src| src != 0)
                .and_then(|src| self.schema.cat.name(src))
                .map(str::to_string);
            let mut prop = json!({ "predicate": name, "card": "one", "value": fmt_obj(ok) });
            if let Some(src) = source {
                prop["source"] = json!(src);
            }
            // coarse confidence tier (no reference time here, so freshness never downgrades it) —
            // the belief-strength companion to the provenance chip
            let conf = query::confidence_signals(&facts, subject, p, None, None);
            prop["confidence"] = json!(conf.tier.as_str());
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
                json!({ "name": name, "card": card, "domain": domain, "range": range, "display": p.display, "label": p.label, "label_floor": p.label_floor })
            })
            .collect();
        preds.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        // Stored rule names, so a client can offer "run rule X" without knowing the declarations.
        let mut rules: Vec<&str> = self.schema.rules.keys().map(|s| s.as_str()).collect();
        rules.sort_unstable();
        json!({ "predicates": preds, "labels": labels_in_use(&self.snap, &self.schema.cat), "rules": rules })
    }

    /// Read back stored rule declarations. With `rule_name`, returns `{ "name", "rule" }` where
    /// `rule` is the JSON exactly as declared in its latest `rule_def` (an unknown name is an
    /// error). Without it, returns `{ "rules": [ { "name", "rule" }, .. ] }` for every stored rule,
    /// sorted by name. Rules are schema-level, so no label mask applies, as with `schema`.
    /// The declaration has the shape [`Db::query`]'s `conformance` op accepts (see
    /// `parse_conformance_rule`): hop paths with optional `as_of` anchors, or first-match `cases`
    /// in place of `required`, and conditions testing `equals` or a numeric range, optionally
    /// read as-of their own anchor.
    fn rule_definition(&self, req: &Value) -> DbResult<Value> {
        if let Some(name) = req.get("rule_name").filter(|v| !v.is_null()) {
            let name = name.as_str().ok_or("rule.rule_name must be a string")?;
            let stored = self
                .schema
                .rules
                .get(name)
                .ok_or(format!("unknown rule_name: {name}"))?;
            return Ok(json!({ "name": name, "rule": stored.definition }));
        }
        let mut names: Vec<&String> = self.schema.rules.keys().collect();
        names.sort_unstable();
        let rules: Vec<Value> = names
            .into_iter()
            .map(|n| json!({ "name": n, "rule": self.schema.rules[n].definition }))
            .collect();
        Ok(json!({ "rules": rules }))
    }

    /// Whole-graph view: every declared node and its node-valued edges, authz-scoped and capped at
    /// `max_nodes` (default 3000). Unlike `neighborhood` there is no focal distance — the result is
    /// the entire visible graph (or a `truncated` prefix when it exceeds the cap). The prefix is
    /// by ascending node id unless `order` is `"recent"`, which keeps the nodes whose first stored
    /// fact has the latest transaction time (newest first). Same
    /// `{nodes:[{id,depth,name,type,placeholder}], edges:[[a,b,strength,[predicates]]]}` shape so the UI renders it identically.
    fn graph(&self, req: &Value) -> DbResult<Value> {
        let cap = req["max_nodes"].as_u64().unwrap_or(3000) as usize;
        let labels = req_labels(req);
        let facts = self.facts(labels);
        let visible = |n: u64| {
            self.snap
                .node_labels
                .get(&n)
                .is_none_or(|&l| (labels >> l) & 1 == 1)
        };

        let mut all: Vec<u64> = node_ids(&self.snap)
            .into_iter()
            .filter(|&n| visible(n))
            .collect();
        if is_recent(req) {
            sort_recent(&mut all, &first_seen(&self.snap));
        }
        let truncated = all.len() > cap;
        let keep: BTreeSet<u64> = all.into_iter().take(cap).collect();

        let mut edges: BTreeSet<(u64, u64)> = BTreeSet::new();
        for &u in &keep {
            for v in query::neighbors(&facts, u) {
                if keep.contains(&v) {
                    edges.insert(if u < v { (u, v) } else { (v, u) });
                }
            }
        }
        let nodes: Vec<Value> = keep
            .iter()
            .map(|&id| self.graph_node(&facts, id, 0))
            .collect();
        let preds = query::edge_predicates(&facts, None);
        let edges: Vec<Value> = edges
            .iter()
            .map(|(a, b)| self.graph_edge(&preds, *a, *b))
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
        let labels = req_labels(req);
        let facts = self.facts(labels);
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
        facts.for_each_one(.., |&(s, _), v| {
            if let Some(ObjKey::Node(o)) = v {
                bump(s, *o, &mut ew);
            }
        });
        facts.for_each_many(.., |&(s, _), set| {
            for o in set {
                if let ObjKey::Node(o) = o {
                    bump(s, *o, &mut ew);
                }
            }
        });

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

    /// The members of one entity type — the console's drill-down from an `overview` bubble: "list
    /// the nodes of this type, then pick one to draw its neighbourhood." `{"op":"type_nodes",
    /// "type":"Person","limit":50}`; omitting `type` lists untyped nodes (the `overview` response's
    /// `(untyped)` bubble). Ascending node-id order, authz-scoped, capped at `limit` (default 50).
    /// Returns `{"nodes":[{"id","name"}],"count","truncated"}` — `count` is the *visible* total
    /// (before the cap), so the console can say "N nodes" even when the list itself is truncated.
    fn type_nodes(&self, req: &Value) -> DbResult<Value> {
        let type_id = match req["type"].as_str() {
            Some(name) => Some(
                self.schema
                    .cat
                    .field_id(name)
                    .ok_or(format!("unknown type: {name}"))?,
            ),
            None => None,
        };
        let limit = req["limit"].as_u64().unwrap_or(50) as usize;
        let labels = req_labels(req);
        let facts = self.facts(labels);
        let visible = |n: u64| {
            self.snap
                .node_labels
                .get(&n)
                .is_none_or(|&l| (labels >> l) & 1 == 1)
        };
        let matches = |n: u64| self.snap.node_types.get(&n).copied() == type_id;

        let members: Vec<u64> = node_ids(&self.snap)
            .into_iter()
            .filter(|&n| matches(n) && visible(n))
            .collect();
        let count = members.len();
        let nodes: Vec<Value> = members
            .into_iter()
            .take(limit)
            .map(|id| json!({ "id": id, "name": self.display_name(&facts, id) }))
            .collect();
        Ok(json!({ "nodes": nodes, "count": count, "truncated": count > limit }))
    }

    /// A node's display name — the value of a predicate the schema flags with `display: true`,
    /// falling back to the first `name`/`title`/`display_name`/`full_name` text predicate when no
    /// flagged predicate covers the node. Used to label nodes in graph/neighbourhood results.
    /// Read through the caller's fact view: a hidden display value is skipped as if absent, so
    /// the name falls back to the next visible candidate (or none).
    fn display_name(&self, facts: &Masked, id: u64) -> Option<String> {
        const NAME_PREDS: [&str; 4] = ["name", "title", "display_name", "full_name"];
        let mut flagged: Option<String> = None;
        let mut fallback: Option<String> = None;
        facts.for_each_one((id, u32::MIN)..=(id, u32::MAX), |&(_, p), v| {
            let Some(ObjKey::Text(s)) = v else { return };
            if flagged.is_none() && self.schema.cat.predicate(p).is_some_and(|d| d.display) {
                flagged = Some(s.clone());
            }
            if fallback.is_none()
                && self
                    .schema
                    .cat
                    .name(p)
                    .is_some_and(|n| NAME_PREDS.contains(&n))
            {
                fallback = Some(s.clone());
            }
        });
        flagged.or(fallback)
    }

    /// Free-word node search: case-insensitive substring over every text property value (one- and
    /// many-cardinality), post-authz scoped (a masked node is absent), deduped per node in
    /// ascending-id order. Returns the carrying node, its display name, and where the hit was —
    /// the "just find the node for X" read the numeric-id ops don't cover.
    fn find(&self, req: &Value) -> DbResult<Value> {
        let needle = req["text"]
            .as_str()
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .ok_or("text required")?;
        let limit = req["limit"].as_u64().unwrap_or(20) as usize;
        let labels = req_labels(req);
        let facts = self.facts(labels);
        let visible = |n: u64| {
            self.snap
                .node_labels
                .get(&n)
                .is_none_or(|&l| (labels >> l) & 1 == 1)
        };
        // First hit per node (iteration order is deterministic: one values, then many values,
        // ascending (node, predicate)); collected fully, then cut to `limit` in id order.
        let mut hits: BTreeMap<u64, (FieldId, String)> = BTreeMap::new();
        facts.for_each_one(.., |&(n, p), v| {
            if let Some(ObjKey::Text(s)) = v
                && !hits.contains_key(&n)
                && visible(n)
                && s.to_lowercase().contains(&needle)
            {
                hits.insert(n, (p, excerpt(s, &needle)));
            }
        });
        facts.for_each_many(.., |&(n, p), set| {
            if hits.contains_key(&n) || !visible(n) {
                return;
            }
            for o in set {
                if let ObjKey::Text(s) = o
                    && s.to_lowercase().contains(&needle)
                {
                    hits.insert(n, (p, excerpt(s, &needle)));
                    break;
                }
            }
        });
        let nodes: Vec<Value> = hits
            .iter()
            .take(limit)
            .map(|(&n, (p, s))| {
                let ty = self
                    .snap
                    .node_types
                    .get(&n)
                    .and_then(|&t| self.schema.cat.name(t));
                json!({
                    "id": n,
                    "name": self.display_name(&facts, n),
                    "type": ty,
                    "matched": { "predicate": self.schema.cat.name(*p).unwrap_or("?"), "value": s },
                })
            })
            .collect();
        Ok(json!({ "nodes": nodes, "truncated": hits.len() > limit }))
    }

    /// Exact-value node resolution — the "external key → node id" read. Returns the nodes whose
    /// one-cardinality `predicate` currently equals `value` (or, with `valid_at`, equalled it at
    /// that valid-time instant), optionally restricted to a `type`, post-authz scoped (a masked node
    /// is absent, a hidden fact never matches), ascending by id, cut to `limit` (default 10, capped
    /// at 100) with a `truncated` flag. `value` takes a bare scalar (a string is text) or the ingest
    /// object form (`{"text": ..}`, `{"int": ..}`, `{"node": N}`); `equals` is accepted as an alias.
    ///
    /// Batch form: `queries: [{value, valid_at?}, ..]` (each item's `valid_at` defaults to the
    /// top-level one) or the shorthand `values: [V, ..]`, at most [`LOOKUP_BATCH_MAX`] items,
    /// answers `{"results": [..]}` with one single-form response per item, in request order, all
    /// read from one pinned snapshot under one mask.
    ///
    /// Cost: O(log n + candidates) per value through the snapshot's reverse value index, where the
    /// candidates are the subjects with a live row (current or superseded) of that value; see
    /// DECISIONS D36.
    fn lookup(&self, req: &Value) -> DbResult<Value> {
        let pname = req["predicate"]
            .as_str()
            .ok_or("lookup.predicate missing")?;
        let pid = self
            .schema
            .cat
            .field_id(pname)
            .ok_or(format!("unknown predicate: {pname}"))?;
        if !matches!(self.schema.cardinality.get(pname), Some(Cardinality::One)) {
            return Err(format!("lookup.predicate must be one-cardinality: {pname}"));
        }
        let ty = match req["type"].as_str() {
            Some(t) => Some(
                self.schema
                    .cat
                    .field_id(t)
                    .ok_or(format!("unknown type: {t}"))?,
            ),
            None => None,
        };
        let limit = req["limit"].as_u64().unwrap_or(10).min(100) as usize;
        let valid_at = req["valid_at"].as_i64();
        let labels = req_labels(req);
        let facts = self.facts(labels);
        let keep = |n: NodeId| {
            label_visible(&self.snap, n, labels)
                && ty.is_none_or(|t| self.snap.node_types.get(&n) == Some(&t))
        };
        let answer = |want: &ObjKey, at: Option<i64>| {
            let ids = query::lookup_one(&facts, pid, want, at, keep);
            let nodes: Vec<Value> = ids
                .iter()
                .take(limit)
                .map(|&n| {
                    json!({
                        "id": n,
                        "type": self.snap.node_types.get(&n).and_then(|&t| self.schema.cat.name(t)),
                        "display": self.display_name(&facts, n),
                    })
                })
                .collect();
            let mut resp = json!({ "nodes": nodes, "truncated": ids.len() > limit });
            if let Some(at) = at {
                resp["valid_at"] = json!(at);
            }
            resp
        };
        let batch: Option<Vec<(ObjKey, Option<i64>)>> = match (&req["queries"], &req["values"]) {
            (Value::Null, Value::Null) => None,
            (Value::Array(qs), Value::Null) => Some(
                qs.iter()
                    .map(|q| Ok((lookup_value(q)?, q["valid_at"].as_i64().or(valid_at))))
                    .collect::<DbResult<_>>()?,
            ),
            (Value::Null, Value::Array(vs)) => Some(
                vs.iter()
                    .map(|v| Ok((lookup_scalar(v)?, valid_at)))
                    .collect::<DbResult<_>>()?,
            ),
            _ => return Err("lookup takes one of value, queries (array) or values (array)".into()),
        };
        match batch {
            None => Ok(answer(&lookup_value(req)?, valid_at)),
            Some(items) => {
                if !req["value"].is_null() || !req["equals"].is_null() {
                    return Err("lookup takes one of value, queries or values".into());
                }
                if items.len() > LOOKUP_BATCH_MAX {
                    return Err(format!(
                        "lookup batch too large: {} items (max {LOOKUP_BATCH_MAX})",
                        items.len()
                    ));
                }
                let results: Vec<Value> = items.iter().map(|(v, at)| answer(v, *at)).collect();
                Ok(json!({ "results": results }))
            }
        }
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
        let labels = req_labels(req);
        let facts = self.facts(labels);
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
            &facts,
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
        let labels = req_labels(req);
        let facts = self.facts(labels);
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
            run(&facts, idx, &pipeline, &principal, vv)
        } else {
            run(&facts, &NoAnn, &pipeline, &principal, vv)
        };
        let nodes: Vec<Value> = t
            .ids
            .iter()
            .map(|&id| json!({ "id": id, "name": self.display_name(&facts, id) }))
            .collect();
        Ok(json!({ "ids": t.ids, "nodes": nodes }))
    }

    /// Evaluate a declared conformance rule into deterministic per-subject verdicts. The rule is given
    /// either inline as `req["rule"]` or by `req["rule_name"]` (a rule declared once via `rule_def` and
    /// stored in the registry) — exactly one is required. Predicate/type names are resolved against the
    /// catalog (unknown names are a clear error), then [`conformance::evaluate`] composes the existing
    /// read primitives into `OK | ABSENT | MISMATCH | NOT_APPLICABLE` verdicts, authz-scoped by
    /// `allowed_labels` (default all). A `MISMATCH` carries a `kind` of `"stale"` (the actual value
    /// held the last as-of hop at another valid-time) or `"wrong"` (it never did); a
    /// `NOT_APPLICABLE` row, and only such a row, carries a `reason`: `"out_of_scope"`,
    /// `"no_matching_case"`, or `"required_unresolved"` (the actual is present but the required
    /// path resolved to no value, including no value in effect at the as-of anchor; the row keeps
    /// its values and is never `stale`). `case` is the matched case index of a
    /// banded rule (`cases`), else null. Returns
    /// `{ "verdicts": [ { subject, verdict, kind, required, distinct, actual, as_of, case, reason? },
    /// .. ], "total", "returned", "truncated", "counts": {OK, ABSENT, MISMATCH, NOT_APPLICABLE},
    /// "reasons": {out_of_scope, no_matching_case, required_unresolved, not_subject_type,
    /// unknown_subject} }`.
    ///
    /// Optional narrowing, all additive: `subject` / `subjects` evaluate only those ids, one row per
    /// distinct requested id (sorted by id). A requested id the rule does not judge answers
    /// `NOT_APPLICABLE` with no values and the `reason` `"not_subject_type"` for a visible node of
    /// another type, `"unknown_subject"` for an id with no typed node or a node hidden by
    /// `allowed_labels` (hidden and nonexistent are indistinguishable). `only: [verdict names]`
    /// keeps those outcomes; `offset` / `limit` page the kept rows. `counts` (per verdict) and
    /// `reasons` (per reason of the `NOT_APPLICABLE` rows) cover every row before `only` and paging,
    /// `total` is the kept count, and `truncated` means rows remain after this page. With none of
    /// them the op returns every verdict, as before; the MCP tool applies its own smaller defaults.
    ///
    /// Missing information, also additive: every row carries `missing` — what stopped the judgment
    /// short, `{kind: "anchor", node, predicate}` (an as-of hop or condition whose anchor has no value
    /// on the subject) or `{kind: "hop", node, predicate}` (a path predicate with no value on that
    /// node) — and `assumed`, the anchors whose instant came from `assume`. `assume: {"<int anchor
    /// predicate>": instant}` is used only where the subject has no value for that anchor (graph
    /// values win) and is evaluated on demand, never stored. `subject` alone (no `rule` /
    /// `rule_name`) evaluates every stored rule covering the subject's type, each row naming its
    /// `rule`; when none covers it, the top-level `missing` holds `{kind: "no_rule", node, type}`
    /// (`type` null for an unknown or hidden id).
    fn conformance(&self, req: &Value) -> DbResult<Value> {
        let labels = req_labels(req);
        let mut top_missing: Vec<Value> = Vec::new();
        let rules: Vec<(Option<String>, conformance::Rule)> = if let Some(name) =
            req["rule_name"].as_str()
        {
            let rule = self
                .schema
                .rules
                .get(name)
                .map(|s| s.rule.clone())
                .ok_or(format!("unknown rule_name: {name}"))?;
            vec![(None, rule)]
        } else if !req["rule"].is_null() {
            vec![(None, parse_conformance_rule(&req["rule"])?)]
        } else if let Some(s) = req["subject"].as_u64() {
            let ty = self
                .snap
                .node_types
                .get(&s)
                .filter(|_| label_visible(&self.snap, s, labels))
                .and_then(|&t| self.schema.cat.name(t));
            let mut covering: Vec<(Option<String>, conformance::Rule)> = self
                .schema
                .rules
                .iter()
                .filter(|(_, sr)| Some(sr.rule.subject_type.as_str()) == ty)
                .map(|(n, sr)| (Some(n.clone()), sr.rule.clone()))
                .collect();
            covering.sort_by(|a, b| a.0.cmp(&b.0));
            if covering.is_empty() {
                top_missing.push(json!({ "kind": "no_rule", "node": s, "type": ty }));
            }
            covering
        } else {
            return Err(
                    "conformance requires 'rule' (inline), 'rule_name' (stored), or 'subject' alone (every stored rule covering its type)"
                        .into(),
                );
        };
        for (_, rule) in &rules {
            let missing = conformance::unresolved_names(rule, &self.schema.cat);
            if !missing.is_empty() {
                return Err(format!(
                    "unknown name(s) in conformance rule: {}",
                    missing.join(", ")
                ));
            }
            check_rule_paths(rule, &self.schema.cat)?;
        }
        let assume = parse_assumptions(&req["assume"], &self.schema.cat)?;
        // `subject` (one id) or `subjects` (a list): evaluate only those.
        let subjects: Option<Vec<NodeId>> =
            if let Some(s) = req.get("subjects").filter(|v| !v.is_null()) {
                let arr = s
                    .as_array()
                    .ok_or("conformance.subjects must be an array of node ids")?;
                Some(
                    arr.iter()
                        .map(|v| {
                            v.as_u64()
                                .ok_or("conformance.subjects entries must be node ids")
                        })
                        .collect::<Result<_, _>>()?,
                )
            } else if let Some(s) = req.get("subject").filter(|v| !v.is_null()) {
                Some(vec![
                    s.as_u64().ok_or("conformance.subject must be a node id")?,
                ])
            } else {
                None
            };
        let only: Option<Vec<&str>> = match req.get("only") {
            None | Some(Value::Null) => None,
            Some(v) => {
                let arr = v
                    .as_array()
                    .ok_or("conformance.only must be an array of verdict names")?;
                let mut names = Vec::with_capacity(arr.len());
                for o in arr {
                    let name = o
                        .as_str()
                        .filter(|n| CONFORMANCE_OUTCOMES.contains(n))
                        .ok_or(format!(
                            "conformance.only entries must be one of {}",
                            CONFORMANCE_OUTCOMES.join(", ")
                        ))?;
                    names.push(name);
                }
                Some(names)
            }
        };
        let offset = req["offset"].as_u64().unwrap_or(0) as usize;
        let limit = req["limit"].as_u64().map(|l| l as usize);
        let cat = &self.schema.cat;
        let mut rows: Vec<(Option<&str>, conformance::Verdict)> = Vec::new();
        for (name, rule) in &rules {
            let verdicts = match &subjects {
                Some(s) => conformance::evaluate_subjects_assuming(
                    &self.snap, cat, rule, labels, s, &assume,
                ),
                None => conformance::evaluate_assuming(&self.snap, cat, rule, labels, &assume),
            };
            rows.extend(verdicts.into_iter().map(|v| (name.as_deref(), v)));
        }
        // Counts per verdict, and per reason of the NOT_APPLICABLE rows, over every row (each
        // visible subject, or each requested id), before `only`/paging.
        let zeros = |names: &[&str]| -> serde_json::Map<String, Value> {
            names.iter().map(|n| (n.to_string(), json!(0))).collect()
        };
        let mut counts = zeros(&CONFORMANCE_OUTCOMES);
        let mut reasons = zeros(&conformance::NotApplicableReason::ALL.map(|r| r.as_str()));
        let bump = |m: &mut serde_json::Map<String, Value>, k: &str| {
            let c = &mut m[k];
            *c = json!(c.as_u64().unwrap_or(0) + 1);
        };
        for (_, r) in &rows {
            bump(&mut counts, r.verdict.as_str());
            if let Some(reason) = r.reason {
                bump(&mut reasons, reason.as_str());
            }
        }
        let selected: Vec<&(Option<&str>, conformance::Verdict)> = rows
            .iter()
            .filter(|(_, r)| {
                only.as_ref()
                    .is_none_or(|o| o.contains(&r.verdict.as_str()))
            })
            .collect();
        let total = selected.len();
        let out: Vec<Value> = selected
            .into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .map(|(name, v)| {
                let mut j = verdict_json(v);
                if let Some(n) = name {
                    j["rule"] = json!(n);
                }
                j
            })
            .collect();
        let returned = out.len();
        Ok(json!({
            "verdicts": out,
            "total": total,
            "returned": returned,
            "truncated": offset.saturating_add(returned) < total,
            "counts": counts,
            "reasons": reasons,
            "missing": top_missing,
        }))
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
        let labels = req_labels(req);
        let facts = self.facts(labels);
        let incomplete: Vec<Value> =
            completeness::evaluate(&facts, &self.schema.cat, type_name, &required, labels)
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
        // hit content and dates are read through the caller's fact view: a hidden literal is
        // neither returned nor used in the assembled context
        let facts = self.facts(req_labels(req));
        // gather (node, score, date, content) — current values (fold LWW resolves supersession)
        let mut rows: Vec<(u64, f32, Option<i64>, String)> = t
            .ids
            .iter()
            .zip(&t.scores)
            .map(|(&n, &sc)| {
                let content = match query::point_one(&facts, n, content_p) {
                    Some(ObjKey::Text(s)) => s,
                    _ => String::new(),
                };
                let date = date_p.and_then(|dp| match query::point_one(&facts, n, dp) {
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

/// Vectors sampled (deterministic stride over the whole corpus) when training the index quantizers.
const INDEX_TRAIN_SAMPLE: usize = 20_000;

/// Fit-ratio ceiling for reusing trained quantizers on rebuild: at or below it the quantizers still
/// describe the corpus and k-means is skipped; above it the rebuild retrains on a fresh sample.
const INDEX_DRIFT_RATIO_MAX: f32 = 1.5;

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
/// A short window around the first (case-insensitive) occurrence of `needle_lc` in `s`, so a hit
/// inside a long text value (a rule body, a document excerpt) stays readable in a result list.
/// Char-based slicing — the window may drift slightly for case-folds that change length, but it
/// never panics and always contains readable context.
fn excerpt(s: &str, needle_lc: &str) -> String {
    const CTX: usize = 80;
    let total = s.chars().count();
    if total <= 2 * CTX {
        return s.to_string();
    }
    let at = s.to_lowercase().find(needle_lc).unwrap_or(0);
    let match_char = s.to_lowercase()[..at].chars().count();
    let start = match_char.saturating_sub(CTX);
    let end = (match_char + needle_lc.chars().count() + CTX).min(total);
    let body: String = s.chars().skip(start).take(end - start).collect();
    format!(
        "{}{}{}",
        if start > 0 { "…" } else { "" },
        body,
        if end < total { "…" } else { "" }
    )
}

/// Transaction time of each subject's earliest stored row, the creation-order key behind
/// `order: "recent"`. Nodes with no fact rows are absent (callers treat them as oldest).
fn first_seen(snap: &Snapshot) -> HashMap<NodeId, u64> {
    let mut m: HashMap<NodeId, u64> = HashMap::new();
    let mut note = |n: NodeId, rows: &[stromadb_core::fold::VersionRow]| {
        if let Some(tx) = rows.iter().map(|r| r.0.tx).min() {
            m.entry(n).and_modify(|t| *t = (*t).min(tx)).or_insert(tx);
        }
    };
    for (&(n, _), rows) in &snap.one_history {
        note(n, rows);
    }
    for (&(n, _), elems) in &snap.many_history {
        for rows in elems.values() {
            note(n, rows);
        }
    }
    m
}

/// Sort `ids` newest-first by `first_seen`; ties and unseen nodes fall back to descending id.
fn sort_recent(ids: &mut [NodeId], seen: &HashMap<NodeId, u64>) {
    ids.sort_unstable_by_key(|n| std::cmp::Reverse((seen.get(n).copied().unwrap_or(0), *n)));
}

fn is_recent(req: &Value) -> bool {
    req["order"].as_str() == Some("recent")
}

fn node_ids(snap: &Snapshot) -> Vec<NodeId> {
    let mut s: BTreeSet<NodeId> = snap.node_types.keys().copied().collect();
    s.extend(snap.node_labels.keys().copied());
    s.into_iter().collect()
}

/// The distinct access labels in use, sorted ascending: on nodes, on stored fact rows, and as
/// predicate label floors — every label a mask can hide something with.
fn labels_in_use(snap: &Snapshot, cat: &Catalog) -> Vec<u8> {
    let mut s: BTreeSet<u8> = snap.node_labels.values().copied().collect();
    s.extend(snap.fact_label_counts.keys().copied());
    s.extend(cat.predicates().filter_map(|p| p.label_floor));
    s.into_iter().collect()
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
            b.as_chunks::<4>()
                .0
                .iter()
                .map(|c| f32::from_le_bytes(*c))
                .collect()
        })
        .unwrap_or_default()
}

fn read_u64(p: &Path) -> Vec<u64> {
    fs::read(p)
        .map(|b| {
            b.as_chunks::<8>()
                .0
                .iter()
                .map(|c| u64::from_le_bytes(*c))
                .collect()
        })
        .unwrap_or_default()
}

fn apply_def(schema: &mut Schema, v: &Value) -> DbResult<()> {
    // A `source_def` registers a provenance source name — sent by a client up front (SPEC §2) or
    // appended by `WriteState::source_id` when a fact's `source` interns a new name. Replaying it
    // here, interleaved with the type/pred defs in `schema.jsonl`, re-interns it in the same order
    // so the numeric `source` the WAL stores resolves back to this name after a reopen.
    if let Some(sd) = v.get("source_def") {
        let id = schema
            .cat
            .intern_ref(sd["name"].as_str().ok_or("source_def.name missing")?);
        schema.sources.insert(id);
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
        // Optional access-label floor. Unlike the presentation fields below it is sticky: a re-sent
        // def that omits `label_floor` keeps the declared floor (a connector re-sending its schema
        // must not silently lower it); an explicit `null` clears it.
        let floor_before = schema
            .cat
            .field_id(name)
            .and_then(|id| schema.cat.predicate(id))
            .and_then(|d| d.label_floor);
        let label_floor = match p.get("label_floor") {
            None => floor_before,
            Some(Value::Null) => None,
            Some(l) => Some(parse_label(l, "pred_def.label_floor")?),
        };
        let pid = schema
            .cat
            .register_predicate(name, c, props, domain_id, range);
        schema.cat.set_label_floor(pid, label_floor);
        // Optional display flag: this predicate's text value labels its subject node in graph views.
        // Presentation metadata, not a constraint — unlike cardinality, a re-sent def may change it
        // (register_predicate rebuilds the def from this line, so the latest declaration wins).
        schema
            .cat
            .set_display(pid, p["display"].as_bool().unwrap_or(false));
        // Optional human-friendly label (e.g. a connector's opaque generated predicate name, such
        // as `backlog-cf-900001`): presentation metadata, latest declaration wins, same as display.
        schema
            .cat
            .set_label(pid, p["label"].as_str().map(str::to_string));
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
    schema.rules.insert(
        name,
        StoredRule {
            rule,
            definition: rd["rule"].clone(),
        },
    );
    Ok(())
}

/// A typed `{node|int|float|text|bool}` object → its ObjKey. A `float` is keyed by the bits of the
/// `f64` the JSON number parses to, with no narrowing: the fold key, the WAL/snapshot codec, and the
/// conformance range comparison all carry the full 64 bits, so a value such as `16777217.0` or
/// `1234567.89` reads back and compares exactly as ingested.
fn obj_key(v: &Value) -> DbResult<ObjKey> {
    if let Some(n) = v.get("node").and_then(|x| x.as_u64()) {
        return Ok(ObjKey::Node(n));
    }
    if let Some(i) = v.get("int").and_then(|x| x.as_i64()) {
        return Ok(ObjKey::Int(i));
    }
    if let Some(f) = v.get("float").and_then(|x| x.as_f64()) {
        return Ok(ObjKey::Float(f.to_bits()));
    }
    if let Some(t) = v.get("text").and_then(|x| x.as_str()) {
        return Ok(ObjKey::Text(t.to_string()));
    }
    if let Some(b) = v.get("bool").and_then(|x| x.as_bool()) {
        return Ok(ObjKey::Bool(b));
    }
    Err("object must be one of {node|int|float|text|bool}".into())
}

/// An edge-property value → literal ObjKey. Accepts both shapes SPEC shows: a bare JSON scalar
/// (`{"level": 5, "role": "lead"}`) and the single-key typed object used everywhere else
/// (`{"role": {"text": "lead"}}`). `node` stays excluded — edge properties are literals.
fn value_key(v: &Value) -> DbResult<ObjKey> {
    match v {
        Value::Bool(b) => Ok(ObjKey::Bool(*b)),
        Value::String(s) => Ok(ObjKey::Text(s.clone())),
        Value::Number(n) if n.is_i64() => Ok(ObjKey::Int(n.as_i64().unwrap())),
        Value::Number(n) if n.is_u64() => Ok(ObjKey::Int(n.as_u64().unwrap() as i64)),
        Value::Number(n) => Ok(ObjKey::Float(n.as_f64().unwrap().to_bits())),
        Value::Object(_) if v.get("node").is_none() => obj_key(v),
        _ => Err(
            "edge-property value must be a literal: a number/string/bool, or {int|float|text|bool}"
                .into(),
        ),
    }
}

/// The most items one batched `lookup` (`queries` / `values`) answers.
const LOOKUP_BATCH_MAX: usize = 1000;

/// A lookup value: a bare scalar (a string is text) or the ingest object form.
fn lookup_scalar(raw: &Value) -> DbResult<ObjKey> {
    match raw {
        Value::Null => Err("lookup.value missing".into()),
        Value::Object(_) => obj_key(raw),
        _ => value_key(raw),
    }
}

/// The value a lookup request (or one batch item) asks for: `value`, or its alias `equals`.
fn lookup_value(req: &Value) -> DbResult<ObjKey> {
    if req["value"].is_null() {
        lookup_scalar(&req["equals"])
    } else {
        lookup_scalar(&req["value"])
    }
}

/// Refuse a rule whose derived paths can never resolve against the declared predicates (a
/// many-cardinality hop, or a literal-valued hop before the last; see [`conformance::path_errors`]).
fn check_rule_paths(rule: &conformance::Rule, cat: &Catalog) -> DbResult<()> {
    let errors = conformance::path_errors(rule, cat);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "conformance rule path cannot resolve: {}",
            errors.join("; ")
        ))
    }
}

/// Parse a conformance rule from its JSON declaration into the name-based [`conformance::Rule`]
/// (names are resolved to field ids later, at evaluation, via the catalog). `required` and
/// `distinct_from` are each optional derived paths; `cases` (an ordered first-match list of
/// `{when?, required?}`) replaces `required` for a banded rule. A rule must declare `required`,
/// `cases`, or `distinct_from`, and may not declare both `required` and `cases`.
fn parse_conformance_rule(v: &Value) -> DbResult<conformance::Rule> {
    let subject_type = v["subject_type"]
        .as_str()
        .ok_or("rule.subject_type missing")?
        .to_string();
    let actual = v["actual"]
        .as_str()
        .ok_or("rule.actual missing")?
        .to_string();
    let required = parse_conformance_hops(&v["required"], "required")?;
    let distinct_from = parse_conformance_hops(&v["distinct_from"], "distinct_from")?;
    let cases = parse_conformance_cases(&v["cases"])?;
    if !required.is_empty() && !cases.is_empty() {
        return Err("rule declares both required and cases; put the default path in a final case without 'when'".into());
    }
    if required.is_empty() && cases.is_empty() && distinct_from.is_empty() {
        return Err("rule must declare required.hops, cases, and/or distinct_from.hops".into());
    }
    Ok(conformance::Rule {
        subject_type,
        scope: parse_conformance_cond(&v["scope"], "scope")?,
        required,
        cases,
        distinct_from,
        actual,
        absent_when: parse_conformance_cond(&v["absent_when"], "absent_when")?,
    })
}

/// Parse an optional `cases: [ { "when"?: condition, "required"?: {"hops": [..]} }, .. ]` list
/// (absent/null → empty). An empty array is rejected: it would make every subject inapplicable.
fn parse_conformance_cases(v: &Value) -> DbResult<Vec<conformance::Case>> {
    if v.is_null() {
        return Ok(Vec::new());
    }
    let arr = v
        .as_array()
        .ok_or("rule.cases must be an array of {when?, required?}")?;
    if arr.is_empty() {
        return Err("rule.cases must not be empty".into());
    }
    arr.iter()
        .enumerate()
        .map(|(i, c)| {
            if !c.is_object() {
                return Err(format!(
                    "rule.cases[{i}] must be an object {{when?, required?}}"
                ));
            }
            Ok(conformance::Case {
                when: parse_conformance_cond(&c["when"], &format!("cases[{i}].when"))?,
                required: parse_conformance_hops(&c["required"], &format!("cases[{i}].required"))?,
            })
        })
        .collect()
}

/// Parse an optional derived path `{ "hops": [ { "predicate": name, "as_of"?: anchor }, .. ] }`
/// (absent/null → empty, no expectation on that side).
fn parse_conformance_hops(v: &Value, what: &str) -> DbResult<Vec<conformance::Hop>> {
    if v.is_null() {
        return Ok(Vec::new());
    }
    let hops_v = v["hops"]
        .as_array()
        .ok_or(format!("rule.{what}.hops missing"))?;
    let mut hops = Vec::with_capacity(hops_v.len());
    for h in hops_v {
        let predicate = h["predicate"]
            .as_str()
            .ok_or(format!("rule.{what} hop predicate missing"))?
            .to_string();
        let as_of = h.get("as_of").and_then(|a| a.as_str()).map(str::to_string);
        hops.push(conformance::Hop { predicate, as_of });
    }
    Ok(hops)
}

/// Parse an optional condition (absent/null → `None`): `{ "predicate": name, "as_of"?: anchor, .. }`
/// with exactly one test — `"equals": value`, or a numeric range given by `gt`/`gte` (lower) and/or
/// `lt`/`lte` (upper), or `"between": [lo, hi]` (both inclusive). `equals` takes either the ingest
/// object form (`{"node": N}` / `{"int": …}` / `{"text": …}` / `{"bool": …}` — the documented shape,
/// required for node-valued conditions) or a bare scalar; range bounds must be numbers (bare or
/// `{"int"|"float": …}`). `as_of` names an integer predicate on the subject whose value is the
/// valid-time instant the condition's predicate is read at.
fn parse_conformance_cond(v: &Value, what: &str) -> DbResult<Option<conformance::Cond>> {
    if v.is_null() {
        return Ok(None);
    }
    let predicate = v["predicate"]
        .as_str()
        .ok_or(format!("rule.{what}.predicate missing"))?
        .to_string();
    let as_of = match v.get("as_of") {
        None | Some(Value::Null) => None,
        Some(a) => Some(
            a.as_str()
                .ok_or(format!("rule.{what}.as_of must be a predicate name"))?
                .to_string(),
        ),
    };
    let bound = |key: &str| -> DbResult<Option<ObjKey>> {
        match v.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(b) => numeric_bound(b)
                .map(Some)
                .map_err(|e| format!("rule.{what}.{key}: {e}")),
        }
    };
    let (gt, gte, lt, lte) = (bound("gt")?, bound("gte")?, bound("lt")?, bound("lte")?);
    let between = match v.get("between") {
        None | Some(Value::Null) => None,
        Some(b) => {
            let pair = b
                .as_array()
                .filter(|a| a.len() == 2)
                .ok_or(format!("rule.{what}.between must be [lo, hi]"))?;
            let lo = numeric_bound(&pair[0]).map_err(|e| format!("rule.{what}.between: {e}"))?;
            let hi = numeric_bound(&pair[1]).map_err(|e| format!("rule.{what}.between: {e}"))?;
            Some((lo, hi))
        }
    };
    let has_equals = v.get("equals").is_some_and(|e| !e.is_null());
    let has_range = gt.is_some() || gte.is_some() || lt.is_some() || lte.is_some();
    let tests = usize::from(has_equals) + usize::from(has_range) + usize::from(between.is_some());
    if tests != 1 {
        return Err(format!(
            "rule.{what} needs exactly one test: equals, a range (gt/gte/lt/lte), or between"
        ));
    }
    if gt.is_some() && gte.is_some() {
        return Err(format!("rule.{what} declares both gt and gte"));
    }
    if lt.is_some() && lte.is_some() {
        return Err(format!("rule.{what} declares both lt and lte"));
    }
    let mk = |value: ObjKey, inclusive: bool| conformance::Bound { value, inclusive };
    let test = if has_equals {
        conformance::Test::Equals(if v["equals"].is_object() {
            obj_key(&v["equals"])?
        } else {
            value_key(&v["equals"])?
        })
    } else if let Some((lo, hi)) = between {
        conformance::Test::Range(conformance::NumRange {
            lower: Some(mk(lo, true)),
            upper: Some(mk(hi, true)),
        })
    } else {
        conformance::Test::Range(conformance::NumRange {
            lower: gt.map(|b| mk(b, false)).or(gte.map(|b| mk(b, true))),
            upper: lt.map(|b| mk(b, false)).or(lte.map(|b| mk(b, true))),
        })
    };
    Ok(Some(conformance::Cond {
        predicate,
        test,
        as_of,
    }))
}

/// A numeric range bound: a bare JSON number or `{"int": …}` / `{"float": …}`, keyed exactly as the
/// ingest path keys a stored value of that form (so a bound and a stored value compare alike).
fn numeric_bound(v: &Value) -> DbResult<ObjKey> {
    let key = if v.is_object() {
        obj_key(v)?
    } else {
        value_key(v)?
    };
    match key {
        ObjKey::Int(_) => Ok(key),
        ObjKey::Float(bits) if f64::from_bits(bits).is_finite() => Ok(key),
        _ => Err("a range bound must be a finite number".into()),
    }
}

/// The wire names of every conformance outcome, in `counts` order.
const CONFORMANCE_OUTCOMES: [&str; 4] = ["OK", "ABSENT", "MISMATCH", "NOT_APPLICABLE"];

/// One verdict in the wire shape shared by the `conformance` op, `conformance_watch`, and the
/// `conformance_changes` diff entries.
fn verdict_json(v: &conformance::Verdict) -> Value {
    let mut out = json!({
        "subject": v.subject,
        "verdict": v.verdict.as_str(),
        "kind": v.mismatch_kind.map(|k| k.as_str()),
        "required": v.required.clone().map(fmt_obj),
        "distinct": v.distinct.clone().map(fmt_obj),
        "actual": v.actual.clone().map(fmt_obj),
        "as_of": v.as_of,
        "case": v.case,
        "missing": v.missing.iter().map(missing_json).collect::<Vec<Value>>(),
        "assumed": v.assumed,
    });
    // present exactly on NOT_APPLICABLE rows
    if let Some(reason) = v.reason {
        out["reason"] = json!(reason.as_str());
    }
    out
}

fn missing_json(m: &conformance::Missing) -> Value {
    match m {
        conformance::Missing::Anchor { node, predicate }
        | conformance::Missing::Hop { node, predicate } => {
            json!({ "kind": m.kind(), "node": node, "predicate": predicate })
        }
    }
}

/// Parse an optional `assume` map `{ "<predicate name>": instant, .. }` (absent/null → empty). An
/// instant is a bare integer or `{"int": N}`; an unknown predicate name is a clear error.
fn parse_assumptions(v: &Value, cat: &Catalog) -> DbResult<conformance::Assumptions> {
    let mut out = conformance::Assumptions::new();
    if v.is_null() {
        return Ok(out);
    }
    let map = v
        .as_object()
        .ok_or("conformance.assume must be an object of { predicate: instant }")?;
    for (name, val) in map {
        if cat.field_id(name).is_none() {
            return Err(format!("unknown predicate in conformance.assume: {name}"));
        }
        let t = val
            .as_i64()
            .or_else(|| val.get("int").and_then(Value::as_i64))
            .ok_or(format!(
                "conformance.assume.{name} must be an integer instant"
            ))?;
        out.insert(name.clone(), t);
    }
    Ok(out)
}

/// A request's `allowed_labels` bitmask (absent = every label).
fn req_labels(req: &Value) -> u32 {
    req["allowed_labels"]
        .as_u64()
        .map(|m| m as u32)
        .unwrap_or(u32::MAX)
}

/// An access label: an integer in `0..=31` (one bit of the 32-bit `allowed_labels` mask).
fn parse_label(v: &Value, what: &str) -> DbResult<u8> {
    v.as_u64()
        .filter(|&l| l <= mask::MAX_LABEL as u64)
        .map(|l| l as u8)
        .ok_or(format!(
            "{what} must be an integer label 0..={}",
            mask::MAX_LABEL
        ))
}

/// The optional `label` of a fact or close record.
fn record_label(r: &Value, what: &str) -> DbResult<Option<u8>> {
    match r.get("label") {
        None | Some(Value::Null) => Ok(None),
        Some(l) => parse_label(l, &format!("{what}.label")).map(Some),
    }
}

/// The access label stored on the row `ok` of key `(subject, predicate)`, if any.
fn row_label(
    snap: &Snapshot,
    subject: NodeId,
    predicate: FieldId,
    ok: &stromadb_core::fold::OrderKey,
) -> Option<u8> {
    snap.fact_labels
        .get(&(subject, predicate))
        .and_then(|m| m.get(ok))
        .copied()
}

/// Whether `node` is visible to a principal with `allowed_labels` (unlabeled = public) — the same
/// bit-test the read ops apply.
fn label_visible(snap: &Snapshot, node: NodeId, allowed_labels: u32) -> bool {
    snap.node_labels
        .get(&node)
        .is_none_or(|&l| (allowed_labels >> l) & 1 == 1)
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

#[cfg(test)]
mod stats_tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("stroma_stats_{tag}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    /// The maintained counters must equal a full recount of the published snapshot.
    fn assert_exact(db: &Db) {
        let rs = db.read_state();
        let mut labels: BTreeMap<u8, u64> = BTreeMap::new();
        for &l in rs.snap.node_labels.values() {
            *labels.entry(l).or_insert(0) += 1;
        }
        let expect_labels: serde_json::Map<String, Value> = labels
            .iter()
            .map(|(l, n)| (l.to_string(), json!(n)))
            .collect();
        let s = db.stats();
        assert_eq!(s["schema"]["nodes"], json!(node_ids(&rs.snap).len()));
        assert_eq!(s["labels"], Value::Object(expect_labels));
        assert_eq!(s["facts"]["durable_head"], json!(rs.durable_head));
        assert_eq!(
            db.counts(),
            Counts {
                nodes: node_ids(&rs.snap).len() as u64,
                facts: rs.durable_head
            }
        );
    }

    #[test]
    fn counts_stay_exact_across_ingest_retract_compaction_and_reopen() {
        let dir = tmp("exact");
        let db = Db::open_or_init(&dir).unwrap();
        assert_exact(&db);
        db.ingest_str(concat!(
            "{\"type_def\":{\"name\":\"T\"}}\n",
            "{\"pred_def\":{\"name\":\"rel\",\"cardinality\":\"many\",\"domain\":\"T\",\"range\":\"T\"}}\n",
            "{\"node\":{\"id\":1,\"type\":\"T\",\"label\":0}}\n",
            "{\"node\":{\"id\":2,\"type\":\"T\"}}\n",
            "{\"node\":{\"id\":3,\"label\":2}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"rel\",\"object\":{\"node\":2}}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"rel\",\"object\":{\"node\":3}}}\n",
        ))
        .unwrap();
        assert_exact(&db);
        assert_eq!(db.counts().nodes, 3);
        // re-sent (suppressed) node, a label added to a typed node, a relabel, a new node
        db.ingest_str(concat!(
            "{\"node\":{\"id\":1,\"type\":\"T\",\"label\":0}}\n",
            "{\"node\":{\"id\":2,\"label\":2}}\n",
            "{\"node\":{\"id\":1,\"label\":3}}\n",
            "{\"node\":{\"id\":4,\"type\":\"T\",\"label\":3}}\n",
            "{\"retract\":{\"subject\":1,\"predicate\":\"rel\",\"object\":{\"node\":3}}}\n",
        ))
        .unwrap();
        assert_exact(&db);
        assert_eq!(db.counts().nodes, 4);
        assert_eq!(db.stats()["labels"], json!({"2": 2, "3": 2}));
        let wal_before = db.stats()["storage"]["wal_bytes"].as_u64().unwrap();
        db.compact().unwrap();
        assert_exact(&db);
        assert!(db.stats()["storage"]["wal_bytes"].as_u64().unwrap() < wal_before);
        db.ingest_str("{\"node\":{\"id\":5,\"type\":\"T\"}}\n")
            .unwrap();
        assert_exact(&db);
        let before = db.counts();
        drop(db);
        // close leaves an exact manifest for unopened listing
        assert_eq!(persisted_counts(&dir), Some(before));
        let db = Db::open(&dir).unwrap();
        assert_exact(&db);
        assert_eq!(db.counts(), before);
        db.reset().unwrap();
        assert_exact(&db);
        assert_eq!(persisted_counts(&dir), Some(Counts { nodes: 0, facts: 0 }));
        drop(db);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Structural lock-freedom: stats/counts must answer while a writer holds the write mutex (an
    /// ingest batch or index rebuild in flight). A regression to taking the lock deadlocks the
    /// reader, which the timeout turns into a failure instead of a hang.
    #[test]
    fn stats_never_waits_for_the_write_lock() {
        let dir = tmp("lockfree");
        let db = Db::open_or_init(&dir).unwrap();
        db.ingest_str("{\"type_def\":{\"name\":\"T\"}}\n{\"node\":{\"id\":1,\"type\":\"T\"}}\n")
            .unwrap();
        let held = db.write.lock().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::scope(|s| {
            s.spawn(|| {
                let st = db.stats();
                tx.send((st["schema"]["nodes"].clone(), db.counts()))
                    .unwrap();
            });
            let got = rx.recv_timeout(Duration::from_secs(30));
            drop(held);
            let (nodes, counts) = got.expect("stats() blocked on the write lock");
            assert_eq!(nodes, json!(1));
            assert_eq!(counts.nodes, 1);
        });
        drop(db);
        let _ = fs::remove_dir_all(&dir);
    }
}
