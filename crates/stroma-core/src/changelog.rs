//! The changelog: the append-only, version-authoritative write log (SoT).
//!
//! Every write is appended and assigned a monotonic `seqno` — the version authority. The seqno is
//! the basis of the fold's [`OrderKey`] (so deterministic replay reproduces the exact same state),
//! and the watermark other (derived) stores chase. Under overload the changelog returns explicit
//! [`Backpressure`] rather than stalling silently (CAP-1).
//!
//! This is the semantics layer. Durability is optional and slots in behind the same
//! append/replay/watermark contract: [`Changelog::open`] recovers from a framed WAL (see [`crate::wal`])
//! and [`Changelog::sync`] fsyncs the appended-but-not-yet-durable tail (group commit). The eventual
//! LSM backend (RocksDB/Speedb, rkyv zero-copy, O_DIRECT) replaces the file WAL behind this same
//! contract; [`Changelog::new`] keeps the pure in-memory mode for tests and ephemeral use.

use crate::catalog::Cardinality;
use crate::fact::{FieldId, NodeId};
use crate::fold::{Fold, ObjKey, Op, OrderKey};
use crate::wal;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// A write submitted to the changelog — a diff *without* its order key (the changelog assigns it).
/// `RemoveMany` carries the order keys it observed (resolved by the ingest layer from current tags).
#[derive(Clone, Debug)]
pub enum WriteKind {
    SetOne {
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
        valid_from: i64,
        /// End of the valid-time interval (`None` = open / currently valid). Used by valid-time
        /// as-of reads; the fold's LWW ordering is unaffected.
        valid_to: Option<i64>,
    },
    CloseOne {
        subject: NodeId,
        predicate: FieldId,
        valid_from: i64,
    },
    AddMany {
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
        /// Start of the element's valid-time interval (mirrors `SetOne`; `0` = unreported).
        valid_from: i64,
        /// End of the element's valid-time interval (`None` = open). Used by valid-time as-of
        /// reads; presence (the current set) and the OR-Set ordering are unaffected.
        valid_to: Option<i64>,
    },
    /// End ONE element's valid-time interval at `valid_from` — the cardinality-Many analogue of
    /// `CloseOne`. The element leaves the current set (when this row wins by order key) but its
    /// history stays sliceable; `RemoveMany` stays the history-destroying hard retract.
    CloseMany {
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
        valid_from: i64,
    },
    RemoveMany {
        subject: NodeId,
        predicate: FieldId,
        observed: Vec<OrderKey>,
    },
    HardDelete {
        subject: NodeId,
        predicate: FieldId,
        cardinality: Cardinality,
    },
    /// Set a property on the edge `(subject, predicate, object)`. The key is carried as a string
    /// (self-contained; not interned in the catalog, so it survives replay without a schema entry).
    /// Last-writer-wins per `(edge, key)` by order key.
    SetEdgeProp {
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
        key: String,
        value: ObjKey,
    },
    /// Set node `node`'s entity type; last-writer-wins by order key. Node-scoped (not
    /// `(subject, predicate)`-keyed).
    SetNodeType { node: NodeId, type_id: FieldId },
    /// Set node `node`'s ABAC sensitivity label; last-writer-wins by order key. Node-scoped.
    SetNodeLabel { node: NodeId, label: u8 },
}

#[derive(Clone, Debug)]
struct Record {
    source: FieldId,
    kind: WriteKind,
}

/// Returned when the unmaterialized backlog would exceed the bound — apply backpressure upstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Backpressure {
    pub unmaterialized: usize,
    pub limit: usize,
}

/// Append-only changelog. `seqno` = `base + index` in the log = version authority. `base` is `0`
/// for a log that has never been compacted; a snapshot+truncate compaction advances it to the
/// covered seqno, so seqnos stay globally continuous across the boundary.
pub struct Changelog {
    records: Vec<Record>,
    base: u64,         // seqno of records[0] (a compaction truncated everything before it)
    materialized: u64, // count of records a derived store has caught up to
    max_unmaterialized: usize,
    wal: Option<File>,     // durable backend; `None` = pure in-memory mode
    path: Option<PathBuf>, // the WAL's path (needed by compaction's file swaps)
    durable_upto: usize,   // records[..durable_upto] have been fsynced
}

fn record_to_op(seqno: u64, source: FieldId, kind: &WriteKind) -> Op {
    let ok = OrderKey {
        tx: seqno,
        source,
        seq: seqno,
    };
    match kind {
        WriteKind::SetOne {
            subject,
            predicate,
            object,
            valid_from,
            valid_to,
        } => Op::SetOne {
            subject: *subject,
            predicate: *predicate,
            object: object.clone(),
            valid_from: *valid_from,
            valid_to: *valid_to,
            ok,
        },
        WriteKind::CloseOne {
            subject,
            predicate,
            valid_from,
        } => Op::CloseOne {
            subject: *subject,
            predicate: *predicate,
            valid_from: *valid_from,
            ok,
        },
        WriteKind::AddMany {
            subject,
            predicate,
            object,
            valid_from,
            valid_to,
        } => Op::AddMany {
            subject: *subject,
            predicate: *predicate,
            object: object.clone(),
            valid_from: *valid_from,
            valid_to: *valid_to,
            ok,
        },
        WriteKind::CloseMany {
            subject,
            predicate,
            object,
            valid_from,
        } => Op::CloseMany {
            subject: *subject,
            predicate: *predicate,
            object: object.clone(),
            valid_from: *valid_from,
            ok,
        },
        WriteKind::RemoveMany {
            subject,
            predicate,
            observed,
        } => Op::RemoveMany {
            subject: *subject,
            predicate: *predicate,
            observed: observed.clone(),
        },
        WriteKind::HardDelete {
            subject,
            predicate,
            cardinality,
        } => Op::HardDelete {
            subject: *subject,
            predicate: *predicate,
            ok,
            cardinality: *cardinality,
        },
        WriteKind::SetEdgeProp {
            subject,
            predicate,
            object,
            key,
            value,
        } => Op::SetEdgeProp {
            subject: *subject,
            predicate: *predicate,
            object: object.clone(),
            key: key.clone(),
            value: value.clone(),
            ok,
        },
        WriteKind::SetNodeType { node, type_id } => Op::SetNodeType {
            node: *node,
            type_id: *type_id,
            ok,
        },
        WriteKind::SetNodeLabel { node, label } => Op::SetNodeLabel {
            node: *node,
            label: *label,
            ok,
        },
    }
}

// --- compaction snapshot file ------------------------------------------------------------------
// `[magic "SSNP"][version u8 = 1][covered_seqno u64 LE][fold_len u64 LE][crc32 u32 LE][fold bytes]`
// The fold bytes are the verbatim fold state (Fold::encode_into — superseded rows, tombstones and
// order keys included, so as-of reads and LWW tie-breaks survive the boundary).

const SNAP_MAGIC: &[u8; 4] = b"SSNP";

/// Encode a compaction snapshot covering everything below `upto`.
pub fn encode_snapshot(fold: &Fold, upto: u64) -> Vec<u8> {
    let mut fold_bytes = Vec::new();
    fold.encode_into(&mut fold_bytes);
    let mut out = Vec::with_capacity(fold_bytes.len() + 25);
    out.extend_from_slice(SNAP_MAGIC);
    out.push(1);
    out.extend_from_slice(&upto.to_le_bytes());
    out.extend_from_slice(&(fold_bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(&wal::checksum(&fold_bytes).to_le_bytes());
    out.extend_from_slice(&fold_bytes);
    out
}

/// Decode a compaction snapshot → `(covered_seqno, fold)`. `None` on any malformation — magic,
/// version, length, checksum, or fold decode.
pub fn decode_snapshot(bytes: &[u8]) -> Option<(u64, Fold)> {
    if bytes.len() < 25 || &bytes[..4] != SNAP_MAGIC || bytes[4] != 1 {
        return None;
    }
    let upto = u64::from_le_bytes(bytes[5..13].try_into().ok()?);
    let len = u64::from_le_bytes(bytes[13..21].try_into().ok()?) as usize;
    let crc = u32::from_le_bytes(bytes[21..25].try_into().ok()?);
    let fold_bytes = bytes.get(25..)?;
    if fold_bytes.len() != len || wal::checksum(fold_bytes) != crc {
        return None;
    }
    Some((upto, Fold::decode(fold_bytes)?))
}

/// Read the compaction snapshot next to `wal_path`, if any. A missing file is a normal state
/// (never compacted → `None`); a PRESENT but malformed one is an error — the WAL prefix it covered
/// is gone, so guessing would silently drop history.
pub fn read_snapshot(wal_path: &Path) -> io::Result<Option<(u64, Fold)>> {
    let snap = Changelog::snapshot_path(wal_path);
    let bytes = match std::fs::read(&snap) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    decode_snapshot(&bytes).map(Some).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("corrupt compaction snapshot at {}", snap.display()),
        )
    })
}

impl Changelog {
    /// `max_unmaterialized` bounds the in-flight (appended but not-yet-materialized) backlog.
    /// Pure in-memory mode (no durability) — see [`Changelog::open`] for the durable variant.
    pub fn new(max_unmaterialized: usize) -> Self {
        Changelog {
            records: Vec::new(),
            base: 0,
            materialized: 0,
            max_unmaterialized,
            wal: None,
            path: None,
            durable_upto: 0,
        }
    }

    /// Open a durable changelog backed by the framed WAL at `path`, recovering any committed prefix
    /// (a torn tail from a crash mid-append is dropped — see [`crate::wal::recover`]). The recovered
    /// records count as already materialized and already durable, so a fresh open neither backpressures
    /// nor re-fsyncs them; new appends are made durable by [`Changelog::sync`]. A missing file starts
    /// an empty database.
    pub fn open(path: impl AsRef<Path>, max_unmaterialized: usize) -> io::Result<Self> {
        let path = path.as_ref();
        let (base, recovered) = wal::recover(path)?;
        let records: Vec<Record> = recovered
            .into_iter()
            .map(|(source, kind)| Record { source, kind })
            .collect();
        let n = records.len();
        let wal = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Changelog {
            records,
            base,
            materialized: n as u64,
            max_unmaterialized,
            wal: Some(wal),
            path: Some(path.to_path_buf()),
            durable_upto: n,
        })
    }

    /// Seqno of the first record still in this log — `0` until a compaction truncates a prefix.
    pub fn base(&self) -> u64 {
        self.base
    }

    /// The sibling path a compaction snapshot lives at (`<wal>.snap`).
    pub fn snapshot_path(wal_path: &Path) -> PathBuf {
        let mut p = wal_path.as_os_str().to_owned();
        p.push(".snap");
        PathBuf::from(p)
    }

    /// Snapshot + truncate: persist `snapshot_bytes` (the encoded fold + the seqno it covers,
    /// built by the engine) as `<wal>.snap`, archive the covered WAL as `<wal>.archive-<S>`, and
    /// start a fresh WAL whose first frame names `S` — so seqnos stay continuous and any crash
    /// window self-reconciles on open (a committed snapshot next to a not-yet-truncated WAL just
    /// replays from `S`, skipping the stale prefix by seqno). Caller contract: every record is
    /// already materialized AND durable (the engine drains + syncs first). Returns `S`.
    pub fn compact_to(&mut self, snapshot_bytes: &[u8]) -> io::Result<u64> {
        let Some(path) = self.path.clone() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "in-memory changelog has no WAL to compact",
            ));
        };
        if self.durable_upto < self.records.len()
            || (self.materialized as usize) < self.records.len()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "compact requires a fully materialized, fully synced log",
            ));
        }
        let upto = self.head();

        // 1. commit the snapshot: tmp + fsync + atomic rename — after this instant the log prefix
        //    is redundant even if the truncation below never happens (open replays from `upto`).
        let snap = Self::snapshot_path(&path);
        let tmp = {
            let mut p = snap.as_os_str().to_owned();
            p.push(".tmp");
            PathBuf::from(p)
        };
        {
            let mut f = File::create(&tmp)?;
            f.write_all(snapshot_bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &snap)?;

        // 2. truncate: archive the covered WAL, start a fresh one whose first frame names `upto`.
        self.wal = None; // close the handle before the rename
        let archive = {
            let mut p = path.as_os_str().to_owned();
            p.push(format!(".archive-{upto}"));
            PathBuf::from(p)
        };
        std::fs::rename(&path, &archive)?;
        let mut buf = Vec::new();
        wal::frame_wal_start(&mut buf, upto);
        let mut f = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        f.write_all(&buf)?;
        f.sync_all()?;
        self.wal = Some(f);
        self.records.clear();
        self.base = upto;
        self.materialized = 0;
        self.durable_upto = 0;
        Ok(upto)
    }

    /// Make every appended record durable: frame the `[durable_upto, head)` tail and `fsync` it
    /// (group commit — the caller decides the commit boundary, typically per ETL chunk). A crash
    /// before `sync` loses only the un-synced tail, never a synced prefix. No-op in in-memory mode.
    pub fn sync(&mut self) -> io::Result<()> {
        if self.wal.is_none() || self.durable_upto >= self.records.len() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for r in &self.records[self.durable_upto..] {
            wal::frame_record(&mut buf, r.source, &r.kind);
        }
        let file = self.wal.as_mut().unwrap();
        file.write_all(&buf)?;
        file.sync_all()?;
        self.durable_upto = self.records.len();
        Ok(())
    }

    /// Seqno up to which records are guaranteed durable (fsynced). Equals [`Changelog::head`] right
    /// after a successful [`Changelog::sync`]; `0` in in-memory mode.
    pub fn durable_head(&self) -> u64 {
        if self.wal.is_none() {
            return 0;
        }
        self.base + self.durable_upto as u64
    }

    /// Append a write; returns its assigned `seqno`, or [`Backpressure`] if the backlog is full.
    pub fn append(&mut self, source: FieldId, kind: WriteKind) -> Result<u64, Backpressure> {
        let unmaterialized = self.records.len() - self.materialized as usize;
        if unmaterialized >= self.max_unmaterialized {
            return Err(Backpressure {
                unmaterialized,
                limit: self.max_unmaterialized,
            });
        }
        let seqno = self.base + self.records.len() as u64;
        self.records.push(Record { source, kind });
        Ok(seqno)
    }

    /// Append a chunk of writes atomically w.r.t. backpressure (all-or-nothing). Returns their
    /// seqnos in order. This is the ETL chunk/batch receiver (a chunk becomes one append).
    pub fn append_batch(
        &mut self,
        writes: Vec<(FieldId, WriteKind)>,
    ) -> Result<Vec<u64>, Backpressure> {
        let unmaterialized = self.records.len() - self.materialized as usize;
        if unmaterialized + writes.len() > self.max_unmaterialized {
            return Err(Backpressure {
                unmaterialized: unmaterialized + writes.len(),
                limit: self.max_unmaterialized,
            });
        }
        let mut seqnos = Vec::with_capacity(writes.len());
        for (source, kind) in writes {
            let seqno = self.base + self.records.len() as u64;
            self.records.push(Record { source, kind });
            seqnos.push(seqno);
        }
        Ok(seqnos)
    }

    /// Next seqno that will be assigned (`base +` current length).
    pub fn head(&self) -> u64 {
        self.base + self.records.len() as u64
    }

    /// Records appended but not yet materialized by a derived store.
    pub fn unmaterialized(&self) -> usize {
        self.records.len() - self.materialized as usize
    }

    /// Advance the materialized watermark (a derived store reports its progress, relieving backpressure).
    pub fn mark_materialized(&mut self, up_to: u64) {
        let rel = up_to.saturating_sub(self.base);
        self.materialized = rel.min(self.records.len() as u64).max(self.materialized);
    }

    /// Deterministically rebuild state by folding the whole log in seqno order.
    pub fn replay(&self) -> Fold {
        let mut f = Fold::default();
        self.replay_into(&mut f);
        f
    }

    /// Fold the whole log into an existing fold.
    pub fn replay_into(&self, fold: &mut Fold) {
        self.replay_range_into(0, fold);
    }

    /// Fold records `[from, head)` into an existing fold (incremental catch-up / read-merge tail).
    pub fn replay_range_into(&self, from: u64, fold: &mut Fold) {
        self.replay_range_into_tracked(from, fold);
    }

    /// Like [`Changelog::replay_range_into`], but returns what the range touched — the input for
    /// incremental snapshot refresh: the `(subject, predicate)` graph keys (for
    /// [`Fold::observe_key_into`]) and, separately, the node ids whose attributes changed (for
    /// [`Fold::observe_node_into`]). Node-attribute ops are routed by node and never appear in the
    /// graph key set.
    pub fn replay_range_into_tracked(
        &self,
        from: u64,
        fold: &mut Fold,
    ) -> (
        std::collections::BTreeSet<(NodeId, FieldId)>,
        std::collections::BTreeSet<NodeId>,
    ) {
        let mut keys = std::collections::BTreeSet::new();
        let mut nodes = std::collections::BTreeSet::new();
        let start = (from.saturating_sub(self.base) as usize).min(self.records.len());
        for (offset, r) in self.records[start..].iter().enumerate() {
            let seqno = self.base + (start + offset) as u64;
            let op = record_to_op(seqno, r.source, &r.kind);
            match op.node_attr_node() {
                Some(node) => {
                    nodes.insert(node);
                }
                None => {
                    keys.insert(op.key());
                }
            }
            fold.apply(&op);
        }
        (keys, nodes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(s: NodeId, p: FieldId, o: u64) -> WriteKind {
        WriteKind::AddMany {
            subject: s,
            predicate: p,
            object: ObjKey::Node(o),
            valid_from: 0,
            valid_to: None,
        }
    }

    #[test]
    fn append_assigns_monotonic_seqno() {
        let mut log = Changelog::new(1024);
        assert_eq!(log.append(0, add(1, 100, 2)), Ok(0));
        assert_eq!(log.append(0, add(1, 100, 3)), Ok(1));
        assert_eq!(log.head(), 2);
    }

    #[test]
    fn replay_is_deterministic_and_matches_fold() {
        let mut log = Changelog::new(1024);
        log.append(0, add(1, 100, 2)).unwrap();
        log.append(1, add(1, 100, 3)).unwrap();
        log.append(
            0,
            WriteKind::SetOne {
                subject: 1,
                predicate: 0,
                object: ObjKey::Node(9),
                valid_from: 0,
                valid_to: None,
            },
        )
        .unwrap();
        let a = log.replay().observe();
        let b = log.replay().observe();
        assert_eq!(a, b);
        // many{(1,100)} = {2,3}; one{(1,0)} = 9
        assert_eq!(
            a.many.get(&(1, 100)).unwrap(),
            &[ObjKey::Node(2), ObjKey::Node(3)].into_iter().collect()
        );
        assert_eq!(a.one.get(&(1, 0)), Some(&Some(ObjKey::Node(9))));
    }

    fn tmp(name: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("stroma_wal_test_{name}.log"));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn seed(log: &mut Changelog) {
        log.append(0, add(1, 100, 2)).unwrap();
        log.append(1, add(1, 100, 3)).unwrap();
        log.append(
            0,
            WriteKind::SetOne {
                subject: 1,
                predicate: 0,
                object: ObjKey::Text("v".into()),
                valid_from: 5,
                valid_to: None,
            },
        )
        .unwrap();
        log.append(
            0,
            WriteKind::RemoveMany {
                subject: 1,
                predicate: 100,
                observed: vec![OrderKey {
                    tx: 0,
                    source: 0,
                    seq: 0,
                }],
            },
        )
        .unwrap();
    }

    #[test]
    fn durable_reopen_replays_identical_state() {
        let path = tmp("reopen");
        let expected = {
            let mut log = Changelog::open(&path, 1024).unwrap();
            seed(&mut log);
            log.sync().unwrap();
            assert_eq!(log.durable_head(), log.head());
            log.replay().observe()
        };
        // reopen from cold: recovered log must fold to the same state
        let reopened = Changelog::open(&path, 1024).unwrap();
        assert_eq!(reopened.head(), 4);
        assert_eq!(reopened.durable_head(), 4);
        assert_eq!(reopened.replay().observe(), expected);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unsynced_tail_is_not_durable() {
        let path = tmp("unsynced");
        {
            let mut log = Changelog::open(&path, 1024).unwrap();
            log.append(0, add(1, 100, 2)).unwrap();
            log.sync().unwrap(); // 1 record durable
            log.append(0, add(1, 100, 3)).unwrap(); // appended, NOT synced
            assert_eq!(log.head(), 2);
            assert_eq!(log.durable_head(), 1);
        }
        // crash before syncing the 2nd append → only the 1st survives
        let reopened = Changelog::open(&path, 1024).unwrap();
        assert_eq!(reopened.head(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn torn_tail_frame_is_dropped_on_recovery() {
        let path = tmp("torn");
        {
            let mut log = Changelog::open(&path, 1024).unwrap();
            seed(&mut log);
            log.sync().unwrap();
        }
        // simulate a torn write: chop the last few bytes off the file
        let len = std::fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 3)
            .unwrap();
        // recovery drops only the torn last record; the committed prefix is intact
        let reopened = Changelog::open(&path, 1024).unwrap();
        assert_eq!(reopened.head(), 3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn backpressure_triggers_and_clears() {
        let mut log = Changelog::new(2);
        assert!(log.append(0, add(1, 100, 1)).is_ok());
        assert!(log.append(0, add(1, 100, 2)).is_ok());
        // backlog full (2 unmaterialized, limit 2)
        assert_eq!(
            log.append(0, add(1, 100, 3)),
            Err(Backpressure {
                unmaterialized: 2,
                limit: 2
            })
        );
        // a derived store catches up → backpressure relieved
        log.mark_materialized(2);
        assert_eq!(log.unmaterialized(), 0);
        assert!(log.append(0, add(1, 100, 3)).is_ok());
    }
}
