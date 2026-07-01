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
use std::path::Path;

/// A write submitted to the changelog — a diff *without* its order key (the changelog assigns it).
/// `RemoveMany` carries the order keys it observed (resolved by the ingest layer from current tags).
#[derive(Clone, Debug)]
pub enum WriteKind {
    SetOne {
        subject: NodeId,
        predicate: FieldId,
        object: ObjKey,
        valid_from: i64,
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

/// Append-only changelog. `seqno` = index in the log = version authority.
pub struct Changelog {
    records: Vec<Record>,
    materialized: u64, // count of records a derived store has caught up to
    max_unmaterialized: usize,
    wal: Option<File>,   // durable backend; `None` = pure in-memory mode
    durable_upto: usize, // records[..durable_upto] have been fsynced
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
        } => Op::SetOne {
            subject: *subject,
            predicate: *predicate,
            object: object.clone(),
            valid_from: *valid_from,
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
        } => Op::AddMany {
            subject: *subject,
            predicate: *predicate,
            object: object.clone(),
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
    }
}

impl Changelog {
    /// `max_unmaterialized` bounds the in-flight (appended but not-yet-materialized) backlog.
    /// Pure in-memory mode (no durability) — see [`Changelog::open`] for the durable variant.
    pub fn new(max_unmaterialized: usize) -> Self {
        Changelog {
            records: Vec::new(),
            materialized: 0,
            max_unmaterialized,
            wal: None,
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
        let recovered = wal::recover(path)?;
        let records: Vec<Record> = recovered
            .into_iter()
            .map(|(source, kind)| Record { source, kind })
            .collect();
        let n = records.len();
        let wal = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Changelog {
            records,
            materialized: n as u64,
            max_unmaterialized,
            wal: Some(wal),
            durable_upto: n,
        })
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
        self.durable_upto as u64
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
        let seqno = self.records.len() as u64;
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
            let seqno = self.records.len() as u64;
            self.records.push(Record { source, kind });
            seqnos.push(seqno);
        }
        Ok(seqnos)
    }

    /// Next seqno that will be assigned (== current length).
    pub fn head(&self) -> u64 {
        self.records.len() as u64
    }

    /// Records appended but not yet materialized by a derived store.
    pub fn unmaterialized(&self) -> usize {
        self.records.len() - self.materialized as usize
    }

    /// Advance the materialized watermark (a derived store reports its progress, relieving backpressure).
    pub fn mark_materialized(&mut self, up_to: u64) {
        self.materialized = up_to.min(self.records.len() as u64).max(self.materialized);
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
        let start = (from as usize).min(self.records.len());
        for (offset, r) in self.records[start..].iter().enumerate() {
            let seqno = (start + offset) as u64;
            let op = record_to_op(seqno, r.source, &r.kind);
            fold.apply(&op);
        }
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
