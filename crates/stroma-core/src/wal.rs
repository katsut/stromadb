//! Framed write-ahead-log codec + crash recovery for the changelog's durable backend.
//!
//! On-disk format is a sequence of self-describing frames:
//!
//! ```text
//! [payload_len u32 LE][crc32 u32 LE][payload payload_len bytes]
//! ```
//!
//! Each payload encodes one `(source, WriteKind)` record. Recovery reads frames in order and stops
//! at the first frame that is short (torn write) or whose checksum/decode fails — so a crash mid-append
//! costs only the torn tail record, never a committed prefix. `crc` is FNV-1a (dependency-free; this is
//! a torn-write detector, not a cryptographic MAC). The durable backend (LSM) slots in behind the same
//! frame contract later; this file-WAL is the first crash-sound backend.

use crate::catalog::Cardinality;
use crate::changelog::WriteKind;
use crate::fact::{FieldId, NodeId};
use crate::fold::{ObjKey, OrderKey};
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

/// Records larger than this are treated as a torn/garbage frame during recovery (guards against a
/// corrupt length field triggering a huge allocation). No legitimate single write approaches this.
const MAX_RECORD_LEN: usize = 16 * 1024 * 1024;

pub(crate) fn checksum(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

// --- encode ---

pub(crate) fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub(crate) fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub(crate) fn put_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub(crate) fn put_str(buf: &mut Vec<u8>, s: &str) {
    put_u32(buf, s.len() as u32);
    buf.extend_from_slice(s.as_bytes());
}

pub(crate) fn put_objkey(buf: &mut Vec<u8>, o: &ObjKey) {
    match o {
        ObjKey::Node(n) => {
            buf.push(0);
            put_u64(buf, *n);
        }
        ObjKey::Int(i) => {
            buf.push(1);
            put_i64(buf, *i);
        }
        ObjKey::Float(b) => {
            buf.push(2);
            put_u64(buf, *b);
        }
        ObjKey::Text(t) => {
            buf.push(3);
            put_str(buf, t);
        }
        ObjKey::Bool(b) => {
            buf.push(4);
            buf.push(*b as u8);
        }
    }
}

pub(crate) fn put_orderkey(buf: &mut Vec<u8>, ok: &OrderKey) {
    put_u64(buf, ok.tx);
    put_u32(buf, ok.source);
    put_u64(buf, ok.seq);
}

fn encode_payload(buf: &mut Vec<u8>, source: FieldId, kind: &WriteKind) {
    put_u32(buf, source);
    match kind {
        WriteKind::SetOne {
            subject,
            predicate,
            object,
            valid_from,
            valid_to,
        } => {
            buf.push(0);
            put_u64(buf, *subject);
            put_u32(buf, *predicate);
            put_objkey(buf, object);
            put_i64(buf, *valid_from);
            // trailing optional valid_to (backward compatible: absent tail decodes to None)
            match valid_to {
                Some(t) => {
                    buf.push(1);
                    put_i64(buf, *t);
                }
                None => buf.push(0),
            }
        }
        WriteKind::CloseOne {
            subject,
            predicate,
            valid_from,
        } => {
            buf.push(1);
            put_u64(buf, *subject);
            put_u32(buf, *predicate);
            put_i64(buf, *valid_from);
        }
        WriteKind::AddMany {
            subject,
            predicate,
            object,
            valid_from,
            valid_to,
        } => {
            buf.push(2);
            put_u64(buf, *subject);
            put_u32(buf, *predicate);
            put_objkey(buf, object);
            // trailing valid-time (backward compatible: a record written before these fields
            // existed simply ends after the object and decodes to `0` / `None`)
            put_i64(buf, *valid_from);
            match valid_to {
                Some(t) => {
                    buf.push(1);
                    put_i64(buf, *t);
                }
                None => buf.push(0),
            }
        }
        WriteKind::RemoveMany {
            subject,
            predicate,
            observed,
        } => {
            buf.push(3);
            put_u64(buf, *subject);
            put_u32(buf, *predicate);
            put_u32(buf, observed.len() as u32);
            for ok in observed {
                put_orderkey(buf, ok);
            }
        }
        WriteKind::HardDelete {
            subject,
            predicate,
            cardinality,
        } => {
            buf.push(4);
            put_u64(buf, *subject);
            put_u32(buf, *predicate);
            buf.push(match cardinality {
                Cardinality::One => 0,
                Cardinality::Many => 1,
            });
        }
        WriteKind::SetEdgeProp {
            subject,
            predicate,
            object,
            key,
            value,
        } => {
            buf.push(5);
            put_u64(buf, *subject);
            put_u32(buf, *predicate);
            put_objkey(buf, object);
            put_str(buf, key);
            put_objkey(buf, value);
        }
        WriteKind::SetNodeType { node, type_id } => {
            buf.push(6);
            put_u64(buf, *node);
            put_u32(buf, *type_id);
        }
        WriteKind::SetNodeLabel { node, label } => {
            buf.push(7);
            put_u64(buf, *node);
            buf.push(*label);
        }
        WriteKind::CloseMany {
            subject,
            predicate,
            object,
            valid_from,
        } => {
            buf.push(8);
            put_u64(buf, *subject);
            put_u32(buf, *predicate);
            put_objkey(buf, object);
            put_i64(buf, *valid_from);
        }
    }
}

// A WAL-start frame: file-level metadata, not a write. Written as the FIRST frame of a WAL created
// by compaction, it names the seqno of the file's first record — recovery offsets every following
// record by it, so a snapshot-truncated log keeps globally continuous seqnos. Payload:
// [u32 source=0][tag 255][u64 first_seqno]. A pre-compaction WAL simply has no such frame and
// recovers with first_seqno = 0 (backward compatible).
const WAL_START_TAG: u8 = 255;

/// Append the WAL-start frame naming the file's first record seqno.
pub(crate) fn frame_wal_start(buf: &mut Vec<u8>, first_seqno: u64) {
    let header = buf.len();
    buf.extend_from_slice(&[0u8; 8]);
    let payload_start = buf.len();
    put_u32(buf, 0);
    buf.push(WAL_START_TAG);
    put_u64(buf, first_seqno);
    finish_frame(buf, header, payload_start);
}

fn decode_wal_start(payload: &[u8]) -> Option<u64> {
    let mut r = Reader { b: payload, pos: 0 };
    if r.u32()? != 0 || r.u8()? != WAL_START_TAG {
        return None;
    }
    let first = r.u64()?;
    (r.pos == payload.len()).then_some(first)
}

/// Append one framed record (`[len][crc][payload]`) to `buf`. The framing header is backfilled after
/// the payload is encoded in place, so no scratch allocation is needed.
pub(crate) fn frame_record(buf: &mut Vec<u8>, source: FieldId, kind: &WriteKind) {
    let header = buf.len();
    buf.extend_from_slice(&[0u8; 8]); // placeholder for [len][crc]
    let payload_start = buf.len();
    encode_payload(buf, source, kind);
    finish_frame(buf, header, payload_start);
}

/// Backfill a frame's `[len][crc]` header once its payload is in place.
fn finish_frame(buf: &mut [u8], header: usize, payload_start: usize) {
    let len = (buf.len() - payload_start) as u32;
    let crc = checksum(&buf[payload_start..]);
    buf[header..header + 4].copy_from_slice(&len.to_le_bytes());
    buf[header + 4..header + 8].copy_from_slice(&crc.to_le_bytes());
}

// --- decode ---

pub(crate) struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(b: &'a [u8]) -> Self {
        Reader { b, pos: 0 }
    }
    pub(crate) fn done(&self) -> bool {
        self.pos == self.b.len()
    }
    pub(crate) fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.b.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }
    pub(crate) fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|s| s[0])
    }
    pub(crate) fn u32(&mut self) -> Option<u32> {
        self.take(4)
            .map(|s| u32::from_le_bytes(s.try_into().unwrap()))
    }
    pub(crate) fn u64(&mut self) -> Option<u64> {
        self.take(8)
            .map(|s| u64::from_le_bytes(s.try_into().unwrap()))
    }
    pub(crate) fn i64(&mut self) -> Option<i64> {
        self.take(8)
            .map(|s| i64::from_le_bytes(s.try_into().unwrap()))
    }
    pub(crate) fn string(&mut self) -> Option<String> {
        let len = self.u32()? as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).ok()
    }
    pub(crate) fn objkey(&mut self) -> Option<ObjKey> {
        Some(match self.u8()? {
            0 => ObjKey::Node(self.u64()?),
            1 => ObjKey::Int(self.i64()?),
            2 => ObjKey::Float(self.u64()?),
            3 => ObjKey::Text(self.string()?),
            4 => ObjKey::Bool(self.u8()? != 0),
            _ => return None,
        })
    }
    pub(crate) fn orderkey(&mut self) -> Option<OrderKey> {
        Some(OrderKey {
            tx: self.u64()?,
            source: self.u32()?,
            seq: self.u64()?,
        })
    }
    pub(crate) fn remaining(&self) -> usize {
        self.b.len().saturating_sub(self.pos)
    }
    /// Trailing optional i64 (`[0]` = None, `[1][i64]` = Some). A record written before this field
    /// existed simply has no trailing bytes, so an empty tail decodes to `None` (backward compatible).
    pub(crate) fn opt_i64_tail(&mut self) -> Option<Option<i64>> {
        if self.remaining() == 0 {
            return Some(None);
        }
        match self.u8()? {
            0 => Some(None),
            1 => Some(Some(self.i64()?)),
            _ => None,
        }
    }
    /// Trailing valid-time pair `[valid_from i64][opt valid_to]`. A record written before these
    /// fields existed has no trailing bytes and decodes to `(0, None)` (backward compatible).
    pub(crate) fn valid_time_tail(&mut self) -> Option<(i64, Option<i64>)> {
        if self.remaining() == 0 {
            return Some((0, None));
        }
        let valid_from = self.i64()?;
        let valid_to = self.opt_i64_tail()?;
        Some((valid_from, valid_to))
    }
}

fn decode_record(payload: &[u8]) -> Option<(FieldId, WriteKind)> {
    let mut r = Reader { b: payload, pos: 0 };
    let source = r.u32()?;
    let subject: NodeId;
    let predicate: FieldId;
    let kind = match r.u8()? {
        0 => {
            subject = r.u64()?;
            predicate = r.u32()?;
            let object = r.objkey()?;
            let valid_from = r.i64()?;
            let valid_to = r.opt_i64_tail()?;
            WriteKind::SetOne {
                subject,
                predicate,
                object,
                valid_from,
                valid_to,
            }
        }
        1 => {
            subject = r.u64()?;
            predicate = r.u32()?;
            let valid_from = r.i64()?;
            WriteKind::CloseOne {
                subject,
                predicate,
                valid_from,
            }
        }
        2 => {
            subject = r.u64()?;
            predicate = r.u32()?;
            let object = r.objkey()?;
            let (valid_from, valid_to) = r.valid_time_tail()?;
            WriteKind::AddMany {
                subject,
                predicate,
                object,
                valid_from,
                valid_to,
            }
        }
        3 => {
            subject = r.u64()?;
            predicate = r.u32()?;
            let n = r.u32()? as usize;
            let mut observed = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                observed.push(r.orderkey()?);
            }
            WriteKind::RemoveMany {
                subject,
                predicate,
                observed,
            }
        }
        4 => {
            subject = r.u64()?;
            predicate = r.u32()?;
            let cardinality = match r.u8()? {
                0 => Cardinality::One,
                1 => Cardinality::Many,
                _ => return None,
            };
            WriteKind::HardDelete {
                subject,
                predicate,
                cardinality,
            }
        }
        5 => {
            subject = r.u64()?;
            predicate = r.u32()?;
            let object = r.objkey()?;
            let key = r.string()?;
            let value = r.objkey()?;
            WriteKind::SetEdgeProp {
                subject,
                predicate,
                object,
                key,
                value,
            }
        }
        6 => {
            let node = r.u64()?;
            let type_id = r.u32()?;
            WriteKind::SetNodeType { node, type_id }
        }
        7 => {
            let node = r.u64()?;
            let label = r.u8()?;
            WriteKind::SetNodeLabel { node, label }
        }
        8 => {
            subject = r.u64()?;
            predicate = r.u32()?;
            let object = r.objkey()?;
            let valid_from = r.i64()?;
            WriteKind::CloseMany {
                subject,
                predicate,
                object,
                valid_from,
            }
        }
        _ => return None,
    };
    // Trailing bytes ⇒ a shorter record was mis-framed into a longer slice: reject.
    if r.pos != payload.len() {
        return None;
    }
    Some((source, kind))
}

/// Replay the WAL at `path` into `(first_seqno, records)`: the seqno of the file's first record
/// (from the WAL-start frame a compaction writes; `0` for a pre-compaction file without one) and
/// the ordered records. Stops at the first torn/corrupt frame (short read, bad checksum, or
/// undecodable payload), dropping only that tail — a committed prefix is always recovered intact.
/// A missing file recovers as empty (fresh database).
pub fn recover(path: &Path) -> io::Result<(u64, Vec<(FieldId, WriteKind)>)> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((0, Vec::new())),
        Err(e) => return Err(e),
    };
    let mut r = BufReader::new(file);
    let mut first_seqno = 0u64;
    let mut first_frame = true;
    let mut out = Vec::new();
    loop {
        let mut header = [0u8; 8];
        match r.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
        if len == 0 || len > MAX_RECORD_LEN {
            break; // torn/garbage length field
        }
        let mut payload = vec![0u8; len];
        match r.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // torn payload
            Err(e) => return Err(e),
        }
        if checksum(&payload) != crc {
            break; // torn/corrupt tail
        }
        // only the physical first frame may carry the file's start seqno
        if first_frame {
            first_frame = false;
            if let Some(s) = decode_wal_start(&payload) {
                first_seqno = s;
                continue;
            }
        }
        match decode_record(&payload) {
            Some(rec) => out.push(rec),
            None => break, // undecodable ⇒ treat as torn tail
        }
    }
    Ok((first_seqno, out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(source: FieldId, kind: WriteKind) {
        let mut buf = Vec::new();
        frame_record(&mut buf, source, &kind);
        let recovered = {
            // strip the [len][crc] header and decode the payload directly
            let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            decode_record(&buf[8..8 + len]).unwrap()
        };
        assert_eq!(recovered.0, source);
        // WriteKind has no PartialEq; compare via debug repr (sufficient for the codec).
        assert_eq!(format!("{:?}", recovered.1), format!("{kind:?}"));
    }

    #[test]
    fn codec_roundtrips_every_variant() {
        roundtrip(
            7,
            WriteKind::SetOne {
                subject: 42,
                predicate: 3,
                object: ObjKey::Text("héllo".into()),
                valid_from: -100,
                valid_to: Some(500),
            },
        );
        roundtrip(
            0,
            WriteKind::CloseOne {
                subject: 1,
                predicate: 2,
                valid_from: 9,
            },
        );
        roundtrip(
            5,
            WriteKind::AddMany {
                subject: 8,
                predicate: 4,
                object: ObjKey::Node(99),
                valid_from: 123,
                valid_to: Some(456),
            },
        );
        roundtrip(
            5,
            WriteKind::AddMany {
                subject: 8,
                predicate: 4,
                object: ObjKey::Float(1.5f64.to_bits()),
                valid_from: 0,
                valid_to: None,
            },
        );
        roundtrip(
            2,
            WriteKind::RemoveMany {
                subject: 8,
                predicate: 4,
                observed: vec![
                    OrderKey {
                        tx: 1,
                        source: 2,
                        seq: 3,
                    },
                    OrderKey {
                        tx: 4,
                        source: 5,
                        seq: 6,
                    },
                ],
            },
        );
        roundtrip(
            1,
            WriteKind::HardDelete {
                subject: 8,
                predicate: 4,
                cardinality: Cardinality::Many,
            },
        );
        roundtrip(
            3,
            WriteKind::SetEdgeProp {
                subject: 8,
                predicate: 4,
                object: ObjKey::Node(9),
                key: "since".into(),
                value: ObjKey::Int(2020),
            },
        );
        roundtrip(
            6,
            WriteKind::SetNodeType {
                node: 77,
                type_id: 12,
            },
        );
        roundtrip(
            0,
            WriteKind::SetNodeLabel {
                node: 77,
                label: 200,
            },
        );
        roundtrip(
            5,
            WriteKind::CloseMany {
                subject: 8,
                predicate: 4,
                object: ObjKey::Node(99),
                valid_from: -7,
            },
        );
    }

    #[test]
    fn old_format_add_many_without_valid_time_decodes_as_unreported() {
        // A pre-valid-time AddMany record ends right after the object — hand-build one and check
        // it decodes with `valid_from = 0`, open `valid_to` (the backward-compat contract).
        let mut payload = Vec::new();
        put_u32(&mut payload, 9); // source
        payload.push(2); // AddMany tag
        put_u64(&mut payload, 8); // subject
        put_u32(&mut payload, 4); // predicate
        put_objkey(&mut payload, &ObjKey::Node(99));
        let (source, kind) = decode_record(&payload).unwrap();
        assert_eq!(source, 9);
        assert!(matches!(
            kind,
            WriteKind::AddMany {
                subject: 8,
                predicate: 4,
                object: ObjKey::Node(99),
                valid_from: 0,
                valid_to: None,
            }
        ));
    }

    #[test]
    fn corrupt_checksum_is_undecodable_tail() {
        let mut buf = Vec::new();
        frame_record(
            &mut buf,
            0,
            &WriteKind::AddMany {
                subject: 1,
                predicate: 2,
                object: ObjKey::Node(3),
                valid_from: 0,
                valid_to: None,
            },
        );
        // flip a payload byte → checksum mismatch on recovery
        let last = buf.len() - 1;
        buf[last] ^= 0xff;
        let len = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let crc = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        assert_ne!(checksum(&buf[8..8 + len as usize]), crc);
    }
}
