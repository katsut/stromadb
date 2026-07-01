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

fn checksum(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

// --- encode ---

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_str(buf: &mut Vec<u8>, s: &str) {
    put_u32(buf, s.len() as u32);
    buf.extend_from_slice(s.as_bytes());
}

fn put_objkey(buf: &mut Vec<u8>, o: &ObjKey) {
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

fn put_orderkey(buf: &mut Vec<u8>, ok: &OrderKey) {
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
        } => {
            buf.push(0);
            put_u64(buf, *subject);
            put_u32(buf, *predicate);
            put_objkey(buf, object);
            put_i64(buf, *valid_from);
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
        } => {
            buf.push(2);
            put_u64(buf, *subject);
            put_u32(buf, *predicate);
            put_objkey(buf, object);
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
    }
}

/// Append one framed record (`[len][crc][payload]`) to `buf`. The framing header is backfilled after
/// the payload is encoded in place, so no scratch allocation is needed.
pub(crate) fn frame_record(buf: &mut Vec<u8>, source: FieldId, kind: &WriteKind) {
    let header = buf.len();
    buf.extend_from_slice(&[0u8; 8]); // placeholder for [len][crc]
    let payload_start = buf.len();
    encode_payload(buf, source, kind);
    let len = (buf.len() - payload_start) as u32;
    let crc = checksum(&buf[payload_start..]);
    buf[header..header + 4].copy_from_slice(&len.to_le_bytes());
    buf[header + 4..header + 8].copy_from_slice(&crc.to_le_bytes());
}

// --- decode ---

struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.b.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }
    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|s| s[0])
    }
    fn u32(&mut self) -> Option<u32> {
        self.take(4)
            .map(|s| u32::from_le_bytes(s.try_into().unwrap()))
    }
    fn u64(&mut self) -> Option<u64> {
        self.take(8)
            .map(|s| u64::from_le_bytes(s.try_into().unwrap()))
    }
    fn i64(&mut self) -> Option<i64> {
        self.take(8)
            .map(|s| i64::from_le_bytes(s.try_into().unwrap()))
    }
    fn string(&mut self) -> Option<String> {
        let len = self.u32()? as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).ok()
    }
    fn objkey(&mut self) -> Option<ObjKey> {
        Some(match self.u8()? {
            0 => ObjKey::Node(self.u64()?),
            1 => ObjKey::Int(self.i64()?),
            2 => ObjKey::Float(self.u64()?),
            3 => ObjKey::Text(self.string()?),
            4 => ObjKey::Bool(self.u8()? != 0),
            _ => return None,
        })
    }
    fn orderkey(&mut self) -> Option<OrderKey> {
        Some(OrderKey {
            tx: self.u64()?,
            source: self.u32()?,
            seq: self.u64()?,
        })
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
            WriteKind::SetOne {
                subject,
                predicate,
                object,
                valid_from,
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
            WriteKind::AddMany {
                subject,
                predicate,
                object,
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
        _ => return None,
    };
    // Trailing bytes ⇒ a shorter record was mis-framed into a longer slice: reject.
    if r.pos != payload.len() {
        return None;
    }
    Some((source, kind))
}

/// Replay the WAL at `path` into an ordered list of records. Stops at the first torn/corrupt frame
/// (short read, bad checksum, or undecodable payload), dropping only that tail — a committed prefix is
/// always recovered intact. A missing file recovers as empty (fresh database).
pub fn recover(path: &Path) -> io::Result<Vec<(FieldId, WriteKind)>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut r = BufReader::new(file);
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
        match decode_record(&payload) {
            Some(rec) => out.push(rec),
            None => break, // undecodable ⇒ treat as torn tail
        }
    }
    Ok(out)
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
            },
        );
        roundtrip(
            5,
            WriteKind::AddMany {
                subject: 8,
                predicate: 4,
                object: ObjKey::Float(1.5f64.to_bits()),
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
