//! FxHash — a fast, dependency-free hasher for the hot node→{type,label} maps. The authz + type
//! filter on the read path hits these once per candidate, so the default SipHash's per-lookup cost is
//! significant; FxHash is ~an order of magnitude cheaper on `u64` keys. Not DoS-resistant, but the
//! keys are internal node ids, not attacker-controlled input.

use std::hash::{BuildHasherDefault, Hasher};

#[derive(Default)]
pub struct FxHasher(u64);

impl Hasher for FxHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.write_u64(b as u64);
        }
    }
    fn write_u64(&mut self, i: u64) {
        self.0 = (self.0.rotate_left(5) ^ i).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
    fn write_u32(&mut self, i: u32) {
        self.write_u64(i as u64);
    }
}

pub type FxBuild = BuildHasherDefault<FxHasher>;

/// A `std::HashMap` keyed with the fast [`FxBuild`] hasher — used for the snapshot's node→attribute
/// maps (flat, so the read-path authz + type filter probes them in one shot).
pub type FxHashMap<K, V> = std::collections::HashMap<K, V, FxBuild>;
