//! Cross-store version vector + read modes (CAP result contract, R-3).
//!
//! Reads span the authoritative changelog and derived stores that lag (here: the vector index).
//! A read pins a version vector — sampled as one consistent cut — exposing the skew. Two modes:
//! **strict** (read the derived store at its watermark = fully consistent, newest tail excluded) and
//! **fresh** (latest + a brute-force tail that closes index/structure split-brain). Validated in
//! Phase 0 (`poc-crossstore-snapshot`).

/// A version vector across stores. MVP axes: the changelog seqno (authority) and the vector-index
/// watermark (derived). Invariant: `vector_watermark <= changelog_seqno` (no dangling).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersionVector {
    pub changelog_seqno: u64,
    pub vector_watermark: u64,
}

impl VersionVector {
    pub fn new(changelog_seqno: u64, vector_watermark: u64) -> Self {
        debug_assert!(
            vector_watermark <= changelog_seqno,
            "dangling: vector ahead of changelog"
        );
        VersionVector {
            changelog_seqno,
            vector_watermark,
        }
    }

    /// Componentwise dominance (`self ⊒ other`).
    pub fn dominates(&self, other: &Self) -> bool {
        self.changelog_seqno >= other.changelog_seqno
            && self.vector_watermark >= other.vector_watermark
    }

    /// Comparable in the partial order (one dominates the other); otherwise concurrent.
    pub fn comparable(&self, other: &Self) -> bool {
        self.dominates(other) || other.dominates(self)
    }

    /// The skew exposed by this cut (un-indexed-but-structural tail length).
    pub fn skew(&self) -> u64 {
        self.changelog_seqno - self.vector_watermark
    }
}

/// How a read resolves the vector axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadMode {
    /// Read the vector index at its watermark only — fully consistent, newest tail excluded.
    Strict,
    /// Read indexed prefix ∪ brute-force the un-indexed tail — closes split-brain (agent default).
    Fresh,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_order() {
        let a = VersionVector::new(10, 5);
        let b = VersionVector::new(8, 5);
        assert!(a.dominates(&b));
        assert!(a.comparable(&b));
        assert_eq!(a.skew(), 5);
        // concurrent: neither dominates
        let c = VersionVector::new(10, 2);
        let d = VersionVector::new(8, 4);
        assert!(!c.comparable(&d));
    }
}
