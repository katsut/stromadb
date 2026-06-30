//! Type-aware hybrid search (CAP-3, the differentiator) + version-vector read modes (CAP-3 recall
//! completeness, R-3).
//!
//! Plain ANN is type-blind: it returns vector-near results regardless of ontology type, so a query
//! for type T is polluted by semantically-near wrong-type distractors. Type-aware hybrid filters the
//! ANN candidates by type (0 type violations). [`search`] additionally resolves the vector axis of a
//! version vector: **strict** reads the indexed prefix only; **fresh** reads indexed ∪ brute-force
//! tail, closing index/structure split-brain. Validated in Phase 0 (`poc-quality-hybrid`,
//! `poc-crossstore-snapshot`).

use crate::catalog::Catalog;
use crate::fact::{FieldId, NodeId};
use crate::vector::VectorIndex;
use crate::version::{ReadMode, VersionVector};

/// Plain ANN: top-k nearest by vector, type-blind (the baseline).
pub fn plain_ann(index: &VectorIndex, q: &[f32], k: usize) -> Vec<NodeId> {
    index.nearest(q, k).into_iter().map(|(n, _)| n).collect()
}

/// Type-aware hybrid: top-k nearest restricted to `target_type` (rejects wrong-type distractors).
pub fn type_aware(
    index: &VectorIndex,
    catalog: &Catalog,
    q: &[f32],
    target_type: FieldId,
    k: usize,
) -> Vec<NodeId> {
    index
        .nearest_filtered(q, k, |n| catalog.node_type(n) == Some(target_type))
        .into_iter()
        .map(|(n, _)| n)
        .collect()
}

/// Type-aware hybrid at a pinned version vector. `Strict` searches the indexed prefix
/// (`seqno < vv.vector_watermark`) only; `Fresh` searches indexed ∪ brute-force tail (split-brain
/// closed). Both apply the ontology-type filter.
pub fn search(
    index: &VectorIndex,
    catalog: &Catalog,
    q: &[f32],
    target_type: FieldId,
    k: usize,
    vv: &VersionVector,
    mode: ReadMode,
) -> Vec<NodeId> {
    let scope = match mode {
        ReadMode::Strict => Some(vv.vector_watermark),
        ReadMode::Fresh => None,
    };
    index
        .nearest_scoped(q, k, scope, |n| catalog.node_type(n) == Some(target_type))
        .into_iter()
        .map(|(n, _)| n)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Cardinality, Range, RelProps};
    use crate::vector::sqdist;

    fn emb(v: f32) -> Vec<f32> {
        vec![v, 0.0, 0.0, 0.0]
    }

    /// Constructed bench (the CAP-3 failure mode): wrong-type distractors sit closer to the query
    /// than the relevant type-T nodes. Plain ANN is fooled; type-aware is not.
    #[test]
    fn type_aware_beats_plain_ann() {
        let mut c = Catalog::new();
        let person = c.register_type("Person");
        let doc = c.register_type("Document");
        let skill = c.register_type("Skill");
        c.register_predicate(
            "has-skill",
            Cardinality::Many,
            RelProps::default(),
            person,
            Range::Type(skill),
        );

        let mut idx = VectorIndex::new(4);
        let q = [0.0f32; 4];
        for i in 1..=7u64 {
            idx.insert(i, i, emb(0.05 * i as f32)); // distractors closest
            c.set_node_type(i, doc);
        }
        for (j, id) in [11u64, 12, 13].into_iter().enumerate() {
            idx.insert(id, id, emb(0.50 + 0.01 * j as f32)); // relevant
            c.set_node_type(id, person);
        }
        idx.insert(21, 21, emb(2.0)); // far Person (not relevant)
        c.set_node_type(21, person);
        idx.insert(22, 22, emb(2.1));
        c.set_node_type(22, person);

        let k = 5;
        let relevant: std::collections::BTreeSet<NodeId> = [11, 12, 13].into_iter().collect();
        let plain = plain_ann(&idx, &q, k);
        let hybrid = type_aware(&idx, &c, &q, person, k);
        let recall = |r: &[NodeId]| {
            r.iter().filter(|n| relevant.contains(n)).count() as f64 / relevant.len() as f64
        };
        let type_viol = |r: &[NodeId]| {
            r.iter()
                .filter(|n| c.node_type(**n) != Some(person))
                .count()
        };

        assert_eq!(recall(&plain), 0.0);
        assert_eq!(type_viol(&plain), 5);
        assert_eq!(recall(&hybrid), 1.0);
        assert_eq!(type_viol(&hybrid), 0);
        assert!(recall(&hybrid) > recall(&plain));
    }

    /// fresh closes the split-brain that strict (indexed-prefix only) leaves open: a freshly-added,
    /// not-yet-indexed type-T node closest to the query is found only by fresh.
    #[test]
    fn fresh_closes_split_brain() {
        let mut c = Catalog::new();
        let person = c.register_type("Person");
        let mut idx = VectorIndex::new(4);
        let q = [0.0f32; 4];

        // indexed prefix (seqno < watermark=5): two Person at moderate distance
        idx.insert(1, 1, emb(1.0));
        idx.insert(2, 2, emb(1.1));
        c.set_node_type(1, person);
        c.set_node_type(2, person);
        // un-indexed tail (seqno >= 5): a Person closest to q
        idx.insert(6, 6, emb(0.1));
        c.set_node_type(6, person);
        idx.advance_watermark(5);

        let vv = VersionVector::new(10, 5);
        assert_eq!(vv.skew(), 5);

        let strict: std::collections::BTreeSet<NodeId> =
            search(&idx, &c, &q, person, 3, &vv, ReadMode::Strict)
                .into_iter()
                .collect();
        let fresh: std::collections::BTreeSet<NodeId> =
            search(&idx, &c, &q, person, 3, &vv, ReadMode::Fresh)
                .into_iter()
                .collect();

        assert_eq!(strict, [1, 2].into_iter().collect()); // tail node 6 missed
        assert!(fresh.contains(&6)); // brute-force tail found it
        assert!(strict.is_subset(&fresh)); // fresh never loses a strict result
    }

    #[test]
    fn sqdist_is_monotonic() {
        assert!(sqdist(&[0.0, 0.0], &[1.0, 0.0]) < sqdist(&[0.0, 0.0], &[2.0, 0.0]));
    }
}
