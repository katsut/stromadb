//! Type-aware hybrid search (CAP-3, the differentiator).
//!
//! Plain ANN is type-blind: it returns vector-near results regardless of ontology type, so a query
//! for type T is polluted by semantically-near wrong-type distractors. Type-aware hybrid filters the
//! ANN candidates by the catalog's type, returning only semantically-coherent results (0 type
//! violations by construction). Validated statistically in Phase 0 (`poc-quality-hybrid`).

use crate::catalog::Catalog;
use crate::fact::{FieldId, NodeId};
use crate::vector::VectorIndex;

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
        let person = c.register_type("Person"); // target type T
        let doc = c.register_type("Document"); // distractor type
        // a Person->Skill predicate just to exercise the catalog; not used by the search itself.
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

        // 7 wrong-type distractors closest to q (v = 0.05..0.35)
        for i in 1..=7u64 {
            idx.insert(i, emb(0.05 * i as f32));
            c.set_node_type(i, doc);
        }
        // 3 relevant Person nodes (v = 0.50..0.52)
        for (j, id) in [11u64, 12, 13].into_iter().enumerate() {
            idx.insert(id, emb(0.50 + 0.01 * j as f32));
            c.set_node_type(id, person);
        }
        // 2 far Person nodes (v = 2.0, 2.1) — correct type but not relevant
        idx.insert(21, emb(2.0));
        c.set_node_type(21, person);
        idx.insert(22, emb(2.1));
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

        // plain ANN: top-5 are the 5 closest distractors -> 0 relevant, all type violations.
        assert_eq!(recall(&plain), 0.0);
        assert_eq!(type_viol(&plain), 5);
        // type-aware: the 3 relevant (+2 far Person) -> full recall, 0 type violations.
        assert_eq!(recall(&hybrid), 1.0);
        assert_eq!(type_viol(&hybrid), 0);
        assert!(recall(&hybrid) > recall(&plain));
    }

    #[test]
    fn sqdist_is_monotonic() {
        assert!(sqdist(&[0.0, 0.0], &[1.0, 0.0]) < sqdist(&[0.0, 0.0], &[2.0, 0.0]));
    }
}
