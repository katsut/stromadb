//! Composable operator IR — one-shot server-side evaluation (CAP-10) with authz injection and the
//! result contract (token budget + as_of). Runs on the frozen read contracts (query ops, hybrid,
//! version vector); the same operators back Live Query (single algebra).
//!
//! authz is injected at the HEAD of every pipeline and threaded into each source as a *scoped*
//! filter (per H4: a principal only ever scores authorized nodes — no shared-index post-filter leak).
//! Every result is bounded (`max_nodes`) and stamped with the version vector.

use crate::catalog::Catalog;
use crate::fact::{FieldId, NodeId};
use crate::fold::Snapshot;
use crate::ivf::IvfPq;
use crate::query;
use crate::vector::VectorIndex;
use crate::version::{ReadMode, VersionVector};

/// Default IVF probe / re-rank depth for the IR read path — the operating point measured on
/// overlapping-cluster data (`examples/ann_nprobe_curve`): nprobe=8 + R=256 gives recall@10 ~1.0 at
/// authz-on warm p99 <1ms. Re-rank depth is the cheap recall lever (raw reads cost ~0.3ms, #19).
const IR_NPROBE: usize = 8;
const IR_RERANK_R: usize = 256;

/// The vector backend the IR read path depends on — swappable under the frozen IR contract
/// (`vector::VectorIndex` = exact oracle; `ivf::IvfPq` = production IVF-PQ + re-rank). `keep` combines
/// authz + type and is applied *before* scoring (H4 scoped, no post-filter leak); `scope` is the
/// seqno watermark (`Some` = strict indexed-prefix, `None` = fresh). Distances: smaller = nearer.
pub trait AnnBackend {
    fn ann_search(
        &self,
        q: &[f32],
        k: usize,
        scope: Option<u64>,
        keep: &dyn Fn(NodeId) -> bool,
    ) -> Vec<(NodeId, f32)>;
}

impl AnnBackend for VectorIndex {
    fn ann_search(
        &self,
        q: &[f32],
        k: usize,
        scope: Option<u64>,
        keep: &dyn Fn(NodeId) -> bool,
    ) -> Vec<(NodeId, f32)> {
        self.nearest_scoped(q, k, scope, keep)
    }
}

impl AnnBackend for IvfPq {
    fn ann_search(
        &self,
        q: &[f32],
        k: usize,
        scope: Option<u64>,
        keep: &dyn Fn(NodeId) -> bool,
    ) -> Vec<(NodeId, f32)> {
        // authz+type ride on `keep` (checked before scoring); exact re-rank recovers recall.
        self.search_rerank(q, k, IR_NPROBE, IR_RERANK_R, scope, |_| true, keep)
    }
}

/// The querying end-user principal (ABAC label bitmask). Unlabeled nodes are public.
#[derive(Clone, Copy, Debug)]
pub struct Principal {
    pub allowed_labels: u32,
}

impl Principal {
    pub fn can_see_label(&self, label: u8) -> bool {
        (self.allowed_labels >> label) & 1 == 1
    }
}

/// What flows between operators: id set + per-id score + the as_of the read was pinned at.
#[derive(Clone, Debug, PartialEq)]
pub struct Traverser {
    pub ids: Vec<NodeId>,
    pub scores: Vec<f32>,
    pub as_of: VersionVector,
}

/// Pipeline sources.
pub enum Source {
    /// Specific nodes (identity lookup).
    Point { subjects: Vec<NodeId> },
    /// Type-aware hybrid: k nearest of `target_type`, authz+version scoped.
    TypeAnn {
        q: Vec<f32>,
        target_type: FieldId,
        k: usize,
    },
}

/// Pipeline transforms.
pub enum Transform {
    /// 1-hop expand over `predicate` (structural).
    Expand { predicate: FieldId },
    /// Keep the top-k by current score.
    TopK { k: usize },
}

/// A composed pipeline submitted for one-shot evaluation.
pub struct Pipeline {
    pub source: Source,
    pub transforms: Vec<Transform>,
    pub max_nodes: usize,
    pub mode: ReadMode,
}

fn authorized(catalog: &Catalog, principal: &Principal, n: NodeId) -> bool {
    catalog
        .node_label(n)
        .is_none_or(|l| principal.can_see_label(l))
}

fn cap(ids: &mut Vec<NodeId>, scores: &mut Vec<f32>, max_nodes: usize) {
    if ids.len() > max_nodes {
        ids.truncate(max_nodes);
        scores.truncate(max_nodes);
    }
}

/// Evaluate a pipeline one-shot over the read state. authz is enforced at the source (scoped) and on
/// every expanded node; the result is bounded by `max_nodes` and carries `vv` as its as_of.
pub fn run<A: AnnBackend>(
    snapshot: &Snapshot,
    catalog: &Catalog,
    vector: &A,
    pipeline: &Pipeline,
    principal: &Principal,
    vv: VersionVector,
) -> Traverser {
    let (mut ids, mut scores): (Vec<NodeId>, Vec<f32>) = match &pipeline.source {
        Source::Point { subjects } => {
            let ids: Vec<NodeId> = subjects
                .iter()
                .copied()
                .filter(|&n| authorized(catalog, principal, n))
                .collect();
            let scores = vec![1.0; ids.len()];
            (ids, scores)
        }
        Source::TypeAnn { q, target_type, k } => {
            // authz + type + version scoped in one filtered search (no shared-index leak, H4).
            let scope = match pipeline.mode {
                ReadMode::Strict => Some(vv.vector_watermark),
                ReadMode::Fresh => None,
            };
            let keep = |n: NodeId| {
                authorized(catalog, principal, n) && catalog.node_type(n) == Some(*target_type)
            };
            let hits = vector.ann_search(q, *k, scope, &keep);
            let ids = hits.iter().map(|(n, _)| *n).collect();
            // score = relevance (higher = better).
            let scores = hits.iter().map(|(_, d)| 1.0 / (1.0 + d)).collect();
            (ids, scores)
        }
    };
    cap(&mut ids, &mut scores, pipeline.max_nodes);

    for t in &pipeline.transforms {
        match t {
            Transform::Expand { predicate } => {
                let mut next: Vec<NodeId> = Vec::new();
                for &s in &ids {
                    for n in query::expand(snapshot, s, *predicate) {
                        if authorized(catalog, principal, n) && !next.contains(&n) {
                            next.push(n);
                        }
                    }
                }
                ids = next;
                scores = vec![1.0; ids.len()];
            }
            Transform::TopK { k } => {
                let mut order: Vec<usize> = (0..ids.len()).collect();
                order.sort_by(|&a, &b| {
                    scores[b]
                        .partial_cmp(&scores[a])
                        .unwrap()
                        .then(ids[a].cmp(&ids[b]))
                });
                order.truncate(*k);
                ids = order.iter().map(|&i| ids[i]).collect();
                scores = order.iter().map(|&i| scores[i]).collect();
            }
        }
        cap(&mut ids, &mut scores, pipeline.max_nodes);
    }

    Traverser {
        ids,
        scores,
        as_of: vv,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Cardinality, Range, RelProps};
    use crate::fold::{ObjKey, Op, OrderKey, fold};

    fn ok(seq: u64) -> OrderKey {
        OrderKey {
            tx: seq,
            source: 0,
            seq,
        }
    }

    // Person(1) worked-on Project(10,11); has-skill Skill(20). Person(2) worked-on Project(10).
    fn setup() -> (Snapshot, Catalog, VectorIndex) {
        let mut c = Catalog::new();
        let person = c.register_type("Person");
        let project = c.register_type("Project");
        let skill = c.register_type("Skill");
        c.register_predicate(
            "worked-on",
            Cardinality::Many,
            RelProps::default(),
            person,
            Range::Type(project),
        );
        c.register_predicate(
            "has-skill",
            Cardinality::Many,
            RelProps::default(),
            person,
            Range::Type(skill),
        );
        for n in [1u64, 2] {
            c.set_node_type(n, person);
        }
        c.set_node_type(10, project);
        c.set_node_type(11, project);
        c.set_node_type(20, skill);

        let wo = c.field_id("worked-on").unwrap();
        let snap = fold(&[
            Op::AddMany {
                subject: 1,
                predicate: wo,
                object: ObjKey::Node(10),
                ok: ok(0),
            },
            Op::AddMany {
                subject: 1,
                predicate: wo,
                object: ObjKey::Node(11),
                ok: ok(1),
            },
            Op::AddMany {
                subject: 2,
                predicate: wo,
                object: ObjKey::Node(10),
                ok: ok(2),
            },
        ])
        .observe();

        let mut vec = VectorIndex::new(2);
        vec.insert(1, 0, vec![0.0, 0.0]); // person 1 near origin
        vec.insert(2, 1, vec![0.9, 0.0]); // person 2 farther
        vec.insert(20, 2, vec![0.05, 0.0]); // a Skill near origin (wrong type for a Person query)
        (snap, c, vec)
    }

    fn everyone() -> Principal {
        Principal {
            allowed_labels: u32::MAX,
        }
    }

    #[test]
    fn type_ann_then_expand_pipeline() {
        let (snap, c, vec) = setup();
        let person = c.field_id("Person").unwrap();
        let wo = c.field_id("worked-on").unwrap();
        // find Persons near origin, then expand to their projects
        let pl = Pipeline {
            source: Source::TypeAnn {
                q: vec![0.0, 0.0],
                target_type: person,
                k: 5,
            },
            transforms: vec![Transform::Expand { predicate: wo }],
            max_nodes: 100,
            mode: ReadMode::Fresh,
        };
        let t = run(&snap, &c, &vec, &pl, &everyone(), VersionVector::new(3, 3));
        // Skill(20) is nearer than Person(2) but wrong type → not a source hit; projects {10,11}.
        assert_eq!(
            t.ids
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            [10, 11].into_iter().collect()
        );
        assert_eq!(t.as_of, VersionVector::new(3, 3));
    }

    #[test]
    fn authz_scopes_the_source() {
        let (snap, mut c, vec) = setup();
        c.set_node_label(2, 3); // person 2 is sensitive (label 3)
        let person = c.field_id("Person").unwrap();
        let pl = Pipeline {
            source: Source::TypeAnn {
                q: vec![0.9, 0.0],
                target_type: person,
                k: 5,
            },
            transforms: vec![],
            max_nodes: 100,
            mode: ReadMode::Fresh,
        };
        // a principal WITHOUT label 3 never sees person 2 (scoped out at the source)
        let p_no = Principal {
            allowed_labels: 0b1,
        }; // only public (label 0)
        let t = run(&snap, &c, &vec, &pl, &p_no, VersionVector::new(3, 3));
        assert!(!t.ids.contains(&2));
        // a principal WITH label 3 sees it
        let p_yes = Principal {
            allowed_labels: 0b1 | (1 << 3),
        };
        let t2 = run(&snap, &c, &vec, &pl, &p_yes, VersionVector::new(3, 3));
        assert!(t2.ids.contains(&2));
    }

    // Build an IvfPq backend over the same vectors as `setup`'s VectorIndex.
    fn ivf_backend() -> IvfPq {
        let vecs = vec![
            (1u64, 0u64, vec![0.0f32, 0.0]),
            (2, 1, vec![0.9, 0.0]),
            (20, 2, vec![0.05, 0.0]),
        ];
        let sample: Vec<Vec<f32>> = vecs.iter().map(|(_, _, v)| v.clone()).collect();
        let mut idx = IvfPq::new(2, 2, 2);
        idx.train(&sample);
        for (n, s, v) in &vecs {
            idx.add(*n, *s, v, 0);
        }
        idx
    }

    #[test]
    fn ivfpq_backend_matches_exact_through_ir() {
        let (snap, c, vec) = setup();
        let ivf = ivf_backend();
        let person = c.field_id("Person").unwrap();
        let wo = c.field_id("worked-on").unwrap();
        let pl = Pipeline {
            source: Source::TypeAnn {
                q: vec![0.0, 0.0],
                target_type: person,
                k: 5,
            },
            transforms: vec![Transform::Expand { predicate: wo }],
            max_nodes: 100,
            mode: ReadMode::Fresh,
        };
        // exact oracle vs IVF-PQ+rerank must agree on the id set through the whole pipeline
        let exact = run(&snap, &c, &vec, &pl, &everyone(), VersionVector::new(3, 3));
        let real = run(&snap, &c, &ivf, &pl, &everyone(), VersionVector::new(3, 3));
        let set = |t: &Traverser| {
            t.ids
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
        };
        assert_eq!(set(&exact), set(&real));
        assert_eq!(set(&real), [10, 11].into_iter().collect());
    }

    #[test]
    fn ivfpq_backend_scopes_authz_at_source() {
        let (snap, mut c, _) = setup();
        c.set_node_label(2, 3); // person 2 sensitive
        let ivf = ivf_backend();
        let person = c.field_id("Person").unwrap();
        let pl = Pipeline {
            source: Source::TypeAnn {
                q: vec![0.9, 0.0], // nearest is person 2
                target_type: person,
                k: 5,
            },
            transforms: vec![],
            max_nodes: 100,
            mode: ReadMode::Fresh,
        };
        let p_no = Principal {
            allowed_labels: 0b1,
        };
        assert!(
            !run(&snap, &c, &ivf, &pl, &p_no, VersionVector::new(3, 3))
                .ids
                .contains(&2)
        );
        let p_yes = Principal {
            allowed_labels: 0b1 | (1 << 3),
        };
        assert!(
            run(&snap, &c, &ivf, &pl, &p_yes, VersionVector::new(3, 3))
                .ids
                .contains(&2)
        );
    }

    #[test]
    fn token_budget_bounds_result() {
        let (snap, c, vec) = setup();
        let person = c.field_id("Person").unwrap();
        let pl = Pipeline {
            source: Source::TypeAnn {
                q: vec![0.0, 0.0],
                target_type: person,
                k: 5,
            },
            transforms: vec![],
            max_nodes: 1, // token budget = 1 node
            mode: ReadMode::Fresh,
        };
        let t = run(&snap, &c, &vec, &pl, &everyone(), VersionVector::new(3, 3));
        assert_eq!(t.ids.len(), 1);
        assert_eq!(t.ids[0], 1); // nearest Person to origin
    }
}
