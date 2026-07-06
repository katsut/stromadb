//! StromaDB core engine.
//!
//! A real-time GraphRAG engine. This crate is built up epic by epic from the
//! Phase 0-validated design (see `SPEC.md` / `docs/ARCHITECTURE.md`):
//! Fact model + type catalog (here) → fold/changelog → read-merge → type-aware hybrid →
//! version-vector snapshots → IVM/Live Query → composable IR.

pub mod calendar;
pub mod catalog;
pub mod changelog;
pub mod engine;
pub mod fact;
pub mod fold;
pub mod hash;
pub mod hybrid;
pub mod incremental;
pub mod ir;
pub mod ivf;
pub mod live;
pub mod query;
pub mod vector;
pub mod version;
pub mod wal;

pub use calendar::{Calendar, Stamp};
pub use catalog::{
    Cardinality, Catalog, ConstraintError, PredicateDef, Range, RelProps, ValueType,
};
pub use changelog::{Backpressure, Changelog, WriteKind};
pub use engine::Engine;
pub use fact::{Fact, FieldId, NodeId, Object, Provenance, ProvenanceKind, ValidTime, Value};
pub use fold::{Fold, ObjKey, Op, OrderKey, Snapshot, fold};
pub use incremental::{CompletenessRule, Maintained};
pub use ir::{AnnBackend, Cmp, Filter, Pipeline, Principal, Source, Transform, Traverser};
pub use live::{Diff, LiveQueries, QueryId};
pub use vector::VectorIndex;
pub use version::{ReadMode, VersionVector};
