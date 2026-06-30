//! StromaDB core engine.
//!
//! A no-LLM neuro-symbolic knowledge-graph core. This crate is built up epic by epic from the
//! Phase 0-validated design (see `SPEC.md` / `docs/ARCHITECTURE.md`):
//! Fact model + ontology catalog (here) → fold/changelog → read-merge → type-aware hybrid →
//! version-vector snapshots → IVM/Live Query → composable IR.

pub mod catalog;
pub mod changelog;
pub mod engine;
pub mod fact;
pub mod fold;
pub mod query;

pub use catalog::{
    Cardinality, Catalog, ConstraintError, PredicateDef, Range, RelProps, ValueType,
};
pub use changelog::{Backpressure, Changelog, WriteKind};
pub use engine::Engine;
pub use fact::{Fact, FieldId, NodeId, Object, Provenance, ProvenanceKind, ValidTime, Value};
pub use fold::{Fold, ObjKey, Op, OrderKey, Snapshot, fold};
