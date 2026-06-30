//! The Fact: the single unit of the StromaDB data model.
//!
//! `Fact = ⟨subject, predicate, object, valid-time, transaction-time, provenance, confidence⟩`.
//! Nodes and edges are projections of facts; every capability operates on this unit
//! (see SPEC / conceptual-model §1).

/// Interned id for a predicate or entity-type name (the Field-ID catalog, see [`crate::catalog`]).
pub type FieldId = u32;

/// Node identity.
pub type NodeId = u64;

/// The object of a fact: another node (an edge) or a literal value (an attribute).
#[derive(Clone, Debug, PartialEq)]
pub enum Object {
    Node(NodeId),
    Value(Value),
}

/// A literal value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
}

/// Bitemporal valid-time interval. `to == None` means the interval is open (currently valid).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidTime {
    pub from: i64,
    pub to: Option<i64>,
}

impl ValidTime {
    /// An interval open from `from` (currently valid).
    pub fn from(from: i64) -> Self {
        ValidTime { from, to: None }
    }
}

/// Whether a fact is a primary assertion or a derived (LLM/hypothesis) value. Queries default to
/// asserted; derived is returned only on explicit request (conceptual-model §1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProvenanceKind {
    Asserted,
    Derived,
}

/// Where a fact came from: its kind plus the interned source id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Provenance {
    pub kind: ProvenanceKind,
    pub source: FieldId,
}

/// A fact. `transaction_time` is assigned by the changelog on append (0 until persisted).
#[derive(Clone, Debug, PartialEq)]
pub struct Fact {
    pub subject: NodeId,
    pub predicate: FieldId,
    pub object: Object,
    pub valid_time: ValidTime,
    pub transaction_time: u64,
    pub provenance: Provenance,
    pub confidence: f32,
}
