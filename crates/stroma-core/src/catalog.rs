//! The ontology catalog: Field-ID interning + predicate/type definitions + minimal constraint
//! validation at the ingest boundary (conceptual-model §1, §4; SPEC CAP-6).
//!
//! The catalog is the *declarative* half of the ontology: vocabulary (predicates, types) and
//! structural rules (cardinality, relationship properties, domain/range). It does not reason; it
//! validates the minimum (domain/range types, predicate existence). Full reasoning is the caller's.

use std::collections::{HashMap, HashSet};

use crate::fact::{Fact, FieldId, Object, Value};

/// Predicate multiplicity — drives the fold behaviour (`One` → supersede / `Many` → accumulate).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

/// Relationship properties expanded cheaply at query time (never pre-materialized).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RelProps {
    pub symmetric: bool,
    pub transitive: bool,
    pub inverse: Option<FieldId>,
}

/// Literal value types (the value side of a predicate range).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueType {
    Int,
    Float,
    Text,
    Bool,
}

/// What a predicate points at: another entity type (an edge) or a literal value type (an attribute).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Range {
    Type(FieldId),
    Value(ValueType),
}

/// A registered predicate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PredicateDef {
    pub id: FieldId,
    pub cardinality: Cardinality,
    pub props: RelProps,
    pub domain: FieldId, // subject entity type
    pub range: Range,
}

/// Errors from minimal constraint validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConstraintError {
    UnknownPredicate(FieldId),
    DomainMismatch { expected: FieldId, got: FieldId },
    RangeTypeMismatch { expected: FieldId, got: FieldId },
    RangeValueMismatch { expected: ValueType, got: ValueType },
    ExpectedNodeObject,
    ExpectedValueObject,
}

#[derive(Default)]
struct Interner {
    by_name: HashMap<String, FieldId>,
    names: Vec<String>,
}

impl Interner {
    fn intern(&mut self, name: &str) -> FieldId {
        if let Some(&id) = self.by_name.get(name) {
            return id;
        }
        let id = self.names.len() as FieldId;
        self.names.push(name.to_owned());
        self.by_name.insert(name.to_owned(), id);
        id
    }
    fn get(&self, name: &str) -> Option<FieldId> {
        self.by_name.get(name).copied()
    }
    fn name(&self, id: FieldId) -> Option<&str> {
        self.names.get(id as usize).map(String::as_str)
    }
}

fn value_type(v: &Value) -> ValueType {
    match v {
        Value::Int(_) => ValueType::Int,
        Value::Float(_) => ValueType::Float,
        Value::Text(_) => ValueType::Text,
        Value::Bool(_) => ValueType::Bool,
    }
}

/// The ontology catalog. Predicates/types are interned and registered; node→type assignments enable
/// domain/range validation. Bounded by design (tens–hundreds of predicates).
#[derive(Default)]
pub struct Catalog {
    interner: Interner,
    types: HashSet<FieldId>,
    predicates: HashMap<FieldId, PredicateDef>,
    node_types: HashMap<crate::fact::NodeId, FieldId>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or look up) an entity type by name; returns its Field-ID.
    pub fn register_type(&mut self, name: &str) -> FieldId {
        let id = self.interner.intern(name);
        self.types.insert(id);
        id
    }

    /// Register a predicate. `domain`/`range` reference already-registered type ids.
    pub fn register_predicate(
        &mut self,
        name: &str,
        cardinality: Cardinality,
        props: RelProps,
        domain: FieldId,
        range: Range,
    ) -> FieldId {
        let id = self.interner.intern(name);
        self.predicates.insert(
            id,
            PredicateDef {
                id,
                cardinality,
                props,
                domain,
                range,
            },
        );
        id
    }

    /// Assign an entity type to a node (used by domain/range validation).
    pub fn set_node_type(&mut self, node: crate::fact::NodeId, type_id: FieldId) {
        self.node_types.insert(node, type_id);
    }

    pub fn field_id(&self, name: &str) -> Option<FieldId> {
        self.interner.get(name)
    }

    pub fn name(&self, id: FieldId) -> Option<&str> {
        self.interner.name(id)
    }

    pub fn predicate(&self, id: FieldId) -> Option<&PredicateDef> {
        self.predicates.get(&id)
    }

    /// Minimal ingest-boundary validation: the predicate must exist, the subject's type (if known)
    /// must match the predicate domain, and the object must match the predicate range.
    /// Cardinality is enforced later, by the fold (it is a multi-fact property).
    pub fn validate(&self, f: &Fact) -> Result<(), ConstraintError> {
        let pred = self
            .predicates
            .get(&f.predicate)
            .ok_or(ConstraintError::UnknownPredicate(f.predicate))?;

        if let Some(&subject_type) = self.node_types.get(&f.subject)
            && subject_type != pred.domain
        {
            return Err(ConstraintError::DomainMismatch {
                expected: pred.domain,
                got: subject_type,
            });
        }

        match (&pred.range, &f.object) {
            (Range::Type(t), Object::Node(n)) => {
                if let Some(&object_type) = self.node_types.get(n)
                    && object_type != *t
                {
                    return Err(ConstraintError::RangeTypeMismatch {
                        expected: *t,
                        got: object_type,
                    });
                }
            }
            (Range::Type(_), Object::Value(_)) => return Err(ConstraintError::ExpectedNodeObject),
            (Range::Value(vt), Object::Value(v)) => {
                let got = value_type(v);
                if got != *vt {
                    return Err(ConstraintError::RangeValueMismatch { expected: *vt, got });
                }
            }
            (Range::Value(_), Object::Node(_)) => return Err(ConstraintError::ExpectedValueObject),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fact::{Object, Provenance, ProvenanceKind, ValidTime, Value};

    fn fact(subject: u64, predicate: FieldId, object: Object) -> Fact {
        Fact {
            subject,
            predicate,
            object,
            valid_time: ValidTime::from(0),
            transaction_time: 0,
            provenance: Provenance {
                kind: ProvenanceKind::Asserted,
                source: 0,
            },
            confidence: 1.0,
        }
    }

    fn reference_catalog() -> (Catalog, FieldId, FieldId, FieldId) {
        let mut c = Catalog::new();
        let person = c.register_type("Person");
        let skill = c.register_type("Skill");
        let project = c.register_type("Project");
        // has-skill: Person -> Skill (many); member-of: Person -> Project (one)
        c.register_predicate(
            "has-skill",
            Cardinality::Many,
            RelProps::default(),
            person,
            Range::Type(skill),
        );
        c.register_predicate(
            "member-of",
            Cardinality::One,
            RelProps::default(),
            person,
            Range::Type(project),
        );
        (c, person, skill, project)
    }

    #[test]
    fn intern_is_stable() {
        let mut c = Catalog::new();
        let a = c.register_type("Person");
        let b = c.register_type("Person");
        assert_eq!(a, b);
        assert_eq!(c.field_id("Person"), Some(a));
        assert_eq!(c.name(a), Some("Person"));
    }

    #[test]
    fn predicate_registration_carries_cardinality() {
        let (c, _, _, _) = reference_catalog();
        let hs = c.field_id("has-skill").unwrap();
        assert_eq!(c.predicate(hs).unwrap().cardinality, Cardinality::Many);
        let mo = c.field_id("member-of").unwrap();
        assert_eq!(c.predicate(mo).unwrap().cardinality, Cardinality::One);
    }

    #[test]
    fn valid_fact_passes() {
        let (mut c, person, skill, _) = reference_catalog();
        let alice = 1u64;
        let rust = 2u64;
        c.set_node_type(alice, person);
        c.set_node_type(rust, skill);
        let hs = c.field_id("has-skill").unwrap();
        assert_eq!(c.validate(&fact(alice, hs, Object::Node(rust))), Ok(()));
    }

    #[test]
    fn unknown_predicate_rejected() {
        let (c, _, _, _) = reference_catalog();
        assert_eq!(
            c.validate(&fact(1, 999, Object::Node(2))),
            Err(ConstraintError::UnknownPredicate(999))
        );
    }

    #[test]
    fn domain_mismatch_rejected() {
        let (mut c, _person, skill, _) = reference_catalog();
        let n = 1u64;
        c.set_node_type(n, skill); // subject typed Skill, but has-skill domain is Person
        let hs = c.field_id("has-skill").unwrap();
        let person = c.field_id("Person").unwrap();
        assert_eq!(
            c.validate(&fact(n, hs, Object::Node(2))),
            Err(ConstraintError::DomainMismatch {
                expected: person,
                got: skill
            })
        );
    }

    #[test]
    fn range_type_mismatch_rejected() {
        let (mut c, person, _skill, project) = reference_catalog();
        let alice = 1u64;
        let proj = 2u64;
        c.set_node_type(alice, person);
        c.set_node_type(proj, project); // object is a Project, but has-skill range is Skill
        let hs = c.field_id("has-skill").unwrap();
        let skill = c.field_id("Skill").unwrap();
        assert_eq!(
            c.validate(&fact(alice, hs, Object::Node(proj))),
            Err(ConstraintError::RangeTypeMismatch {
                expected: skill,
                got: project
            })
        );
    }

    #[test]
    fn value_object_for_type_range_rejected() {
        let (mut c, person, _, _) = reference_catalog();
        let alice = 1u64;
        c.set_node_type(alice, person);
        let hs = c.field_id("has-skill").unwrap();
        assert_eq!(
            c.validate(&fact(alice, hs, Object::Value(Value::Int(3)))),
            Err(ConstraintError::ExpectedNodeObject)
        );
    }
}
