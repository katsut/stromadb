//! Incremental conformance maintenance vs full re-evaluation — the cost claim behind
//! `MaintainedConformance`: after a write, updating the maintained verdict map is O(touched)
//! (re-judge only the subjects whose support keys changed) while a full `conformance::evaluate`
//! is O(subjects). This bench grows the graph and measures both per write.
//!
//!   cargo run --release --example conformance_ivm [-- <subjects,...>]
//!
//! Default sizes: 1000 5000 25000 100000. Per size it applies WRITES single-subject writes
//! (approval lands on one issue) and TRANSFERS manager transfers (one upstream write that
//! cascades to every issue assigned through the department — the fan-out case), timing
//! maintained.apply vs a full evaluate after each.

#[path = "util/mod.rs"]
mod util;

use std::collections::BTreeMap;
use std::time::Instant;

use stromadb_core::catalog::{Cardinality, Catalog, Range, RelProps, ValueType};
use stromadb_core::changelog::WriteKind;
use stromadb_core::conformance::{self, Cond, Hop, Rule};
use stromadb_core::engine::Engine;
use stromadb_core::fold::ObjKey;
use stromadb_core::incremental::MaintainedConformance;

const WRITES: usize = 200;
const TRANSFERS: usize = 50;
const DEPTS: u64 = 20;
const PERSONS: u64 = 200;

fn main() {
    let sizes: Vec<usize> = std::env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    let sizes = if sizes.is_empty() {
        vec![1_000, 5_000, 25_000, 100_000]
    } else {
        sizes
    };

    println!(
        "{:>9} | {:>14} {:>14} | {:>14} {:>14} | {:>8}",
        "subjects", "incr/write", "full/write", "incr/transfer", "full/transfer", "speedup"
    );
    for n in sizes {
        run(n);
    }
}

fn run(n_issues: usize) {
    let mut cat = Catalog::new();
    let issue = cat.register_type("Issue");
    let person = cat.register_type("Person");
    let dept = cat.register_type("Department");
    let d = RelProps::default();
    let assigned_to = cat.register_predicate(
        "assigned-to",
        Cardinality::One,
        d,
        issue,
        Range::Type(person),
    );
    let member_of =
        cat.register_predicate("member-of", Cardinality::One, d, person, Range::Type(dept));
    let manager_of =
        cat.register_predicate("manager-of", Cardinality::One, d, dept, Range::Type(person));
    let approved_by = cat.register_predicate(
        "approved-by",
        Cardinality::One,
        d,
        issue,
        Range::Type(person),
    );
    let approved_at = cat.register_predicate(
        "approved-at",
        Cardinality::One,
        d,
        issue,
        Range::Value(ValueType::Int),
    );
    let status = cat.register_predicate(
        "status",
        Cardinality::One,
        d,
        issue,
        Range::Value(ValueType::Text),
    );
    let rule = Rule {
        subject_type: "Issue".into(),
        scope: None,
        required: vec![
            Hop {
                predicate: "assigned-to".into(),
                as_of: None,
            },
            Hop {
                predicate: "member-of".into(),
                as_of: None,
            },
            Hop {
                predicate: "manager-of".into(),
                as_of: Some("approved-at".into()),
            },
        ],
        distinct_from: Vec::new(),
        actual: "approved-by".into(),
        absent_when: Some(Cond {
            predicate: "status".into(),
            equals: ObjKey::Text("released".into()),
        }),
    };

    // build: persons in departments, issues assigned round-robin, all approved + released
    let mut eng = Engine::new(1 << 22);
    let mut seed = 0xBEEF_0000_0000_0000u64 ^ n_issues as u64;
    let mut rnd = || {
        util::splitmix(&mut seed);
        seed
    };
    let set = |s: u64, p: u32, o: ObjKey, vf: i64| WriteKind::SetOne {
        subject: s,
        predicate: p,
        object: o,
        valid_from: vf,
        valid_to: None,
    };
    for pid in 1..=PERSONS {
        eng.write(
            0,
            WriteKind::SetNodeType {
                node: pid,
                type_id: person,
            },
        )
        .unwrap();
        eng.write(0, set(pid, member_of, ObjKey::Node(1_000 + pid % DEPTS), 0))
            .unwrap();
    }
    for dp in 0..DEPTS {
        eng.write(
            0,
            WriteKind::SetNodeType {
                node: 1_000 + dp,
                type_id: dept,
            },
        )
        .unwrap();
        eng.write(
            0,
            set(1_000 + dp, manager_of, ObjKey::Node(1 + dp % PERSONS), 0),
        )
        .unwrap();
    }
    for i in 0..n_issues as u64 {
        let id = 10_000 + i;
        eng.write(
            0,
            WriteKind::SetNodeType {
                node: id,
                type_id: issue,
            },
        )
        .unwrap();
        eng.write(0, set(id, assigned_to, ObjKey::Node(1 + i % PERSONS), 0))
            .unwrap();
        eng.write(0, set(id, approved_at, ObjKey::Int(1_000), 0))
            .unwrap();
        eng.write(
            0,
            set(id, approved_by, ObjKey::Node(1 + (i + 1) % PERSONS), 0),
        )
        .unwrap();
        eng.write(0, set(id, status, ObjKey::Text("released".into()), 0))
            .unwrap();
    }
    eng.materialize();
    let mut maintained = MaintainedConformance::new(rule.clone(), &eng.snapshot(), &cat);

    let full = |snap: &stromadb_core::fold::Snapshot| -> BTreeMap<u64, conformance::Verdict> {
        conformance::evaluate(snap, &cat, &rule, u32::MAX)
            .into_iter()
            .map(|v| (v.subject, v))
            .collect()
    };

    // single-subject writes: an approval lands on one issue
    let mut incr_w = 0.0f64;
    let mut full_w = 0.0f64;
    for _ in 0..WRITES {
        let id = 10_000 + rnd() % n_issues as u64;
        eng.write(
            0,
            set(id, approved_by, ObjKey::Node(1 + rnd() % PERSONS), 0),
        )
        .unwrap();
        let (keys, nodes) = eng.materialize_tracked_with_nodes();
        let snap = eng.snapshot_arc();
        let t = Instant::now();
        maintained.apply(&snap, &cat, &keys, &nodes);
        incr_w += t.elapsed().as_secs_f64();
        let t = Instant::now();
        std::hint::black_box(full(&snap));
        full_w += t.elapsed().as_secs_f64();
    }

    // manager transfers: one upstream write cascading through a department's assignees
    let mut incr_t = 0.0f64;
    let mut full_t = 0.0f64;
    for _ in 0..TRANSFERS {
        let dp = 1_000 + rnd() % DEPTS;
        eng.write(
            0,
            set(dp, manager_of, ObjKey::Node(1 + rnd() % PERSONS), 2_000),
        )
        .unwrap();
        let (keys, nodes) = eng.materialize_tracked_with_nodes();
        let snap = eng.snapshot_arc();
        let t = Instant::now();
        maintained.apply(&snap, &cat, &keys, &nodes);
        incr_t += t.elapsed().as_secs_f64();
        let t = Instant::now();
        std::hint::black_box(full(&snap));
        full_t += t.elapsed().as_secs_f64();
    }

    // sanity: the maintained map still equals the full evaluation
    assert_eq!(maintained.verdicts(), &full(&eng.snapshot_arc()));

    let us = |total: f64, n: usize| total / n as f64 * 1e6;
    println!(
        "{:>9} | {:>12.1}µs {:>12.1}µs | {:>12.1}µs {:>12.1}µs | {:>7.0}x",
        n_issues,
        us(incr_w, WRITES),
        us(full_w, WRITES),
        us(incr_t, TRANSFERS),
        us(full_t, TRANSFERS),
        us(full_w, WRITES) / us(incr_w, WRITES).max(1e-9),
    );
}
