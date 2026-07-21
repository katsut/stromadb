//! Snapshot + truncate compaction: a compacted engine must be OBSERVATIONALLY IDENTICAL to a
//! never-compacted twin fed the same writes — current reads, as-of reads (superseded rows survive
//! the boundary), LWW tie-breaks (order keys serialize verbatim), and seqno continuity — while the
//! WAL shrinks to the tail. Plus the crash window: a committed snapshot next to a not-yet-truncated
//! WAL reconciles by seqno on open.

use stromadb_core::{Engine, ObjKey, WriteKind, query};

fn tmp(name: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("stroma_compact_{name}_{}", std::process::id()));
    cleanup(&p);
    p
}

/// Remove the WAL and every compaction sibling (`.snap`, `.archive-*`).
fn cleanup(path: &std::path::Path) {
    let dir = path.parent().unwrap();
    let stem = path.file_name().unwrap().to_string_lossy().to_string();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            if e.file_name().to_string_lossy().starts_with(&stem) {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }
}

/// A history-shaped workload phase: supersessions, closes, Many intervals with a revocation, an
/// edge property, node attributes — the shapes whose rows/tie-breaks a snapshot must preserve.
fn phase_one(e: &mut Engine) {
    let w = |e: &mut Engine, k: WriteKind| {
        e.write(0, k).unwrap();
    };
    w(
        e,
        WriteKind::SetNodeType {
            node: 1,
            type_id: 9,
        },
    );
    w(e, WriteKind::SetNodeLabel { node: 1, label: 2 });
    // One-key supersession chain incl. a retroactive correction (same valid_from tie at 150)
    for (obj, vf) in [(10u64, 100i64), (20, 200), (15, 150)] {
        w(
            e,
            WriteKind::SetOne {
                subject: 1,
                predicate: 0,
                object: ObjKey::Node(obj),
                valid_from: vf,
                valid_to: None,
            },
        );
    }
    // Many grants: 7 added→closed→re-added; 8 bounded [150, 300); 9 added then hard-retracted
    w(
        e,
        WriteKind::AddMany {
            subject: 1,
            predicate: 100,
            object: ObjKey::Node(7),
            valid_from: 100,
            valid_to: None,
        },
    );
    w(
        e,
        WriteKind::CloseMany {
            subject: 1,
            predicate: 100,
            object: ObjKey::Node(7),
            valid_from: 200,
        },
    );
    w(
        e,
        WriteKind::AddMany {
            subject: 1,
            predicate: 100,
            object: ObjKey::Node(7),
            valid_from: 400,
            valid_to: None,
        },
    );
    w(
        e,
        WriteKind::AddMany {
            subject: 1,
            predicate: 100,
            object: ObjKey::Node(8),
            valid_from: 150,
            valid_to: Some(300),
        },
    );
    w(
        e,
        WriteKind::AddMany {
            subject: 1,
            predicate: 100,
            object: ObjKey::Node(9),
            valid_from: 100,
            valid_to: None,
        },
    );
    e.retract_edge(0, 1, 100, ObjKey::Node(9)).unwrap();
    w(
        e,
        WriteKind::SetEdgeProp {
            subject: 1,
            predicate: 100,
            object: ObjKey::Node(7),
            key: "role".into(),
            value: ObjKey::Int(3),
        },
    );
    e.sync().unwrap();
    e.materialize();
}

/// Post-compaction phase: another supersession and a fresh grant, so the boundary is crossed.
fn phase_two(e: &mut Engine) {
    e.write(
        0,
        WriteKind::SetOne {
            subject: 1,
            predicate: 0,
            object: ObjKey::Node(30),
            valid_from: 300,
            valid_to: None,
        },
    )
    .unwrap();
    e.write(
        0,
        WriteKind::AddMany {
            subject: 1,
            predicate: 100,
            object: ObjKey::Node(11),
            valid_from: 500,
            valid_to: None,
        },
    )
    .unwrap();
    e.sync().unwrap();
    e.materialize();
}

/// Every read class the compaction contract covers, asserted equal between two engines.
fn assert_equivalent(a: &Engine, b: &Engine) {
    let (sa, sb) = (a.snapshot(), b.snapshot());
    assert_eq!(sa, sb, "observed snapshots must be identical");
    for at in [50i64, 100, 120, 150, 180, 220, 250, 320, 450, 550] {
        assert_eq!(
            query::point_one_asof(&sa, 1, 0, at),
            query::point_one_asof(&sb, 1, 0, at),
            "as-of One at {at}"
        );
        assert_eq!(
            query::point_many_asof(&sa, 1, 100, at),
            query::point_many_asof(&sb, 1, 100, at),
            "as-of Many at {at}"
        );
    }
    assert_eq!(a.durable_head(), b.durable_head(), "seqno continuity");
}

#[test]
fn compacted_engine_is_observationally_identical_across_reopen() {
    let cp = tmp("main");
    let tp = tmp("twin");

    // twin: same writes, never compacted
    let mut twin = Engine::open(&tp, 1024).unwrap();
    phase_one(&mut twin);
    phase_two(&mut twin);

    let mut e = Engine::open(&cp, 1024).unwrap();
    phase_one(&mut e);
    let wal_before = std::fs::metadata(&cp).unwrap().len();
    let covered = e.compact().unwrap();
    assert_eq!(covered, e.durable_head(), "compaction covers the full log");
    // the WAL shrank to (nearly) nothing and the covered prefix is archived, snapshot present
    let wal_after = std::fs::metadata(&cp).unwrap().len();
    assert!(
        wal_after < wal_before,
        "WAL must shrink ({wal_before} -> {wal_after})"
    );
    assert!(std::fs::metadata(format!("{}.snap", cp.display())).is_ok());
    assert!(std::fs::metadata(format!("{}.archive-{covered}", cp.display())).is_ok());
    // writes continue across the boundary with continuous seqnos
    phase_two(&mut e);
    assert_equivalent(&e, &twin);

    // cold reopen: snapshot load + tail replay must equal the never-compacted twin
    drop(e);
    let e = Engine::open(&cp, 1024).unwrap();
    assert_equivalent(&e, &twin);

    cleanup(&cp);
    cleanup(&tp);
}

#[test]
fn crash_window_snapshot_committed_but_wal_not_truncated_reconciles() {
    let cp = tmp("crash");
    let tp = tmp("crashtwin");

    let mut twin = Engine::open(&tp, 1024).unwrap();
    phase_one(&mut twin);

    let mut e = Engine::open(&cp, 1024).unwrap();
    phase_one(&mut e);
    let covered = e.compact().unwrap();
    drop(e);
    // simulate the crash window: the snapshot committed, but the old WAL was never truncated —
    // restore the archived full log over the fresh tail-only WAL
    std::fs::copy(format!("{}.archive-{covered}", cp.display()), &cp).unwrap();
    let e = Engine::open(&cp, 1024).unwrap();
    // the stale prefix (seqnos below the snapshot) is skipped by seqno, not replayed twice
    assert_equivalent(&e, &twin);

    cleanup(&cp);
    cleanup(&tp);
}

#[test]
fn second_compaction_and_empty_compaction_are_sound() {
    let cp = tmp("twice");
    let tp = tmp("twicetwin");

    let mut twin = Engine::open(&tp, 1024).unwrap();
    phase_one(&mut twin);
    phase_two(&mut twin);

    let mut e = Engine::open(&cp, 1024).unwrap();
    phase_one(&mut e);
    let first = e.compact().unwrap();
    phase_two(&mut e);
    let second = e.compact().unwrap();
    assert!(
        second > first,
        "the second compaction covers the newer head"
    );
    // an immediate third compaction over an empty tail is a no-op state-wise
    let third = e.compact().unwrap();
    assert_eq!(third, second);
    assert_equivalent(&e, &twin);

    drop(e);
    let e = Engine::open(&cp, 1024).unwrap();
    assert_equivalent(&e, &twin);

    cleanup(&cp);
    cleanup(&tp);
}
