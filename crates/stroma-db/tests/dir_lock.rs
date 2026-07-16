//! Data-directory lock: a second open of the same directory fails fast while a handle is alive,
//! and succeeds again once the first handle is dropped (the flock is released on drop).

use stromadb_store::Db;

fn tmp(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("stroma_lock_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d.join("db")
}

// flock is per open file description, so two independent opens of the LOCK file conflict even
// within one process — the same guard that keeps a second process out.
#[cfg(unix)]
#[test]
fn second_open_of_a_held_directory_fails() {
    let dir = tmp("conflict");
    Db::init(&dir).unwrap();
    let _db = Db::open(&dir).unwrap();

    let err = match Db::open(&dir) {
        Ok(_) => panic!("second open of a held directory must fail"),
        Err(e) => e,
    };
    assert!(
        err.contains("is in use by another process"),
        "unexpected error: {err}"
    );
    // the holder stamped its pid into the LOCK file, and the error surfaces it
    assert!(
        err.contains(&format!("(pid {})", std::process::id())),
        "unexpected error: {err}"
    );
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn reopen_succeeds_after_drop() {
    let dir = tmp("reopen");
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    drop(db);

    let db = Db::open(&dir).unwrap();
    drop(db);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}
