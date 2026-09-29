//! `--help` and unknown-flag handling: both must be resolved before any side effect (no server
//! bind, no database directory created), and both must exit with the documented code. Regression
//! test for #239 (`stroma up --help` used to boot a server and create `./stroma-db`).

use std::path::PathBuf;
use std::process::{Command, Output};

fn stroma_in(cwd: &PathBuf, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_stroma"))
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap()
}

fn fresh_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("stroma_help_test_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn out_str(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr)
}

#[test]
fn up_help_prints_usage_without_side_effects() {
    let cwd = fresh_dir("up_help");
    let out = stroma_in(&cwd, &["up", "--help"]);
    assert_eq!(out.status.code(), Some(0), "output: {}", out_str(&out));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("usage: stroma serve|up"), "{stdout}");
    assert!(stdout.contains("--db <dir>"), "{stdout}");
    assert!(
        !cwd.join("stroma-db").exists(),
        "up --help must not create a database directory"
    );
    let _ = std::fs::remove_dir_all(&cwd);
}

#[test]
fn up_unknown_flag_is_rejected_without_side_effects() {
    let cwd = fresh_dir("up_unknown");
    let out = stroma_in(&cwd, &["up", "--bogus"]);
    assert_eq!(out.status.code(), Some(2), "output: {}", out_str(&out));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("error: unknown flag --bogus"), "{stderr}");
    assert!(stderr.contains("usage: stroma serve|up"), "{stderr}");
    assert!(
        !cwd.join("stroma-db").exists(),
        "an unknown flag must not create a database directory"
    );
    let _ = std::fs::remove_dir_all(&cwd);
}

#[test]
fn serve_help_and_unknown_flag() {
    let cwd = fresh_dir("serve_help");
    let out = stroma_in(&cwd, &["serve", "-h"]);
    assert_eq!(out.status.code(), Some(0), "output: {}", out_str(&out));

    let out = stroma_in(&cwd, &["serve", "--nope"]);
    assert_eq!(out.status.code(), Some(2), "output: {}", out_str(&out));
    assert!(!cwd.join("stroma-db").exists());
    let _ = std::fs::remove_dir_all(&cwd);
}

#[test]
fn top_level_help_lists_subcommands() {
    let cwd = fresh_dir("top_help");
    let out = stroma_in(&cwd, &["--help"]);
    assert_eq!(out.status.code(), Some(0), "output: {}", out_str(&out));
    let stdout = String::from_utf8_lossy(&out.stdout);
    for sub in [
        "init", "ingest", "import", "embed", "query", "stats", "serve", "up",
    ] {
        assert!(stdout.contains(sub), "missing {sub} in: {stdout}");
    }
    let _ = std::fs::remove_dir_all(&cwd);
}

#[test]
fn init_help_and_unknown_flag_do_not_create_a_database() {
    let cwd = fresh_dir("init_help");
    let out = stroma_in(&cwd, &["init", "--help"]);
    assert_eq!(out.status.code(), Some(0), "output: {}", out_str(&out));
    assert!(!cwd.join("wal.log").exists());

    let out = stroma_in(&cwd, &["init", "--bogus"]);
    assert_eq!(out.status.code(), Some(2), "output: {}", out_str(&out));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("error: unknown flag --bogus"), "{stderr}");
    assert!(!cwd.join("wal.log").exists());
    let _ = std::fs::remove_dir_all(&cwd);
}
