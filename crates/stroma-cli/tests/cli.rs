//! End-to-end CLI test: init → ingest (defs/nodes/facts/retract) → embed → query → stats,
//! each step a separate process, so catalog replay + WAL recovery + embedding reload across
//! restarts is exercised too.

use std::path::PathBuf;
use std::process::Command;

fn stroma(args: &[&str]) -> (bool, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_stroma"))
        .args(args)
        .output()
        .unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr),
    )
}

#[test]
fn cli_end_to_end() {
    let tmp = std::env::temp_dir().join(format!("stroma_cli_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let db = tmp.join("db");
    let dbs = db.to_str().unwrap();

    let data: PathBuf = tmp.join("data.jsonl");
    std::fs::write(
        &data,
        concat!(
            "{\"type_def\":{\"name\":\"Person\"}}\n",
            "{\"type_def\":{\"name\":\"Project\"}}\n",
            "{\"pred_def\":{\"name\":\"works-on\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Project\"}}\n",
            "{\"pred_def\":{\"name\":\"age\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"int\"}}\n",
            "{\"node\":{\"id\":1,\"type\":\"Person\",\"label\":0}}\n",
            "{\"node\":{\"id\":2,\"type\":\"Project\",\"label\":0}}\n",
            "{\"node\":{\"id\":3,\"type\":\"Project\",\"label\":3}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"works-on\",\"object\":{\"node\":2}}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"works-on\",\"object\":{\"node\":3}}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"age\",\"object\":{\"int\":34}}}\n",
            "{\"fact\":{\"subject\":1,\"predicate\":\"age\",\"object\":{\"int\":35},\"valid_from\":1}}\n",
            "{\"retract\":{\"subject\":1,\"predicate\":\"works-on\",\"object\":{\"node\":3}}}\n",
        ),
    )
    .unwrap();
    let emb = tmp.join("emb.jsonl");
    std::fs::write(
        &emb,
        "{\"node\":1,\"vector\":[1.0,0.0,0.0,0.0]}\n{\"node\":2,\"vector\":[0.9,0.1,0.0,0.0]}\n{\"node\":3,\"vector\":[0.0,1.0,0.0,0.0]}\n",
    )
    .unwrap();
    let qf = tmp.join("q.json");
    std::fs::write(&qf, "[1.0,0.0,0.0,0.0]").unwrap();

    let (ok, out) = stroma(&["init", "--db", dbs]);
    assert!(ok, "init failed: {out}");
    let (ok, out) = stroma(&["ingest", data.to_str().unwrap(), "--db", dbs]);
    assert!(
        ok && out.contains("4 facts") && out.contains("1 retracts"),
        "ingest: {out}"
    );
    let (ok, out) = stroma(&["embed", emb.to_str().unwrap(), "--db", dbs]);
    assert!(ok && out.contains("3 vectors"), "embed: {out}");

    // LWW: the later valid_from wins for cardinality-one
    let (ok, out) = stroma(&["query", "point", "1", "age", "--db", dbs]);
    assert!(ok && out.contains("\"int\":35"), "point: {out}");
    // retract removed node 3 from the OR-set
    let (ok, out) = stroma(&["query", "expand", "1", "works-on", "--db", dbs]);
    assert!(ok && out.contains("[2]"), "expand: {out}");
    // typed search: Person node 1 excluded despite nearest vector; both projects returned
    let (ok, out) = stroma(&[
        "query",
        "search",
        "--type",
        "Project",
        "--k",
        "5",
        "--vector-file",
        qf.to_str().unwrap(),
        "--db",
        dbs,
    ]);
    assert!(ok && out.contains("[2,3]"), "search: {out}");
    // authz: label 3 denied when only label 0 allowed
    let (ok, out) = stroma(&[
        "query",
        "search",
        "--type",
        "Project",
        "--k",
        "5",
        "--vector-file",
        qf.to_str().unwrap(),
        "--allowed-labels",
        "1",
        "--db",
        dbs,
    ]);
    assert!(ok && out.contains("\"ids\":[2]"), "authz search: {out}");

    let (ok, out) = stroma(&["stats", "--db", dbs]);
    assert!(ok && out.contains("\"durable_head\": 5"), "stats: {out}");

    let _ = std::fs::remove_dir_all(&tmp);
}
