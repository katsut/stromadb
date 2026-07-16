//! MCP stdio wiring: populate a DB, spawn the server, drive initialize / tools/list / tools/call
//! over newline-delimited JSON-RPC, assert responses.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde_json::{Value, json};
use stromadb_store::Db;

struct Mcp {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Mcp {
    fn call(&mut self, req: Value) -> Value {
        writeln!(self.stdin, "{req}").unwrap();
        self.stdin.flush().unwrap();
        let mut line = String::new();
        self.stdout.read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap()
    }
    fn notify(&mut self, req: Value) {
        writeln!(self.stdin, "{req}").unwrap();
        self.stdin.flush().unwrap();
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn mcp_initialize_list_call() {
    let base = std::env::temp_dir().join(format!("stroma_mcp_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let dir = base.join("db");
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"knows\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"status\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Person\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":2}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"status\",\"object\":{\"text\":\"active\"},\"valid_from\":100}}\n",
    ))
    .unwrap();
    drop(db);

    let mut child = Command::new(env!("CARGO_BIN_EXE_stroma-mcp"))
        .args(["--db", dir.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let stdout = BufReader::new(child.stdout.take().unwrap());
    let mut mcp = Mcp {
        child,
        stdin,
        stdout,
    };

    // initialize
    let r = mcp.call(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}));
    assert_eq!(r["result"]["serverInfo"]["name"], "stroma-mcp", "init: {r}");
    mcp.notify(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));

    // tools/list
    let r = mcp.call(json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}));
    let names: Vec<&str> = r["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"schema")
            && names.contains(&"search")
            && names.contains(&"expand")
            && names.contains(&"conformance"),
        "tools: {names:?}"
    );

    // tools/call schema → predicates are discoverable
    let r = mcp.call(json!({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"schema","arguments":{}}}));
    let text = r["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("predicates") && text.contains("knows") && text.contains("status"),
        "schema: {text}"
    );

    // tools/call expand → text content with [2]
    let r = mcp.call(json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"expand","arguments":{"subject":1,"predicate":"knows"}}}));
    let text = r["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("[2]"), "call result: {text}");

    // tools/call point with valid_at (as-of read of a one-cardinality predicate)
    let r = mcp.call(json!({"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"point","arguments":{"subject":1,"predicate":"status","valid_at":150}}}));
    let text = r["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("active"),
        "point valid_at (in effect): {text}"
    );
    // before the value's valid_from → no value in effect
    let r = mcp.call(json!({"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"point","arguments":{"subject":1,"predicate":"status","valid_at":50}}}));
    let text = r["result"]["content"][0]["text"].as_str().unwrap();
    assert!(!text.contains("active"), "point valid_at (before): {text}");

    // tools/call ingest (write) then read back
    let r = mcp.call(json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"ingest","arguments":{"jsonl":"{\"fact\":{\"subject\":2,\"predicate\":\"knows\",\"object\":{\"node\":1}}}"}}}));
    assert!(
        r["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"facts\":1"),
        "ingest: {r}"
    );
    let r = mcp.call(json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"expand","arguments":{"subject":2,"predicate":"knows"}}}));
    assert!(
        r["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("[1]")
    );

    // unknown method → JSON-RPC error
    let r = mcp.call(json!({"jsonrpc":"2.0","id":6,"method":"bogus"}));
    assert_eq!(r["error"]["code"], -32601, "err: {r}");

    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn mcp_conformance() {
    let base = std::env::temp_dir().join(format!("stroma_mcp_conf_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let dir = base.join("db");
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    // A tiny release-approval graph: department 1's manager transfers Alice(10) → Carol(12) at
    // valid-time 5000 (a one-cardinality `manager-of` that changes). The required derived path is
    // Issue --assigned-to--> Person --member-of--> Department --manager-of (as-of `approved-at`)-->
    // Person, compared to the issue's `approved-by`. Issue 1001 was approved at 1200 (Alice still
    // manager) → OK; issue 1002 was approved at 6000 (Carol is manager, but Alice held the role
    // before the transfer) → MISMATCH with kind `stale`.
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Department\"}}\n",
        "{\"type_def\":{\"name\":\"Issue\"}}\n",
        "{\"pred_def\":{\"name\":\"member-of\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range\":\"Department\"}}\n",
        "{\"pred_def\":{\"name\":\"manager-of\",\"cardinality\":\"one\",\"domain\":\"Department\",\"range\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"assigned-to\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"approved-by\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"approved-at\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range_value\":\"int\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Department\"}}\n",
        "{\"node\":{\"id\":10,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":12,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":201,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":1001,\"type\":\"Issue\"}}\n",
        "{\"node\":{\"id\":1002,\"type\":\"Issue\"}}\n",
        "{\"fact\":{\"subject\":201,\"predicate\":\"member-of\",\"object\":{\"node\":1},\"valid_from\":1000}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"manager-of\",\"object\":{\"node\":10},\"valid_from\":1000}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"manager-of\",\"object\":{\"node\":12},\"valid_from\":5000}}\n",
        "{\"fact\":{\"subject\":1001,\"predicate\":\"assigned-to\",\"object\":{\"node\":201}}}\n",
        "{\"fact\":{\"subject\":1001,\"predicate\":\"approved-at\",\"object\":{\"int\":1200}}}\n",
        "{\"fact\":{\"subject\":1001,\"predicate\":\"approved-by\",\"object\":{\"node\":10},\"valid_from\":1200}}\n",
        "{\"fact\":{\"subject\":1002,\"predicate\":\"assigned-to\",\"object\":{\"node\":201}}}\n",
        "{\"fact\":{\"subject\":1002,\"predicate\":\"approved-at\",\"object\":{\"int\":6000}}}\n",
        "{\"fact\":{\"subject\":1002,\"predicate\":\"approved-by\",\"object\":{\"node\":10},\"valid_from\":6000}}\n",
    ))
    .unwrap();
    drop(db);

    let mut child = Command::new(env!("CARGO_BIN_EXE_stroma-mcp"))
        .args(["--db", dir.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let stdout = BufReader::new(child.stdout.take().unwrap());
    let mut mcp = Mcp {
        child,
        stdin,
        stdout,
    };

    let r = mcp.call(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}));
    assert_eq!(r["result"]["serverInfo"]["name"], "stroma-mcp", "init: {r}");

    // tools/call conformance → deterministic per-subject verdicts for the declared rule.
    let rule = json!({
        "subject_type": "Issue",
        "required": { "hops": [
            { "predicate": "assigned-to" },
            { "predicate": "member-of" },
            { "predicate": "manager-of", "as_of": "approved-at" }
        ] },
        "actual": "approved-by"
    });
    let r = mcp.call(json!({"jsonrpc":"2.0","id":2,"method":"tools/call",
        "params":{"name":"conformance","arguments":{"rule":rule}}}));
    let text = r["result"]["content"][0]["text"].as_str().unwrap();
    let out: Value = serde_json::from_str(text).unwrap();
    let verdicts = out["verdicts"].as_array().unwrap();

    // 1001 approved at 1200 while Alice(10) was still the manager → OK.
    let v1001 = verdicts.iter().find(|v| v["subject"] == 1001).unwrap();
    assert_eq!(v1001["verdict"], "OK", "1001: {v1001}");

    // 1002 approved at 6000: as-of that anchor the required manager is Carol(12), not the approver
    // Alice(10) → MISMATCH; Alice held the role before the 5000 transfer → kind `stale`.
    let v1002 = verdicts.iter().find(|v| v["subject"] == 1002).unwrap();
    assert_eq!(v1002["verdict"], "MISMATCH", "1002: {v1002}");
    assert_eq!(v1002["kind"], "stale", "1002 kind: {v1002}");
    assert_eq!(v1002["required"], json!({ "node": 12 }));
    assert_eq!(v1002["actual"], json!({ "node": 10 }));
    assert_eq!(v1002["as_of"], json!(6000));

    // Declare the same rule by name (a rule_def ingest), then evaluate via `rule_name` → same verdicts.
    let rule_def = json!({ "rule_def": { "name": "approval", "rule": rule } }).to_string();
    let r = mcp.call(json!({"jsonrpc":"2.0","id":3,"method":"tools/call",
        "params":{"name":"ingest","arguments":{"jsonl":rule_def}}}));
    assert!(
        r["result"]["isError"] != json!(true),
        "rule_def ingest: {r}"
    );
    let r = mcp.call(json!({"jsonrpc":"2.0","id":4,"method":"tools/call",
        "params":{"name":"conformance","arguments":{"rule_name":"approval"}}}));
    let text = r["result"]["content"][0]["text"].as_str().unwrap();
    let out: Value = serde_json::from_str(text).unwrap();
    let v1002 = out["verdicts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["subject"] == 1002)
        .unwrap();
    assert_eq!(v1002["verdict"], "MISMATCH", "by name 1002: {v1002}");
    assert_eq!(v1002["kind"], "stale", "by name 1002 kind: {v1002}");

    let _ = std::fs::remove_dir_all(&base);
}
