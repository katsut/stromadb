//! MCP stdio wiring: populate a DB, spawn the server, drive initialize / tools/list / tools/call
//! over newline-delimited JSON-RPC, assert responses.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde_json::{Value, json};
use stroma_db::Db;

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
    let mut db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"knows\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Person\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":2}}}\n",
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
        names.contains(&"search") && names.contains(&"expand"),
        "tools: {names:?}"
    );

    // tools/call expand → text content with [2]
    let r = mcp.call(json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"expand","arguments":{"subject":1,"predicate":"knows"}}}));
    let text = r["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("[2]"), "call result: {text}");

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
