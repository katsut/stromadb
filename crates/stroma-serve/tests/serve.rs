//! HTTP wiring test: populate a DB, spawn the server, hit /health, /query, /ingest over raw HTTP.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::Duration;

use stroma_db::Db;

fn http(addr: &str, method: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).unwrap();
    let mut resp = String::new();
    stream.read_to_string(&mut resp).unwrap();
    let status: u16 = resp
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = resp.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

struct Kill(Child);
impl Drop for Kill {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn serve_health_query_ingest() {
    let base = std::env::temp_dir().join(format!("stroma_serve_test_{}", std::process::id()));
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

    let port = 7700 + (std::process::id() % 900) as u16;
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
        .args(["--db", dir.to_str().unwrap(), "--addr", &addr])
        .spawn()
        .unwrap();
    let _guard = Kill(child);

    // wait for bind
    let mut up = false;
    for _ in 0..50 {
        if TcpStream::connect(&addr).is_ok() {
            up = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(up, "server did not come up");

    let (st, body) = http(&addr, "GET", "/health", "");
    assert_eq!(st, 200, "health: {body}");
    assert!(body.contains("\"ok\""), "health body: {body}");

    // web UI is served at /
    let (st, body) = http(&addr, "GET", "/", "");
    assert_eq!(st, 200, "ui: {body}");
    assert!(body.contains("<title>StromaDB"), "ui body missing title");

    let (st, body) = http(
        &addr,
        "POST",
        "/query",
        "{\"op\":\"expand\",\"subject\":1,\"predicate\":\"knows\"}",
    );
    assert_eq!(st, 200, "query: {body}");
    assert!(body.contains("[2]"), "query body: {body}");

    // live ingest over HTTP, then read it back
    let (st, _) = http(
        &addr,
        "POST",
        "/ingest",
        "{\"fact\":{\"subject\":2,\"predicate\":\"knows\",\"object\":{\"node\":1}}}",
    );
    assert_eq!(st, 200);
    let (_, body) = http(
        &addr,
        "POST",
        "/query",
        "{\"op\":\"expand\",\"subject\":2,\"predicate\":\"knows\"}",
    );
    assert!(body.contains("[1]"), "post-ingest query: {body}");

    // concurrent reads: many parallel /query requests must all succeed
    let mut threads = Vec::new();
    for _ in 0..16 {
        let a = addr.clone();
        threads.push(std::thread::spawn(move || {
            let (st, body) = http(
                &a,
                "POST",
                "/query",
                "{\"op\":\"expand\",\"subject\":1,\"predicate\":\"knows\"}",
            );
            (st, body.contains("[2]"))
        }));
    }
    for t in threads {
        let (st, ok) = t.join().unwrap();
        assert_eq!(st, 200);
        assert!(ok, "concurrent read returned unexpected body");
    }

    let _ = std::fs::remove_dir_all(&base);
}
