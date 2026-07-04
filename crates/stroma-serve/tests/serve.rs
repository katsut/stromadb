//! HTTP wiring test: populate a DB, spawn the server, hit /health, /query, /ingest over raw HTTP.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::Duration;

use stroma_db::Db;

/// Minimal HTTP/1.1 client: returns (status, set-cookie token if any, body). An optional session
/// cookie is sent on the request.
fn http(
    addr: &str,
    method: &str,
    path: &str,
    body: &str,
    cookie: Option<&str>,
) -> (u16, Option<String>, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let cookie_hdr = cookie
        .map(|c| format!("Cookie: stroma_session={c}\r\n"))
        .unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n{cookie_hdr}Connection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let resp = String::from_utf8_lossy(&raw).into_owned();
    let status: u16 = resp
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let (head, body) = resp.split_once("\r\n\r\n").unwrap_or((&resp, ""));
    let set_cookie = head.lines().find_map(|l| {
        l.strip_prefix("Set-Cookie: stroma_session=")
            .and_then(|v| v.split(';').next())
            .filter(|v| !v.is_empty())
            .map(str::to_string)
    });
    (status, set_cookie, body.to_string())
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

    // /health is public (container probes need no auth)
    let (st, _, body) = http(&addr, "GET", "/health", "", None);
    assert_eq!(st, 200, "health: {body}");
    assert!(body.contains("\"ok\""), "health body: {body}");

    // unauthenticated API call is rejected
    let (st, _, _) = http(
        &addr,
        "POST",
        "/query",
        "{\"op\":\"expand\",\"subject\":1,\"predicate\":\"knows\"}",
        None,
    );
    assert_eq!(st, 401, "unauthenticated query must be 401");

    // unauthenticated page load gets the login page
    let (st, _, body) = http(&addr, "GET", "/", "", None);
    assert_eq!(st, 200, "login page: {body}");
    assert!(body.contains("Sign in"), "expected login page, got: {body}");

    // wrong credentials rejected
    let (st, _, _) = http(
        &addr,
        "POST",
        "/login",
        "{\"user\":\"admin\",\"password\":\"nope\"}",
        None,
    );
    assert_eq!(st, 401, "bad credentials must be 401");

    // default admin/password logs in and returns a session cookie
    let (st, cookie, _) = http(
        &addr,
        "POST",
        "/login",
        "{\"user\":\"admin\",\"password\":\"password\"}",
        None,
    );
    assert_eq!(st, 200, "login must succeed");
    let tok = cookie.expect("login must set a session cookie");

    // authenticated page load serves the app
    let (st, _, body) = http(&addr, "GET", "/", "", Some(&tok));
    assert_eq!(st, 200, "ui: {body}");
    assert!(
        body.contains("Draw neighbourhood"),
        "ui body missing app marker"
    );

    let (st, _, body) = http(
        &addr,
        "POST",
        "/query",
        "{\"op\":\"expand\",\"subject\":1,\"predicate\":\"knows\"}",
        Some(&tok),
    );
    assert_eq!(st, 200, "query: {body}");
    assert!(body.contains("[2]"), "query body: {body}");

    // live ingest over HTTP, then read it back
    let (st, _, _) = http(
        &addr,
        "POST",
        "/ingest",
        "{\"fact\":{\"subject\":2,\"predicate\":\"knows\",\"object\":{\"node\":1}}}",
        Some(&tok),
    );
    assert_eq!(st, 200);
    let (_, _, body) = http(
        &addr,
        "POST",
        "/query",
        "{\"op\":\"expand\",\"subject\":2,\"predicate\":\"knows\"}",
        Some(&tok),
    );
    assert!(body.contains("[1]"), "post-ingest query: {body}");

    // concurrent reads: many parallel /query requests must all succeed
    let mut threads = Vec::new();
    for _ in 0..16 {
        let a = addr.clone();
        let c = tok.clone();
        threads.push(std::thread::spawn(move || {
            let (st, _, body) = http(
                &a,
                "POST",
                "/query",
                "{\"op\":\"expand\",\"subject\":1,\"predicate\":\"knows\"}",
                Some(&c),
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

/// Minimal HTTP/1.1 client that sends an optional `Authorization: Bearer` header.
fn http_bearer(addr: &str, method: &str, path: &str, body: &str, bearer: Option<&str>) -> u16 {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let auth_hdr = bearer
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n{auth_hdr}Connection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let resp = String::from_utf8_lossy(&raw).into_owned();
    resp.split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

#[test]
fn serve_api_token_auth() {
    let base = std::env::temp_dir().join(format!("stroma_serve_token_test_{}", std::process::id()));
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

    let port = 8600 + (std::process::id() % 900) as u16;
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
        .args([
            "--db",
            dir.to_str().unwrap(),
            "--addr",
            &addr,
            "--api-token",
            "s3cr3t-token",
        ])
        .spawn()
        .unwrap();
    let _guard = Kill(child);

    let mut up = false;
    for _ in 0..50 {
        if TcpStream::connect(&addr).is_ok() {
            up = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(up, "server did not come up");

    let q = "{\"op\":\"expand\",\"subject\":1,\"predicate\":\"knows\"}";
    // no token → 401
    assert_eq!(
        http_bearer(&addr, "POST", "/query", q, None),
        401,
        "no token must be 401"
    );
    // wrong token → 401
    assert_eq!(
        http_bearer(&addr, "POST", "/query", q, Some("nope")),
        401,
        "wrong token must be 401"
    );
    // correct token → 200 (no login/cookie round-trip)
    assert_eq!(
        http_bearer(&addr, "POST", "/query", q, Some("s3cr3t-token")),
        200,
        "valid token must authorize"
    );
    // token also authorizes ingest
    assert_eq!(
        http_bearer(
            &addr,
            "POST",
            "/ingest",
            "{\"fact\":{\"subject\":2,\"predicate\":\"knows\",\"object\":{\"node\":1}}}",
            Some("s3cr3t-token")
        ),
        200,
        "valid token must authorize ingest"
    );

    let _ = std::fs::remove_dir_all(&base);
}
