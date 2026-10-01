//! HTTP wiring test: populate a DB, spawn the server, hit /health, /query, /ingest over raw HTTP.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::Duration;

use stromadb_store::Db;

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
    let db = Db::open(&dir).unwrap();
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
    // the left panel can be collapsed to a slim rail; the control is keyboard accessible
    // and its state is exposed via aria-expanded
    assert!(
        body.contains("id=\"askCollapse\"") && body.contains("aria-expanded=\"true\""),
        "ui body missing the panel collapse control"
    );
    assert!(
        body.contains("aria-controls=\"askBody\"") && body.contains("id=\"askBody\""),
        "collapse control must reference the panel body it toggles"
    );
    // the settings drawer keeps its groups in order, with the reset action last of all
    let at = |needle: &str| {
        body.find(needle)
            .unwrap_or_else(|| panic!("ui missing {needle}"))
    };
    let order = [
        at("id=\"grpServer\""),
        at("id=\"grpAdmin\""),
        at("id=\"grpDanger\""),
        at("id=\"resetBtn\""),
        at("</aside>"),
    ];
    assert!(order.is_sorted(), "settings groups out of order: {order:?}");

    // theme and language controls live in the topbar, not the settings drawer
    assert!(
        !body.contains("id=\"grpAppearance\""),
        "Appearance group must be removed from the settings drawer"
    );
    assert!(
        at("id=\"lang\"") < at("id=\"settings\"") && at("id=\"theme\"") < at("id=\"settings\""),
        "lang select and theme toggle must be in the topbar, before the settings drawer"
    );

    // fmtVal must coerce int/float to strings (#284): every caller chains .replace() onto its
    // result, which throws on a bare number. Pin the fix rather than the whole function body.
    assert!(
        body.contains("String(o.int)") && body.contains("String(o.float)"),
        "fmtVal must coerce int/float values to strings before callers call .replace() on them"
    );

    // inspect panel property rows (#288): value, source chip and confidence badge must be
    // separate elements (not one string blob), and a resolved node-ref name must keep its id
    // as a secondary, muted span rather than dropping it.
    assert!(
        body.contains("class=\"ins-vwrap\"")
            && body.contains("class=\"ins-src\"")
            && body.contains("class=\"ins-conf")
            && body.contains("class=\"ins-sec\""),
        "inspect panel rows must render value/source/confidence as separate elements"
    );

    // a console session is unrestricted and reports itself as such (no token identity)
    let (st, _, body) = http(&addr, "GET", "/me", "", Some(&tok));
    assert_eq!(st, 200, "me: {body}");
    let me: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(me["auth"], "session", "{me}");
    assert_eq!(me["user"], "admin");
    assert_eq!(me["read_only"], false);
    assert!(me["token_name"].is_null() && me["labels"].is_null(), "{me}");
    assert_eq!(me["mcp_url"], format!("http://{addr}/mcp"));

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
    let db = Db::open(&dir).unwrap();
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
    // a stored rule's declaration reads back over the HTTP query surface
    let rule = serde_json::json!({"subject_type": "Person", "required": {"hops": [{"predicate": "knows"}]}, "actual": "knows"});
    let def = serde_json::json!({"rule_def": {"name": "knows-self", "rule": rule}}).to_string();
    let (st, body) = http_bearer_body(&addr, "POST", "/ingest", &def, Some("s3cr3t-token"));
    assert_eq!(st, 200, "rule_def ingest: {body}");
    let (st, body) = http_bearer_body(
        &addr,
        "POST",
        "/query",
        "{\"op\":\"rule\",\"rule_name\":\"knows-self\"}",
        Some("s3cr3t-token"),
    );
    assert_eq!(st, 200, "rule read-back: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v, serde_json::json!({"name": "knows-self", "rule": rule}));
    // /reset is disabled by default (server started without --allow-reset) → 403
    assert_eq!(
        http_bearer(&addr, "POST", "/reset", "", Some("s3cr3t-token")),
        403,
        "reset must be disabled without --allow-reset"
    );
    // the refusal names the flag, and /me reports the same state and hint for the console
    let (st, body) = http_bearer_body(&addr, "POST", "/reset", "", Some("s3cr3t-token"));
    assert_eq!(st, 403);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let hint = v["error"].as_str().unwrap();
    assert!(hint.contains("start with --allow-reset"), "hint: {hint}");
    let (st, body) = http_bearer_body(&addr, "GET", "/me", "", Some("s3cr3t-token"));
    assert_eq!(st, 200);
    let me: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(me["allow_reset"], false);
    assert_eq!(me["read_only"], false);
    assert_eq!(me["reset_hint"], hint);
    // server info for the console's settings panel; the legacy token is unnamed and uncapped
    assert_eq!(me["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(
        me["db_path"],
        std::fs::canonicalize(&dir)
            .unwrap()
            .to_string_lossy()
            .as_ref()
    );
    assert!(me["workers"].as_u64().unwrap() >= 2, "{me}");
    assert_eq!(me["mcp_url"], format!("http://{addr}/mcp"));
    assert_eq!(me["auth"], "token");
    assert!(me["token_name"].is_null(), "{me}");
    assert!(me["labels"].is_null(), "{me}");

    let _ = std::fs::remove_dir_all(&base);
}

/// Minimal HTTP/1.1 client sending an optional bearer token; returns (status, body).
fn http_bearer_body(
    addr: &str,
    method: &str,
    path: &str,
    body: &str,
    bearer: Option<&str>,
) -> (u16, String) {
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
    let status: u16 = resp
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = resp
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

#[test]
fn serve_mcp_endpoint() {
    let base = std::env::temp_dir().join(format!("stroma_serve_mcp_test_{}", std::process::id()));
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
        "{\"fact\":{\"subject\":1,\"predicate\":\"status\",\"object\":{\"text\":\"active\"}}}\n",
    ))
    .unwrap();
    drop(db);

    let port = 9100 + (std::process::id() % 900) as u16;
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
        .args([
            "--db",
            dir.to_str().unwrap(),
            "--addr",
            &addr,
            "--api-token",
            "mcp-token",
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

    // /mcp honors the same auth gate as the other endpoints: no token → 401.
    let init = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}";
    let (st, _) = http_bearer_body(&addr, "POST", "/mcp", init, None);
    assert_eq!(st, 401, "unauthenticated /mcp must be 401");

    // initialize handshake: a request gets its JSON-RPC response as application/json.
    let (st, body) = http_bearer_body(&addr, "POST", "/mcp", init, Some("mcp-token"));
    assert_eq!(st, 200, "initialize: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["id"], 1, "initialize id: {body}");
    assert_eq!(v["result"]["protocolVersion"], "2024-11-05", "{body}");
    assert_eq!(v["result"]["serverInfo"]["name"], "stroma-mcp", "{body}");

    // a notification (no id) is accepted with 202 and an empty body.
    let note = "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}";
    let (st, body) = http_bearer_body(&addr, "POST", "/mcp", note, Some("mcp-token"));
    assert_eq!(st, 202, "notification: {body}");
    assert!(body.is_empty(), "notification body must be empty: {body}");

    // tools/list returns the full tool set (same schemas as the stdio binary).
    let list = "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}";
    let (st, body) = http_bearer_body(&addr, "POST", "/mcp", list, Some("mcp-token"));
    assert_eq!(st, 200, "tools/list: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let names: Vec<&str> = v["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    for tool in [
        "schema",
        "lookup",
        "point",
        "expand",
        "search",
        "retrieve_context",
        "conformance",
        "rule",
        "stats",
        "ingest",
    ] {
        assert!(names.contains(&tool), "missing tool {tool}: {names:?}");
    }

    // tools/call point reads the ingested one-cardinality value.
    let call = "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"point\",\"arguments\":{\"subject\":1,\"predicate\":\"status\"}}}";
    let (st, body) = http_bearer_body(&addr, "POST", "/mcp", call, Some("mcp-token"));
    assert_eq!(st, 200, "tools/call: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let text = v["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("active"), "point result: {text}");

    // no server-initiated stream: GET /mcp is 405.
    let (st, _) = http_bearer_body(&addr, "GET", "/mcp", "", Some("mcp-token"));
    assert_eq!(st, 405, "GET /mcp must be 405");

    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn serve_reset_when_enabled() {
    let base = std::env::temp_dir().join(format!("stroma_serve_reset_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let dir = base.join("db");
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"knows\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Person\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":2}}}\n",
    ))
    .unwrap();
    drop(db);
    let tokens = base.join("tokens.json");
    std::fs::write(
        &tokens,
        r#"{"tokens":[{"name":"viewer","token":"ro","labels":3,"read_only":true}]}"#,
    )
    .unwrap();

    let port = 8300 + (std::process::id() % 900) as u16;
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
        .args([
            "--db",
            dir.to_str().unwrap(),
            "--addr",
            &addr,
            "--api-token",
            "tok",
            "--tokens",
            tokens.to_str().unwrap(),
            "--allow-reset",
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

    // the fact is queryable before reset
    assert_eq!(
        http_bearer(
            &addr,
            "POST",
            "/query",
            "{\"op\":\"expand\",\"subject\":1,\"predicate\":\"knows\"}",
            Some("tok")
        ),
        200,
        "fact should be queryable before reset"
    );
    // /me reports the flag (no hint) and each caller's read-only bit
    let me = |tok: &str| -> serde_json::Value {
        let (st, body) = http_bearer_body(&addr, "GET", "/me", "", Some(tok));
        assert_eq!(st, 200);
        serde_json::from_str(&body).unwrap()
    };
    let rw = me("tok");
    assert_eq!(rw["allow_reset"], true);
    assert_eq!(rw["read_only"], false);
    assert!(rw["reset_hint"].is_null());
    let ro = me("ro");
    assert_eq!(ro["read_only"], true);
    // a named token reports its identity and label cap
    assert_eq!(ro["auth"], "token");
    assert_eq!(ro["token_name"], "viewer");
    assert_eq!(ro["labels"], 3);
    // a read-only token may not reset even with the flag
    assert_eq!(http_bearer(&addr, "POST", "/reset", "", Some("ro")), 403);
    let head = |tok: &str| -> u64 {
        let (st, body) = http_bearer_body(&addr, "GET", "/stats", "", Some(tok));
        assert_eq!(st, 200);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        v["facts"]["durable_head"].as_u64().unwrap()
    };
    assert!(head("tok") > 0, "seeded database has a durable head");
    assert_eq!(
        http_bearer(&addr, "POST", "/reset", "", Some("tok")),
        200,
        "reset must succeed when enabled"
    );
    assert_eq!(head("tok"), 0, "reset clears the durable head");
    // after reset the predicate is gone → query errors (400)
    assert_eq!(
        http_bearer(
            &addr,
            "POST",
            "/query",
            "{\"op\":\"expand\",\"subject\":1,\"predicate\":\"knows\"}",
            Some("tok")
        ),
        400,
        "predicate should be unknown after reset"
    );

    let _ = std::fs::remove_dir_all(&base);
}

/// Wait until the server accepts connections.
fn wait_up(addr: &str) {
    for _ in 0..50 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("server did not come up");
}

#[test]
fn serve_namespaces() {
    let base = std::env::temp_dir().join(format!("stroma_serve_ns_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let dir = base.join("db");
    let port = 10100 + (std::process::id() % 900) as u16;
    let addr = format!("127.0.0.1:{port}");
    let spawn = || {
        Kill(
            Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
                .args(["--db", dir.to_str().unwrap(), "--addr", &addr])
                .spawn()
                .unwrap(),
        )
    };
    let login = || {
        let (st, cookie, _) = http(
            &addr,
            "POST",
            "/login",
            "{\"user\":\"admin\",\"password\":\"password\"}",
            None,
        );
        assert_eq!(st, 200, "login must succeed");
        cookie.expect("login must set a session cookie")
    };
    let graph = concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"knows\",\"cardinality\":\"many\",\"domain\":\"Person\",\"range\":\"Person\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":2,\"type\":\"Person\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"knows\",\"object\":{\"node\":2}}}\n",
    );
    let expand = "{\"op\":\"expand\",\"subject\":1,\"predicate\":\"knows\"}";
    {
        let _guard = spawn();
        wait_up(&addr);
        // the auth gate covers namespaced routes; a namespace's console gets the login page
        assert_eq!(http(&addr, "GET", "/ns/a/stats", "", None).0, 401);
        assert_eq!(http(&addr, "GET", "/namespaces", "", None).0, 401);
        let (st, _, body) = http(&addr, "GET", "/ns/a/", "", None);
        assert_eq!(st, 200);
        assert!(body.contains("Sign in"), "expected login page, got: {body}");

        let tok = login();
        let (st, _, body) = http(&addr, "GET", "/ns/a/stats", "", Some(&tok));
        assert_eq!(st, 404, "unknown namespace read: {body}");
        assert_eq!(http(&addr, "GET", "/ns/Bad!/stats", "", Some(&tok)).0, 400);
        let (st, _, body) = http(&addr, "POST", "/ns/a/ingest", graph, Some(&tok));
        assert_eq!(st, 200, "ingest into a: {body}");
        let (_, _, body) = http(&addr, "POST", "/ns/a/query", expand, Some(&tok));
        assert!(body.contains("[2]"), "a sees its fact: {body}");
        let (_, _, body) = http(&addr, "POST", "/query", expand, Some(&tok));
        assert!(!body.contains("[2]"), "default leaked: {body}");
        let (st, _, body) = http(&addr, "GET", "/ns/a/", "", Some(&tok));
        assert_eq!(st, 200);
        assert!(body.contains("Draw neighbourhood"), "namespace console");
        let (st, _, body) = http(&addr, "GET", "/namespaces", "", Some(&tok));
        assert_eq!(st, 200);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let names: Vec<&str> = v["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["default", "a"]);
        assert_eq!(
            v["namespaces"][0]["nodes"], 0,
            "default is untouched: {body}"
        );
        assert_eq!(v["namespaces"][1]["nodes"], 2, "a has 2 nodes: {body}");
        assert_eq!(
            v["namespaces"][1]["facts"], 3,
            "a has 3 durable ops: {body}"
        );
    }
    // restart on the same directory: the namespace is still there, counts survive
    let _guard = spawn();
    wait_up(&addr);
    let tok = login();
    let (_, _, body) = http(&addr, "GET", "/namespaces", "", Some(&tok));
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let names: Vec<&str> = v["namespaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["default", "a"]);
    assert_eq!(v["namespaces"][1]["nodes"], 2, "a after restart: {body}");
    let (_, _, body) = http(&addr, "POST", "/ns/a/query", expand, Some(&tok));
    assert!(body.contains("[2]"), "a after restart: {body}");
    drop(_guard);
    let _ = std::fs::remove_dir_all(&base);
}

// `--app-url` / `$STROMA_APP_URL`: an optional link back to the app the database feeds, reported
// by `GET /me` as `app_url` (the console's topbar "Back to app" link; hidden when unset).
#[test]
fn me_reports_app_url_when_set_by_flag_or_env() {
    let base = std::env::temp_dir().join(format!("stroma_appurl_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let dir = base.join("db");
    Db::init(&dir).unwrap();
    drop(Db::open(&dir).unwrap());

    let login = |addr: &str| -> String {
        let (st, cookie, _) = http(
            addr,
            "POST",
            "/login",
            "{\"user\":\"admin\",\"password\":\"password\"}",
            None,
        );
        assert_eq!(st, 200, "login must succeed");
        cookie.expect("login must set a session cookie")
    };
    let me = |addr: &str, tok: &str| -> serde_json::Value {
        let (st, _, body) = http(addr, "GET", "/me", "", Some(tok));
        assert_eq!(st, 200);
        serde_json::from_str(&body).unwrap()
    };

    // unset: `app_url` is absent (null)
    let port = 11100 + (std::process::id() % 900) as u16;
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
        .args(["--db", dir.to_str().unwrap(), "--addr", &addr])
        .spawn()
        .unwrap();
    let _guard = Kill(child);
    wait_up(&addr);
    let tok = login(&addr);
    assert!(me(&addr, &tok)["app_url"].is_null(), "unset app_url");
    drop(_guard);

    // set via --app-url
    let port = 11200 + (std::process::id() % 900) as u16;
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
        .args([
            "--db",
            dir.to_str().unwrap(),
            "--addr",
            &addr,
            "--app-url",
            "https://app.example.com/",
        ])
        .spawn()
        .unwrap();
    let _guard = Kill(child);
    wait_up(&addr);
    let tok = login(&addr);
    assert_eq!(
        me(&addr, &tok)["app_url"],
        "https://app.example.com/",
        "flag-set app_url"
    );
    drop(_guard);

    // set via $STROMA_APP_URL
    let port = 11300 + (std::process::id() % 900) as u16;
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
        .args(["--db", dir.to_str().unwrap(), "--addr", &addr])
        .env("STROMA_APP_URL", "https://app.example.com/env")
        .spawn()
        .unwrap();
    let _guard = Kill(child);
    wait_up(&addr);
    let tok = login(&addr);
    assert_eq!(
        me(&addr, &tok)["app_url"],
        "https://app.example.com/env",
        "env-set app_url"
    );
    drop(_guard);

    let _ = std::fs::remove_dir_all(&base);
}
