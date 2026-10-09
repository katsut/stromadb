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

    // live ingest over HTTP, then read it back; the answer brackets the batch's heads
    let (st, _, body) = http(
        &addr,
        "POST",
        "/ingest",
        "{\"fact\":{\"subject\":2,\"predicate\":\"knows\",\"object\":{\"node\":1}}}",
        Some(&tok),
    );
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["durable_head"].as_u64().unwrap(),
        v["head_before"].as_u64().unwrap() + 1,
        "one fact = one head: {body}"
    );
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

// The console no longer carries a link back to any external application, so the flag that used
// to set it is gone: `--app-url` is rejected like any other unrecognized option (exit 2, no
// server started), and `GET /me` no longer reports an `app_url` field at all.
#[test]
fn app_url_flag_is_rejected_as_unknown() {
    let base = std::env::temp_dir().join(format!("stroma_appurl_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let dir = base.join("db");
    Db::init(&dir).unwrap();
    drop(Db::open(&dir).unwrap());

    let port = 11100 + (std::process::id() % 900) as u16;
    let addr = format!("127.0.0.1:{port}");
    let output = Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
        .args([
            "--db",
            dir.to_str().unwrap(),
            "--addr",
            &addr,
            "--app-url",
            "https://app.example.com/",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "unknown flag must exit 2");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unknown flag --app-url"),
        "stderr: {stderr}"
    );

    let _ = std::fs::remove_dir_all(&base);
}

// `GET /me` carries only the server facts the console still uses — no app back-link field.
#[test]
fn me_has_no_app_url_field() {
    let base = std::env::temp_dir().join(format!("stroma_noappurl_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let dir = base.join("db");
    Db::init(&dir).unwrap();
    drop(Db::open(&dir).unwrap());

    let port = 11200 + (std::process::id() % 900) as u16;
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
        .args(["--db", dir.to_str().unwrap(), "--addr", &addr])
        .spawn()
        .unwrap();
    let _guard = Kill(child);
    wait_up(&addr);

    let (st, cookie, _) = http(
        &addr,
        "POST",
        "/login",
        "{\"user\":\"admin\",\"password\":\"password\"}",
        None,
    );
    assert_eq!(st, 200, "login must succeed");
    let tok = cookie.expect("login must set a session cookie");
    let (st, _, body) = http(&addr, "GET", "/me", "", Some(&tok));
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(
        v.get("app_url").is_none(),
        "/me must not report app_url: {body}"
    );
    drop(_guard);

    let _ = std::fs::remove_dir_all(&base);
}

/// Read from `stream` until `buf` contains `needle` (or the read times out).
fn read_until(stream: &mut TcpStream, buf: &mut String, needle: &str) -> bool {
    let mut chunk = [0u8; 4096];
    while !buf.contains(needle) {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return false,
            Ok(n) => buf.push_str(&String::from_utf8_lossy(&chunk[..n])),
        }
    }
    true
}

// The change stream over a real connection: an event-stream response, chunked, whose first event
// announces the head and whose next event carries a write made after it opened, as soon as it
// lands. `Last-Event-ID` resumes after a given head.
#[test]
fn serve_change_stream() {
    let base = std::env::temp_dir().join(format!("stroma_serve_sse_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let dir = base.join("db");
    let port = 12100 + (std::process::id() % 900) as u16;
    let addr = format!("127.0.0.1:{port}");
    let _guard = Kill(
        Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
            .args(["--db", dir.to_str().unwrap(), "--addr", &addr, "--no-auth"])
            .spawn()
            .unwrap(),
    );
    wait_up(&addr);
    let schema = concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"name\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range_value\":\"text\"}}\n",
    );
    assert_eq!(http(&addr, "POST", "/ingest", schema, None).0, 200);

    let open = |extra: &str| {
        let mut s = TcpStream::connect(&addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let req = format!("GET /events/stream HTTP/1.1\r\nHost: localhost\r\n{extra}\r\n");
        s.write_all(req.as_bytes()).unwrap();
        s
    };
    let mut s = open("");
    let mut buf = String::new();
    assert!(
        read_until(&mut s, &mut buf, "\r\n\r\n"),
        "no response head: {buf}"
    );
    let head = buf.to_ascii_lowercase();
    assert!(head.starts_with("http/1.1 200"), "{buf}");
    assert!(head.contains("content-type: text/event-stream"), "{buf}");
    assert!(head.contains("transfer-encoding: chunked"), "{buf}");
    assert!(
        read_until(&mut s, &mut buf, "\"changes\":[]"),
        "no opening event: {buf}"
    );
    assert!(buf.contains("retry: 2000"), "{buf}");

    let ingest = concat!(
        "{\"node\":{\"id\":1,\"type\":\"Person\"}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"name\",\"object\":{\"text\":\"Alice\"}}}\n",
    );
    let (st, _, body) = http(&addr, "POST", "/ingest", ingest, None);
    assert_eq!(st, 200, "{body}");
    assert!(
        read_until(&mut s, &mut buf, "\"predicates\":[\"name\"]"),
        "no change event: {buf}"
    );
    assert!(!buf.contains("Alice"), "values never ride the feed: {buf}");
    // every event is framed as one chunk: `<hex len>\r\n<event>\r\n`
    let ev = buf.rfind("id: ").unwrap();
    let size_line = buf[..ev - 2].rsplit("\r\n").next().unwrap();
    let len = usize::from_str_radix(size_line, 16).unwrap();
    assert!(buf[ev..ev + len].ends_with("\n\n"), "{buf}");
    drop(s);

    // resume after the schema-only head: the node write arrives again as the first event
    let v: serde_json::Value =
        serde_json::from_str(&http(&addr, "GET", "/stats", "", None).2).unwrap();
    let h = v["facts"]["durable_head"].as_u64().unwrap();
    let mut s = open(&format!("Last-Event-ID: {}\r\n", h - 2));
    let mut buf = String::new();
    assert!(
        read_until(&mut s, &mut buf, "\"predicates\":[\"name\"]"),
        "resume missed the change: {buf}"
    );
    let _ = std::fs::remove_dir_all(&base);
}

// Verdict events over a real connection: opt-in `verdicts=1`, the verdict event right after its
// batch's fact event, the configured heartbeat on an idle stream, and a rejected heartbeat value.
#[test]
fn serve_verdict_stream() {
    let base =
        std::env::temp_dir().join(format!("stroma_serve_verdict_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let dir = base.join("db");
    let port = 13100 + (std::process::id() % 900) as u16;
    let addr = format!("127.0.0.1:{port}");
    let _guard = Kill(
        Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
            .args([
                "--db",
                dir.to_str().unwrap(),
                "--addr",
                &addr,
                "--no-auth",
                "--sse-heartbeat",
                "1",
            ])
            .spawn()
            .unwrap(),
    );
    wait_up(&addr);
    let graph = concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Issue\"}}\n",
        "{\"pred_def\":{\"name\":\"status\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range_value\":\"text\"}}\n",
        "{\"pred_def\":{\"name\":\"owner\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"approved-by\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range\":\"Person\"}}\n",
        "{\"node\":{\"id\":10,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":1001,\"type\":\"Issue\"}}\n",
        "{\"fact\":{\"subject\":1001,\"predicate\":\"status\",\"object\":{\"text\":\"released\"}}}\n",
        "{\"fact\":{\"subject\":1001,\"predicate\":\"owner\",\"object\":{\"node\":10}}}\n",
        "{\"rule_def\":{\"name\":\"approval\",\"rule\":{\"subject_type\":\"Issue\",\"required\":{\"hops\":[{\"predicate\":\"owner\"}]},\"actual\":\"approved-by\",\"absent_when\":{\"predicate\":\"status\",\"equals\":\"released\"}}}}\n",
    );
    assert_eq!(http(&addr, "POST", "/ingest", graph, None).0, 200);
    let (st, _, body) = http(
        &addr,
        "POST",
        "/query",
        "{\"op\":\"conformance_watch\",\"rule_name\":\"approval\"}",
        None,
    );
    assert_eq!(st, 200, "{body}");

    let open = |path: &str| {
        let mut s = TcpStream::connect(&addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
        s.write_all(req.as_bytes()).unwrap();
        s
    };
    let mut s = open("/events/stream?verdicts=1");
    let mut buf = String::new();
    assert!(read_until(&mut s, &mut buf, "\"changes\":[]"), "{buf}");
    // idle: the configured one-second heartbeat arrives well inside the read timeout
    assert!(read_until(&mut s, &mut buf, ": keepalive"), "{buf}");

    let ingest =
        "{\"fact\":{\"subject\":1001,\"predicate\":\"approved-by\",\"object\":{\"node\":10}}}\n";
    assert_eq!(http(&addr, "POST", "/ingest", ingest, None).0, 200);
    assert!(read_until(&mut s, &mut buf, "event: verdict"), "{buf}");
    assert!(read_until(&mut s, &mut buf, "\"verdict\":\"OK\""), "{buf}");
    let verdict = buf.rfind("event: verdict\n").unwrap();
    let line = buf[verdict..]
        .lines()
        .find_map(|l| l.strip_prefix("data: "))
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(line).unwrap();
    assert_eq!(v["rule"], "approval");
    assert_eq!(v["subject"], 1001);
    assert_eq!(v["old"]["verdict"], "ABSENT");
    assert_eq!(v["new"]["verdict"], "OK");
    assert!(
        buf[..verdict].contains("\"predicates\":[\"approved-by\"]"),
        "the fact event comes first: {buf}"
    );
    drop(s);

    // without verdicts=1 the same write produces no verdict event
    let h = http(&addr, "GET", "/stats", "", None).2;
    let h: serde_json::Value = serde_json::from_str(&h).unwrap();
    let head = h["facts"]["durable_head"].as_u64().unwrap();
    let ingest =
        "{\"fact\":{\"subject\":1001,\"predicate\":\"approved-by\",\"object\":{\"node\":1001}}}\n";
    assert_eq!(http(&addr, "POST", "/ingest", ingest, None).0, 200);
    let mut s = open(&format!("/events/stream?since={head}"));
    let mut buf = String::new();
    assert!(
        read_until(&mut s, &mut buf, "\"predicates\":[\"approved-by\"]"),
        "{buf}"
    );
    assert!(!buf.contains("verdict"), "{buf}");
    drop(s);

    // a zero or non-numeric heartbeat is refused at startup
    for bad in ["0", "soon"] {
        let out = Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
            .args([
                "--db",
                base.join("bad").to_str().unwrap(),
                "--sse-heartbeat",
                bad,
            ])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2), "{bad}");
        assert!(String::from_utf8_lossy(&out.stderr).contains("--sse-heartbeat"));
    }
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn console_conformance_missing_and_assume() {
    let base = std::env::temp_dir().join(format!("stroma_serve_cf_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let dir = base.join("db");
    Db::init(&dir).unwrap();
    let db = Db::open(&dir).unwrap();
    db.ingest_str(concat!(
        "{\"type_def\":{\"name\":\"Person\"}}\n",
        "{\"type_def\":{\"name\":\"Department\"}}\n",
        "{\"type_def\":{\"name\":\"Issue\"}}\n",
        "{\"pred_def\":{\"name\":\"assigned-to\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"member-of\",\"cardinality\":\"one\",\"domain\":\"Person\",\"range\":\"Department\"}}\n",
        "{\"pred_def\":{\"name\":\"manager-of\",\"cardinality\":\"one\",\"domain\":\"Department\",\"range\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"approved-by\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range\":\"Person\"}}\n",
        "{\"pred_def\":{\"name\":\"approved-at\",\"cardinality\":\"one\",\"domain\":\"Issue\",\"range_value\":\"int\"}}\n",
        "{\"node\":{\"id\":1,\"type\":\"Department\"}}\n",
        "{\"node\":{\"id\":10,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":11,\"type\":\"Person\"}}\n",
        "{\"node\":{\"id\":1003,\"type\":\"Issue\"}}\n",
        "{\"fact\":{\"subject\":10,\"predicate\":\"member-of\",\"object\":{\"node\":1}}}\n",
        "{\"fact\":{\"subject\":1,\"predicate\":\"manager-of\",\"object\":{\"node\":11},\"valid_from\":1000}}\n",
        "{\"fact\":{\"subject\":1003,\"predicate\":\"assigned-to\",\"object\":{\"node\":10}}}\n",
        "{\"fact\":{\"subject\":1003,\"predicate\":\"approved-by\",\"object\":{\"node\":11}}}\n",
        "{\"rule_def\":{\"name\":\"release-approval\",\"rule\":{\"subject_type\":\"Issue\",\"required\":{\"hops\":[{\"predicate\":\"assigned-to\"},{\"predicate\":\"member-of\"},{\"predicate\":\"manager-of\",\"as_of\":\"approved-at\"}]},\"actual\":\"approved-by\"}}}\n",
    ))
    .unwrap();
    drop(db);

    let port = 12300 + (std::process::id() % 900) as u16;
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_stroma-serve"))
        .args(["--db", dir.to_str().unwrap(), "--addr", &addr])
        .spawn()
        .unwrap();
    let _guard = Kill(child);
    wait_up(&addr);
    let (st, cookie, _) = http(
        &addr,
        "POST",
        "/login",
        "{\"user\":\"admin\",\"password\":\"password\"}",
        None,
    );
    assert_eq!(st, 200, "login must succeed");
    let tok = cookie.expect("login must set a session cookie");

    // the console carries the rendering hooks for missing / assumed / no_rule
    let (st, _, ui) = http(&addr, "GET", "/", "", Some(&tok));
    assert_eq!(st, 200);
    for hook in [
        "id=\"qassumePred\"",
        "id=\"qassumeAt\"",
        "id=\"qcfsubj\"",
        "cfCannotJudge",
        "cfNoRule",
        "cf-assumed",
        "required_unresolved",
        "req.assume",
        "kind==='no_rule'",
    ] {
        assert!(ui.contains(hook), "console missing hook {hook}");
    }

    // without assume: the anchor is missing, so the row cannot be judged
    let (st, _, body) = http(
        &addr,
        "POST",
        "/query",
        "{\"op\":\"conformance\",\"rule_name\":\"release-approval\"}",
        Some(&tok),
    );
    assert_eq!(st, 200, "{body}");
    let r: serde_json::Value = serde_json::from_str(&body).unwrap();
    let row = &r["verdicts"][0];
    assert_eq!(row["subject"], 1003, "{body}");
    assert_eq!(row["missing"][0]["kind"], "anchor", "{body}");
    assert_eq!(row["missing"][0]["predicate"], "approved-at", "{body}");
    assert_eq!(row["assumed"], serde_json::json!([]), "{body}");

    // with assume, exactly as the console sends it: assumed lists the anchor
    let (st, _, body) = http(
        &addr,
        "POST",
        "/query",
        "{\"op\":\"conformance\",\"rule_name\":\"release-approval\",\"assume\":{\"approved-at\":7000}}",
        Some(&tok),
    );
    assert_eq!(st, 200, "{body}");
    let r: serde_json::Value = serde_json::from_str(&body).unwrap();
    let row = &r["verdicts"][0];
    assert_eq!(row["assumed"], serde_json::json!(["approved-at"]), "{body}");
    assert_eq!(row["missing"], serde_json::json!([]), "{body}");
    assert_eq!(row["verdict"], "OK", "{body}");

    // a subject no rule covers yields a top-level no_rule entry
    let (st, _, body) = http(
        &addr,
        "POST",
        "/query",
        "{\"op\":\"conformance\",\"subject\":10}",
        Some(&tok),
    );
    assert_eq!(st, 200, "{body}");
    let r: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(r["missing"][0]["kind"], "no_rule", "{body}");
    assert_eq!(r["missing"][0]["type"], "Person", "{body}");
    let _ = std::fs::remove_dir_all(&base);
}
