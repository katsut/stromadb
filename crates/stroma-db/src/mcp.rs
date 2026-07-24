//! Shared MCP (Model Context Protocol) implementation: the tool schemas and the JSON-RPC 2.0
//! dispatch used by both the `stroma-mcp` stdio binary and the `stroma-serve` `POST /mcp` endpoint
//! (MCP streamable HTTP transport), so the two surfaces expose one identical tool set.
//!
//! Transport-agnostic: [`handle_message`] maps one incoming JSON-RPC message to at most one
//! response — a request (a message with an `id`) yields `Some(response)`, a notification yields
//! `None`. Framing (newline-delimited stdio, HTTP request/response) is the caller's concern.
//!
//! Tools: `schema`, `point`, `expand`, `search` (authz-scoped hybrid), `retrieve_context`,
//! `conformance` (declared-rule per-subject verdicts), `stats`, `ingest`. Read tools map to
//! [`Db::query`]; `ingest` writes facts (serialized on the database's internal write mutex).

use serde_json::{Value, json};

use crate::Db;

/// The MCP protocol revision this server implements (returned by `initialize`).
pub const PROTOCOL_VERSION: &str = "2024-11-05";

fn tools() -> Value {
    json!([
        {
            "name": "schema",
            "description": "Discover what is queryable: the registered predicates (each with `card` one|many and its `domain`/`range`) and the node labels in use. Call this first to learn which predicate names exist and their cardinality before composing point/expand queries.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "point",
            "description": "Look up the value(s) of a (subject, predicate) fact. Returns {one:..} for cardinality-one predicates or {many:[..]} for cardinality-many.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "subject": { "type": "integer", "description": "subject node id" },
                    "predicate": { "type": "string", "description": "predicate name" },
                    "valid_at": { "type": "integer", "description": "as-of valid-time: the value (one-cardinality) or element set (many-cardinality) in effect at instant T" }
                },
                "required": ["subject", "predicate"]
            }
        },
        {
            "name": "expand",
            "description": "1-hop expand: node ids reachable from a subject via a predicate.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "subject": { "type": "integer" },
                    "predicate": { "type": "string" },
                    "valid_at": { "type": "integer", "description": "as-of valid-time: expand over the edges in effect at instant T" }
                },
                "required": ["subject", "predicate"]
            }
        },
        {
            "name": "search",
            "description": "Type-aware hybrid search: k nearest nodes of a type to a query vector, authz-scoped, optionally 1-hop expanded. Returns ids + scores + as_of.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "type": { "type": "string", "description": "target node type name" },
                    "vector": { "type": "array", "items": { "type": "number" }, "description": "query embedding" },
                    "k": { "type": "integer", "default": 10 },
                    "allowed_labels": { "type": "integer", "description": "caller ABAC label bitmask (default: all)" },
                    "expand": { "type": "string", "description": "optional predicate to 1-hop expand results" },
                    "mode": { "type": "string", "enum": ["fresh", "strict"], "default": "fresh" }
                },
                "required": ["type", "vector"]
            }
        },
        {
            "name": "retrieve_context",
            "description": "Assemble LLM-ready context from a hybrid search: each hit's current value of a `content` predicate with a calendar-framed timestamp of its `date` predicate (weekday, days relative to `as_of`, business hours), ordered oldest→newest. Returns a ready-to-inject context block + structured hits.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "type": { "type": "string", "description": "target node type name" },
                    "vector": { "type": "array", "items": { "type": "number" }, "description": "query embedding" },
                    "content": { "type": "string", "description": "predicate whose text value is the excerpt" },
                    "date": { "type": "string", "description": "predicate whose Int value (epoch seconds) is the valid-time to stamp" },
                    "k": { "type": "integer", "default": 10 },
                    "allowed_labels": { "type": "integer", "description": "caller ABAC label bitmask (default: all)" },
                    "as_of": { "type": "integer", "description": "reference instant (epoch seconds) for relative-day stamping; default = newest hit" },
                    "tz_offset_min": { "type": "integer", "description": "calendar frame: minutes offset from UTC (default 0)" }
                },
                "required": ["type", "vector", "content"]
            }
        },
        {
            "name": "conformance",
            "description": "Evaluate a declared conformance rule and return a deterministic verdict per subject: `OK` / `ABSENT` / `MISMATCH` / `NOT_APPLICABLE` (a `MISMATCH` carries a `kind` of `stale`|`wrong`). Pass either `rule` (an inline declaration) or `rule_name` (a rule stored earlier via a `rule_def` ingest line). Act on the verdicts instead of composing the multi-hop as-of check yourself. Rule shape: `{subject_type, scope?{predicate,equals}, required?{hops:[{predicate, as_of?}]}, distinct_from?{hops:[...]}, actual, absent_when?{predicate,equals}}` — `required` and `distinct_from` are derived paths of one-cardinality hops walked from each subject (the last hop optionally read as-of a valid-time instant given by the `as_of` predicate on the subject): the `actual` predicate must equal the `required` value and must NOT equal the `distinct_from` value (declare either or both; e.g. a self-approval ban is `distinct_from: {hops:[{predicate:\"assigned-to\"}]}`). `scope` restricts which subjects are in scope (others are `NOT_APPLICABLE`); `absent_when` marks a missing `actual` as `ABSENT` rather than `OK`. Condition `equals` values take the ingest object forms (`{\"node\": N}`, `{\"int\": ...}`) or a bare string for text.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "rule": { "type": "object", "description": "an inline rule declaration (see description for shape)" },
                    "rule_name": { "type": "string", "description": "the name of a rule stored via a `rule_def` ingest line (alternative to `rule`)" }
                }
            }
        },
        {
            "name": "stats",
            "description": "Database counters: durable head, schema/embedding counts, storage bytes.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "ingest",
            "description": "Ingest a JSONL batch (type_def / pred_def / node / fact / retract / close records, one per line). Durable on return.",
            "inputSchema": {
                "type": "object",
                "properties": { "jsonl": { "type": "string", "description": "newline-delimited records" } },
                "required": ["jsonl"]
            }
        }
    ])
}

fn call_tool(db: &Db, name: &str, args: &Value) -> Result<Value, String> {
    match name {
        "schema" | "point" | "expand" | "search" | "retrieve_context" | "conformance" => {
            let mut req = args.clone();
            req["op"] = json!(name);
            db.query(&req)
        }
        "stats" => Ok(db.stats()),
        "ingest" => {
            let jsonl = args["jsonl"]
                .as_str()
                .ok_or("ingest requires a `jsonl` string")?;
            let s = db.ingest_str(jsonl)?;
            Ok(
                json!({ "defs": s.defs, "nodes": s.nodes, "facts": s.facts, "retracts": s.retracts, "closes": s.closes, "durable_head": s.durable_head }),
            )
        }
        other => Err(format!("unknown tool: {other}")),
    }
}

/// JSON-RPC error object.
pub fn rpc_error(id: &Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn rpc_result(id: &Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// Handle one JSON-RPC message; returns `Some(response)` for requests, `None` for notifications.
pub fn handle_message(db: &Db, msg: &Value) -> Option<Value> {
    let method = msg["method"].as_str().unwrap_or("");
    // Notifications have no id and expect no response (`?` returns None here).
    let id = msg.get("id").cloned()?;
    let params = msg.get("params").cloned().unwrap_or(json!({}));

    let resp = match method {
        "initialize" => rpc_result(
            &id,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "stroma-mcp", "version": env!("CARGO_PKG_VERSION") },
                "instructions": "Call `schema` first to discover the predicates (name, cardinality, domain/range) and node labels. Use `point` for one-cardinality predicates and `expand` for many-cardinality ones (both accept `valid_at` for an as-of read of the state in effect at that instant). There is no join operator: to evaluate a chained/derived relation, compose several calls — e.g. to read an attribute of a node reached via another predicate, point/expand the first predicate, then point the next predicate on each resulting node. To evaluate a declared rule (a required derived path, optionally read as-of a valid-time anchor, compared to an actual predicate) into per-subject verdicts instead of composing the hops yourself, call `conformance`."
            }),
        ),
        "ping" => rpc_result(&id, json!({})),
        "tools/list" => rpc_result(&id, json!({ "tools": tools() })),
        "tools/call" => {
            let name = params["name"].as_str().unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            match call_tool(db, name, &args) {
                Ok(v) => rpc_result(
                    &id,
                    json!({ "content": [{ "type": "text", "text": v.to_string() }] }),
                ),
                // Tool-level failures are reported in the result (isError), not as protocol errors.
                Err(e) => rpc_result(
                    &id,
                    json!({ "content": [{ "type": "text", "text": format!("error: {e}") }], "isError": true }),
                ),
            }
        }
        other => rpc_error(&id, -32601, &format!("method not found: {other}")),
    };
    Some(resp)
}
