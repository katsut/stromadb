//! Shared MCP (Model Context Protocol) implementation: the tool schemas and the JSON-RPC 2.0
//! dispatch used by both the `stroma-mcp` stdio binary and the `stroma-serve` `POST /mcp` endpoint
//! (MCP streamable HTTP transport), so the two surfaces expose one identical tool set.
//!
//! Transport-agnostic: [`handle_message`] maps one incoming JSON-RPC message to at most one
//! response — a request (a message with an `id`) yields `Some(response)`, a notification yields
//! `None`. Framing (newline-delimited stdio, HTTP request/response) is the caller's concern.
//!
//! Tools: `schema`, `lookup` (exact-value key → node id), `point`, `expand`, `search` (authz-scoped hybrid), `retrieve_context`,
//! `conformance` (declared-rule per-subject verdicts), `rule` (read back a stored rule's
//! declaration), `stats`, `ingest`. Read tools map to
//! [`Db::query`]; `ingest` writes facts (serialized on the database's internal write mutex).

use serde_json::{Value, json};

use crate::Db;

/// The MCP protocol revision this server implements (returned by `initialize`).
pub const PROTOCOL_VERSION: &str = "2024-11-05";

fn tools() -> Value {
    json!([
        {
            "name": "schema",
            "description": "Discover what is queryable: the registered predicates (each with `card` one|many and its `domain`/`range`), the node labels in use, and the names of stored conformance rules (`rule` returns a declaration). Call this first to learn which predicate names exist and their cardinality before composing point/expand queries.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "lookup",
            "description": "Resolve an external key such as an issue key to node ids: the nodes whose one-cardinality `predicate` exactly equals `value` (current value, or the value in effect at `valid_at`). Call this first when you are given an identifier string instead of a node id, then pass the returned `id` to point/expand/timeline/conformance. Returns `{nodes:[{id, type, display}], truncated}`; an unknown key returns an empty `nodes` list.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "predicate": { "type": "string", "description": "one-cardinality predicate holding the key (e.g. \"issue-key\"); see `schema`" },
                    "value": { "description": "the exact value to match: a string for text, a number for int, or an object form such as {\"int\": 7} / {\"node\": N}" },
                    "type": { "type": "string", "description": "optional node type to restrict to (e.g. \"Issue\")" },
                    "limit": { "type": "integer", "default": 10, "description": "maximum nodes returned (at most 100); `truncated` reports more matches" },
                    "valid_at": { "type": "integer", "description": "as-of valid-time: match the value in effect at instant T instead of the current one" }
                },
                "required": ["predicate", "value"]
            }
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
            "name": "timeline",
            "description": "Answer \"over which intervals / when was\" instead of probing valid_at repeatedly: the full valid-time timeline of a value reached through a chain of one-cardinality predicates walked from a subject (one entry = that predicate's own history). Returns sorted, non-overlapping segments `{value, valid_from, valid_to}` (`valid_to: null` = still in effect); for any instant inside a segment, the equivalent point/valid_at composition returns that segment's value, and instants no segment covers read as absent. A non-empty answer also carries a weakest-link `confidence`: the minimum coarse tier over every history the walk read, with a `weakest` pointer naming the bottleneck hop.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "subject": { "type": "integer", "description": "subject node id the hop chain starts from" },
                    "hops": { "type": "array", "items": { "type": "string" }, "description": "chain of one-cardinality predicate names, walked left to right (e.g. [\"member-of\",\"manager-of\"] = the subject's manager over time)" },
                    "now": { "type": "integer", "description": "reference time for the confidence freshness signal (with max_age)" },
                    "max_age": { "type": "integer", "description": "age beyond which a support fact counts as stale for confidence" }
                },
                "required": ["subject", "hops"]
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
            "description": "Evaluate a declared conformance rule and return a deterministic verdict per subject: `OK` / `ABSENT` / `MISMATCH` / `NOT_APPLICABLE` (a `MISMATCH` carries a `kind` of `stale`|`wrong`; every `NOT_APPLICABLE` row, and only such a row, carries a `reason`). Pass either `rule` (an inline declaration) or `rule_name` (a rule stored earlier via a `rule_def` ingest line). Act on the verdicts instead of composing the multi-hop as-of check yourself. Rule shape: `{subject_type, scope?COND, required?{hops:[{predicate, as_of?}]} | cases?[{when?COND, required?{hops:[...]}}], distinct_from?{hops:[...]}, actual, absent_when?COND}` — `required` and `distinct_from` are derived paths of one-cardinality hops walked from each subject (the last hop optionally read as-of a valid-time instant given by the `as_of` predicate on the subject): the `actual` predicate must equal the `required` value and must NOT equal the `distinct_from` value (declare either or both; e.g. a self-approval ban is `distinct_from: {hops:[{predicate:\"assigned-to\"}]}`). Every hop but the last must reach a node; the last hop may read a literal value instead (e.g. `required: {hops:[{predicate:\"reports-to\"}, {predicate:\"name\", as_of:\"review-time\"}]}` with `actual: \"manager-name\"`), compared exactly as stored (equal text matches; an int never equals a float), with `stale`/`wrong` decided by that literal's history the same way. A rule with a many-cardinality hop, or a literal-valued hop before the last, is rejected with an error, since such a path can never resolve. `scope` restricts which subjects are in scope (others are `NOT_APPLICABLE` with `reason` `out_of_scope`); `absent_when` marks a missing `actual` as `ABSENT` rather than `OK`. A condition COND is `{predicate, as_of?, <test>}` on a one-cardinality value of the subject, where the test is either `equals` (ingest object forms `{\"node\": N}`, `{\"int\": ...}`, or a bare string for text) or a numeric range: `gt`|`gte` and/or `lt`|`lte`, or `between: [lo, hi]` (inclusive); ints and floats compare numerically. A condition's own `as_of` reads its value at the instant held by that integer predicate on the subject. A missing value, a non-numeric value under a range, or a missing as-of anchor never satisfies a condition (so `scope` gives `NOT_APPLICABLE` and `absent_when` does not fire). `cases` replaces `required` for banded rules: the first case whose `when` holds (no `when` = always) supplies the required path; no match = `NOT_APPLICABLE` with `reason` `no_matching_case`; each verdict reports the matched `case` index. E.g. `cases: [{when:{predicate:\"amount\", lte:500000, as_of:\"approved-at\"}, required:{hops:[...]}}, {when:{predicate:\"amount\", gt:500000, lte:2000000, as_of:\"approved-at\"}, required:{hops:[...]}}, {required:{hops:[...]}}]`. When the `actual` is present but the required path resolves to no value (a hop along the path has no value, or its as-of anchor is missing), the expected value is unknown: the row is `NOT_APPLICABLE` with `reason` `required_unresolved` and keeps its `actual`, `distinct`, `as_of` and `case` values. Treat it as missing data to fill in, not as a violation. A `distinct_from` collision is still `MISMATCH` in that case, and a missing `actual` is still `ABSENT` or `OK` as above. To decide on specific items, pass `subjects: [id, ..]` (ids from `lookup`): only those are evaluated and the answer holds exactly one row per distinct requested id, sorted by id. An id the rule does not judge is still answered, as `NOT_APPLICABLE` with a `reason`: `not_subject_type` when the node exists but its type is not the rule's `subject_type`, `unknown_subject` when no such node exists or it is not visible to you. Without `subjects` the answer is bounded: at most `limit` rows (default 50), `NOT_APPLICABLE` rows omitted unless `only` lists them, and `offset` pages further. The response carries `total` (rows matching `only`), `returned`, `truncated` (more rows after this page), `counts` per verdict, and `reasons` per `NOT_APPLICABLE` reason (`out_of_scope`, `no_matching_case`, `required_unresolved`, `not_subject_type`, `unknown_subject`), both over every row before `only` and paging. So `reasons.required_unresolved` shows how many subjects lack data even when their rows are omitted; fetch them with `only: [\"NOT_APPLICABLE\"]`.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "rule": { "type": "object", "description": "an inline rule declaration (see description for shape)" },
                    "rule_name": { "type": "string", "description": "the name of a rule stored via a `rule_def` ingest line (alternative to `rule`); the `rule` tool returns its declaration" },
                    "subjects": { "type": "array", "items": { "type": "integer" }, "description": "evaluate only these node ids (e.g. the id `lookup` returned); each distinct id gets exactly one row, and an id that is not a visible subject of the rule's type answers `NOT_APPLICABLE` with `reason` `not_subject_type` or `unknown_subject`" },
                    "only": { "type": "array", "items": { "type": "string", "enum": ["OK", "ABSENT", "MISMATCH", "NOT_APPLICABLE"] }, "description": "keep only these verdicts (default without `subjects`: OK, ABSENT, MISMATCH)" },
                    "limit": { "type": "integer", "default": 50, "description": "maximum rows returned (no default limit with `subjects`)" },
                    "offset": { "type": "integer", "default": 0, "description": "rows to skip, for paging with `limit`" }
                }
            }
        },
        {
            "name": "rule",
            "description": "Read back a stored conformance rule's declaration: `{name, rule}` with the rule JSON exactly as declared by its `rule_def` (subject_type, scope, required/distinct_from hops with their as_of anchors, actual, absent_when, or `cases`). How to read it: `required`/`distinct_from` are paths of one-cardinality hops walked from the subject (every hop but the last reaches a node, and the last may read a literal such as a name, which the `actual` must then equal as a value), a hop's `as_of` naming an integer predicate on the subject whose value is the valid-time instant that hop is read at. `cases: [{when?, required?}]` replaces `required` and is evaluated first-match: the first case whose `when` holds supplies the required path, a case without `when` always holds, and no match means `NOT_APPLICABLE` with `reason` `no_matching_case` (verdicts report the matched `case` index). A subject outside `scope` is `NOT_APPLICABLE` with `reason` `out_of_scope`, and one whose `actual` is present while the required path resolves to no value is `NOT_APPLICABLE` with `reason` `required_unresolved`, not a mismatch. A condition (`scope`, `absent_when`, `when`) is `{predicate, as_of?, test}` where the test is `equals` or a numeric range (`gt`|`gte` and/or `lt`|`lte`, or inclusive `between: [lo, hi]`; ints and floats compare numerically); a condition's own `as_of` reads its value at the instant held by that predicate on the subject instead of the current value. A missing value, a non-numeric value under a range, or a missing anchor leaves a condition unsatisfied. Use it to explain a `conformance` verdict or to see which facts a rule reads. Without `rule_name`, returns `{rules:[{name, rule}, ..]}` for every stored rule; `schema` lists the names only.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "rule_name": { "type": "string", "description": "the stored rule's name (as listed under `rules` by `schema`); omit to list every stored rule with its declaration" }
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

/// The caller's identity/visibility scope, resolved by the transport's auth layer (a named API
/// token, or full access for the stdio binary and the legacy single token). Applied inside the
/// dispatch so no tool can skip it.
#[derive(Clone, Debug, Default)]
pub struct Scope {
    /// Stamped as the provenance of writes whose lines carry no `source` (None = no stamping).
    pub default_source: Option<String>,
    /// ABAC label bitmask capping every read: a request's own `allowed_labels` is intersected
    /// with this, never widened. `None` = unrestricted.
    pub allowed_labels: Option<u64>,
    /// Writes (`ingest`) are rejected with a clear error.
    pub read_only: bool,
}

impl Scope {
    /// Intersect the request's `allowed_labels` (absent = all) with the scope's cap.
    pub fn cap_labels(&self, req: &mut Value) {
        if let Some(cap) = self.allowed_labels {
            let asked = req["allowed_labels"].as_u64().unwrap_or(u64::MAX);
            req["allowed_labels"] = json!(asked & cap);
        }
    }
}

/// Default page size of the MCP `conformance` tool when the caller gives no `limit`.
pub const MCP_CONFORMANCE_LIMIT: u64 = 50;

fn absent(req: &Value, key: &str) -> bool {
    req.get(key).is_none_or(Value::is_null)
}

/// The MCP `conformance` tool keeps answers small by default, unlike the HTTP op (which returns
/// every verdict for compatibility): for a full evaluation (no `subject`/`subjects`), `limit`
/// defaults to [`MCP_CONFORMANCE_LIMIT`] and `NOT_APPLICABLE` rows are omitted unless `only` says
/// otherwise — their number stays visible in `counts` and, per reason, in `reasons`. A subject-scoped call returns one row per
/// requested id as is, since the caller already bounded it.
fn apply_conformance_defaults(req: &mut Value) {
    if !absent(req, "subject") || !absent(req, "subjects") {
        return;
    }
    if absent(req, "limit") {
        req["limit"] = json!(MCP_CONFORMANCE_LIMIT);
    }
    if absent(req, "only") {
        req["only"] = json!(["OK", "ABSENT", "MISMATCH"]);
    }
}

fn call_tool(db: &Db, name: &str, args: &Value, scope: &Scope) -> Result<Value, String> {
    match name {
        "schema" | "lookup" | "point" | "expand" | "timeline" | "search" | "retrieve_context"
        | "conformance" | "rule" => {
            let mut req = args.clone();
            req["op"] = json!(name);
            if name == "conformance" {
                apply_conformance_defaults(&mut req);
            }
            scope.cap_labels(&mut req);
            db.query(&req)
        }
        "stats" => Ok(db.stats()),
        "ingest" => {
            if scope.read_only {
                return Err("this token is read-only: ingest is not allowed".into());
            }
            let jsonl = args["jsonl"]
                .as_str()
                .ok_or("ingest requires a `jsonl` string")?;
            let s = db.ingest_str_as(jsonl, scope.default_source.as_deref())?;
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

/// [`handle_message_scoped`] with an unrestricted scope — the stdio binary (one process, one
/// user) and any caller that authenticated with full access.
pub fn handle_message(db: &Db, msg: &Value) -> Option<Value> {
    handle_message_scoped(db, msg, &Scope::default())
}

/// Handle one JSON-RPC message under the caller's [`Scope`]; returns `Some(response)` for
/// requests, `None` for notifications.
pub fn handle_message_scoped(db: &Db, msg: &Value, scope: &Scope) -> Option<Value> {
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
                "instructions": "Call `schema` first to discover the predicates (name, cardinality, domain/range) and node labels. Every read tool takes a numeric node id: when you are given an external identifier instead (an issue key, an email, a document number), call `lookup` with the predicate that holds it (e.g. `issue-key`) to get the node id; never guess ids. Suggested order for a decision on a tracker item: (1) `schema`; (2) `lookup` the key to its id; (3) `point` / `expand` / `timeline` around that id for the facts you need; (4) `conformance` with `rule_name` and `subjects: [id]` for the declared verdict; (5) cite the verdict with its `required`, `actual` and `as_of` values (a `NOT_APPLICABLE` row says why in `reason`; `required_unresolved` means a fact on the required path is missing, not that the item violates the rule). Use `point` for one-cardinality predicates and `expand` for many-cardinality ones (both accept `valid_at` for an as-of read of the state in effect at that instant). There is no join operator: to evaluate a chained/derived relation, compose several calls — e.g. to read an attribute of a node reached via another predicate, point/expand the first predicate, then point the next predicate on each resulting node. To evaluate a declared rule (a required derived path, optionally read as-of a valid-time anchor, compared to an actual predicate) into per-subject verdicts instead of composing the hops yourself, call `conformance`; pass `subjects` for the items you are deciding, since a full evaluation is paged (`limit`, `offset`) and reports `counts`. For 'over which intervals / when was' questions, call `timeline` with a chain of one-cardinality predicates instead of probing `valid_at` repeatedly."
            }),
        ),
        "ping" => rpc_result(&id, json!({})),
        "tools/list" => rpc_result(&id, json!({ "tools": tools() })),
        "tools/call" => {
            let name = params["name"].as_str().unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            match call_tool(db, name, &args, scope) {
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
