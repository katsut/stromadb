//! CSV → ingest-JSONL compilation for `stroma import` — the CLI twin of the console's Import
//! panel. Deliberately mechanical: the user names the columns (id, name, edges, valid-time), the
//! compiler emits schema + node + fact lines for the existing ingest path. No inference beyond
//! column literal-type detection.
//!
//! Node ids are a deterministic 48-bit FNV-1a hash of `type \0 raw-id`, so re-importing the same
//! file yields the same ids (no-op suppression applies) and an edge column resolves its target by
//! hashing `(target type, cell value)` — two files that share key values line up without a join.
//! The console's importer uses the identical function, so both surfaces produce the same graph.

use std::collections::BTreeSet;

/// One column's role in the mapping. Everything not named is imported as a literal predicate.
#[derive(Clone, Debug, PartialEq)]
pub enum Role {
    /// The row key: hashed with the type into the node id. Exactly one.
    Id,
    /// `valid_from` for every fact this row asserts (a date/int column).
    ValidFrom,
    /// `valid_to` for every fact this row asserts (a date/int column; blank cell = open).
    ValidTo,
    /// A node-valued column: `predicate` edges to a node of `target_type` keyed by the cell value.
    Edge {
        target_type: String,
        predicate: String,
    },
    /// A literal predicate column (the default; the predicate name is the header).
    Literal,
    /// Ignore the column.
    Skip,
}

/// The full mapping for one CSV: the node type plus a role per header column.
pub struct Mapping {
    pub node_type: String,
    /// Role per column, same order as the header. Column names double as predicate names.
    pub roles: Vec<(String, Role)>,
    /// Optional provenance stamped on every fact line.
    pub source: Option<String>,
}

/// Deterministic node id: 48-bit FNV-1a over `type \0 raw`. 48 bits keeps ids inside JS's safe
/// integer range (the console importer computes the same hash in the browser).
pub fn node_id(node_type: &str, raw: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in node_type
        .as_bytes()
        .iter()
        .chain([0u8].iter())
        .chain(raw.as_bytes().iter())
    {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h & ((1 << 48) - 1)
}

/// Detected literal type of a column (drives the `pred_def` range and the fact value encoding).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ColType {
    Int,
    Float,
    Date,
    Text,
}

/// Parse `yyyy-mm-dd` / `yyyy/mm/dd`, with an optional ` hh:mm[:ss]`, as UTC epoch seconds.
/// Deterministic and dependency-free — days via a civil-date conversion, no timezone guessing.
pub fn parse_date(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date, time) = match s.split_once(' ') {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };
    let sep = if date.contains('/') { '/' } else { '-' };
    let mut it = date.split(sep);
    let (y, m, d) = (
        it.next()?.parse::<i64>().ok()?,
        it.next()?.parse::<u32>().ok()?,
        it.next()?.parse::<u32>().ok()?,
    );
    if it.next().is_some() || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    // days since epoch (civil-from-days inverse; Howard Hinnant's algorithm)
    let yy = if m <= 2 { y - 1 } else { y };
    let era = if yy >= 0 { yy } else { yy - 399 } / 400;
    let yoe = (yy - era * 400) as u64;
    let doy = ((153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5) + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe as i64 - 719_468;
    let mut secs = days * 86_400;
    if let Some(t) = time {
        let mut it = t.split(':');
        let h = it.next()?.parse::<i64>().ok()?;
        let mi = it.next()?.parse::<i64>().ok()?;
        let se = it
            .next()
            .map(|x| x.parse::<i64>().ok())
            .unwrap_or(Some(0))?;
        if !(0..24).contains(&h) || !(0..60).contains(&mi) || !(0..60).contains(&se) {
            return None;
        }
        secs += h * 3600 + mi * 60 + se;
    }
    Some(secs)
}

/// Detect a column's literal type from its non-empty cells: every cell int → Int, every cell
/// numeric → Float, every cell a date → Date, else Text. An empty column is Text.
pub fn detect_type(cells: &[&str]) -> ColType {
    let vals: Vec<&str> = cells
        .iter()
        .map(|c| c.trim())
        .filter(|c| !c.is_empty())
        .collect();
    if vals.is_empty() {
        return ColType::Text;
    }
    if vals.iter().all(|v| v.parse::<i64>().is_ok()) {
        return ColType::Int;
    }
    if vals.iter().all(|v| v.parse::<f64>().is_ok()) {
        return ColType::Float;
    }
    if vals.iter().all(|v| parse_date(v).is_some()) {
        return ColType::Date;
    }
    ColType::Text
}

fn esc_json(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into())
}

/// Compile parsed CSV rows under `mapping` into ingest JSONL (schema first, then nodes, then
/// facts). `rows` excludes the header; `headers` must match `mapping.roles` order. Errors are
/// readable and name the row/column that caused them.
pub fn compile(
    mapping: &Mapping,
    headers: &[String],
    rows: &[Vec<String>],
) -> Result<String, String> {
    if mapping.roles.len() != headers.len() {
        return Err(format!(
            "mapping covers {} columns but the file has {}",
            mapping.roles.len(),
            headers.len()
        ));
    }
    let id_cols: Vec<usize> = mapping
        .roles
        .iter()
        .enumerate()
        .filter(|(_, (_, r))| *r == Role::Id)
        .map(|(i, _)| i)
        .collect();
    let [id_col] = id_cols[..] else {
        return Err("exactly one column must have the id role".into());
    };
    let col = |role: &Role| {
        mapping
            .roles
            .iter()
            .position(|(_, r)| std::mem::discriminant(r) == std::mem::discriminant(role))
    };
    let vf_col = col(&Role::ValidFrom);
    let vt_col = col(&Role::ValidTo);

    // literal columns get their type detected across the whole file (a mixed column is Text)
    let lit_types: Vec<Option<ColType>> = mapping
        .roles
        .iter()
        .enumerate()
        .map(|(i, (_, r))| {
            (*r == Role::Literal).then(|| {
                detect_type(
                    &rows
                        .iter()
                        .map(|row| row.get(i).map(|s| s.as_str()).unwrap_or(""))
                        .collect::<Vec<_>>(),
                )
            })
        })
        .collect();

    let mut out = String::new();
    let mut types: BTreeSet<&str> = BTreeSet::new();
    types.insert(&mapping.node_type);
    for (_, r) in &mapping.roles {
        if let Role::Edge { target_type, .. } = r {
            types.insert(target_type);
        }
    }
    for t in &types {
        out.push_str(&format!("{{\"type_def\":{{\"name\":{}}}}}\n", esc_json(t)));
    }
    for (name, r) in &mapping.roles {
        match r {
            Role::Literal => {
                let ct = lit_types[mapping.roles.iter().position(|(n, _)| n == name).unwrap()]
                    .unwrap_or(ColType::Text);
                let range = match ct {
                    ColType::Int | ColType::Date => "int",
                    ColType::Float => "float",
                    ColType::Text => "text",
                };
                out.push_str(&format!(
                    "{{\"pred_def\":{{\"name\":{},\"cardinality\":\"one\",\"domain\":{},\"range_value\":\"{}\"{}}}}}\n",
                    esc_json(name),
                    esc_json(&mapping.node_type),
                    range,
                    if name == "name" || name == "title" { ",\"display\":true" } else { "" },
                ));
            }
            Role::Edge {
                target_type,
                predicate,
            } => {
                out.push_str(&format!(
                    "{{\"pred_def\":{{\"name\":{},\"cardinality\":\"one\",\"domain\":{},\"range\":{}}}}}\n",
                    esc_json(predicate),
                    esc_json(&mapping.node_type),
                    esc_json(target_type),
                ));
            }
            _ => {}
        }
    }

    // nodes first (subject + edge targets), then facts
    let mut node_lines: BTreeSet<String> = BTreeSet::new();
    let mut fact_lines = String::new();
    let src = mapping
        .source
        .as_ref()
        .map(|s| format!(",\"source\":{}", esc_json(s)))
        .unwrap_or_default();
    for (rn, row) in rows.iter().enumerate() {
        let cell = |i: usize| row.get(i).map(|s| s.trim()).unwrap_or("");
        let raw_id = cell(id_col);
        if raw_id.is_empty() {
            return Err(format!("row {}: empty id cell", rn + 2));
        }
        let subject = node_id(&mapping.node_type, raw_id);
        node_lines.insert(format!(
            "{{\"node\":{{\"id\":{},\"type\":{}}}}}",
            subject,
            esc_json(&mapping.node_type)
        ));
        let mut vt_json = String::new();
        if let Some(i) = vf_col {
            let v = cell(i);
            if !v.is_empty() {
                let ts = parse_date(v)
                    .or_else(|| v.parse::<i64>().ok())
                    .ok_or(format!("row {}: bad valid_from date {:?}", rn + 2, v))?;
                vt_json.push_str(&format!(",\"valid_from\":{ts}"));
            }
        }
        if let Some(i) = vt_col {
            let v = cell(i);
            if !v.is_empty() {
                let ts = parse_date(v)
                    .or_else(|| v.parse::<i64>().ok())
                    .ok_or(format!("row {}: bad valid_to date {:?}", rn + 2, v))?;
                vt_json.push_str(&format!(",\"valid_to\":{ts}"));
            }
        }
        for (i, (name, r)) in mapping.roles.iter().enumerate() {
            let v = cell(i);
            if v.is_empty() {
                continue;
            }
            match r {
                Role::Literal => {
                    let obj = match lit_types[i].unwrap_or(ColType::Text) {
                        ColType::Int => format!(
                            "{{\"int\":{}}}",
                            v.parse::<i64>()
                                .map_err(|_| format!("row {}: bad int in {name}: {v:?}", rn + 2))?
                        ),
                        ColType::Date => format!(
                            "{{\"int\":{}}}",
                            parse_date(v)
                                .ok_or(format!("row {}: bad date in {name}: {v:?}", rn + 2))?
                        ),
                        ColType::Float => format!(
                            "{{\"float\":{}}}",
                            v.parse::<f64>().map_err(|_| format!(
                                "row {}: bad number in {name}: {v:?}",
                                rn + 2
                            ))?
                        ),
                        ColType::Text => format!("{{\"text\":{}}}", esc_json(v)),
                    };
                    fact_lines.push_str(&format!(
                        "{{\"fact\":{{\"subject\":{},\"predicate\":{},\"object\":{}{}{}}}}}\n",
                        subject,
                        esc_json(name),
                        obj,
                        vt_json,
                        src
                    ));
                }
                Role::Edge {
                    target_type,
                    predicate,
                } => {
                    let target = node_id(target_type, v);
                    node_lines.insert(format!(
                        "{{\"node\":{{\"id\":{},\"type\":{}}}}}",
                        target,
                        esc_json(target_type)
                    ));
                    fact_lines.push_str(&format!(
                        "{{\"fact\":{{\"subject\":{},\"predicate\":{},\"object\":{{\"node\":{}}}{}{}}}}}\n",
                        subject, esc_json(predicate), target, vt_json, src
                    ));
                }
                _ => {}
            }
        }
    }
    for n in node_lines {
        out.push_str(&n);
        out.push('\n');
    }
    out.push_str(&fact_lines);
    Ok(out)
}

/// Parse CSV text (RFC 4180 quoting: quoted fields, doubled quotes, embedded commas/newlines).
/// Strips a UTF-8 BOM; rejects non-UTF-8 input upstream (the caller reads the file as UTF-8 and
/// reports a readable error suggesting conversion).
pub fn parse_csv(text: &str) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut row: Vec<String> = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            match c {
                '"' if chars.peek() == Some(&'"') => {
                    chars.next();
                    field.push('"');
                }
                '"' => in_quotes = false,
                _ => field.push(c),
            }
        } else {
            match c {
                '"' => in_quotes = true,
                ',' => row.push(std::mem::take(&mut field)),
                '\r' => {}
                '\n' => {
                    row.push(std::mem::take(&mut field));
                    if row.iter().any(|f| !f.is_empty()) {
                        rows.push(std::mem::take(&mut row));
                    } else {
                        row.clear();
                    }
                }
                _ => field.push(c),
            }
        }
    }
    if in_quotes {
        return Err("unterminated quoted field".into());
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        if row.iter().any(|f| !f.is_empty()) {
            rows.push(row);
        }
    }
    let mut it = rows.into_iter();
    let headers = it.next().ok_or("empty file")?;
    Ok((
        headers.into_iter().map(|h| h.trim().to_string()).collect(),
        it.collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_ids_are_deterministic_and_type_scoped() {
        assert_eq!(node_id("Person", "42"), node_id("Person", "42"));
        assert_ne!(node_id("Person", "42"), node_id("Department", "42"));
        assert!(node_id("Person", "42") < (1 << 48));
    }

    // Pinned against the console importer's BigInt implementation — the two surfaces must keep
    // producing identical ids or a CLI import and a console import stop lining up in one graph.
    #[test]
    fn node_ids_match_the_console_implementation() {
        assert_eq!(node_id("Person", "42"), 94607205715242);
        assert_eq!(node_id("Department", "eng"), 238290147953783);
        assert_eq!(node_id("人", "α"), 2421719834668);
    }

    #[test]
    fn dates_parse_and_reject() {
        assert_eq!(parse_date("2024-01-01"), Some(1704067200));
        assert_eq!(parse_date("2024/07/01"), Some(1719792000));
        assert_eq!(
            parse_date("2024-09-01 12:30"),
            Some(1725148800 + 12 * 3600 + 1800)
        );
        assert_eq!(parse_date("not-a-date"), None);
        assert_eq!(parse_date("2024-13-01"), None);
    }

    #[test]
    fn csv_parses_quotes_and_bom() {
        let (h, rows) = parse_csv("\u{feff}a,b\n1,\"x, \"\"y\"\"\nz\"\n").unwrap();
        assert_eq!(h, vec!["a", "b"]);
        assert_eq!(rows, vec![vec!["1".to_string(), "x, \"y\"\nz".to_string()]]);
    }

    #[test]
    fn compile_emits_schema_nodes_facts_with_valid_time() {
        let mapping = Mapping {
            node_type: "Person".into(),
            roles: vec![
                ("id".into(), Role::Id),
                ("name".into(), Role::Literal),
                (
                    "dept".into(),
                    Role::Edge {
                        target_type: "Department".into(),
                        predicate: "member-of".into(),
                    },
                ),
                ("hired".into(), Role::ValidFrom),
                ("left".into(), Role::ValidTo),
            ],
            source: Some("hr".into()),
        };
        let headers: Vec<String> = ["id", "name", "dept", "hired", "left"]
            .map(String::from)
            .into();
        let rows = vec![
            vec![
                "1".into(),
                "Alice".into(),
                "eng".into(),
                "2024-01-01".into(),
                "".into(),
            ],
            vec![
                "2".into(),
                "Bob".into(),
                "eng".into(),
                "2024-01-01".into(),
                "2025-01-01".into(),
            ],
        ];
        let jsonl = compile(&mapping, &headers, &rows).unwrap();
        assert!(jsonl.contains("\"type_def\":{\"name\":\"Person\"}"));
        assert!(jsonl.contains("\"type_def\":{\"name\":\"Department\"}"));
        assert!(jsonl.contains("\"display\":true")); // name column
        let alice = node_id("Person", "1");
        let eng = node_id("Department", "eng");
        assert!(jsonl.contains(&format!("\"subject\":{alice},\"predicate\":\"member-of\",\"object\":{{\"node\":{eng}}},\"valid_from\":1704067200,\"source\":\"hr\"")));
        assert!(jsonl.contains("\"valid_to\":1735689600"));
        // open end: Alice's row has no valid_to on its facts
        assert!(!jsonl.contains(&format!("\"subject\":{alice},\"predicate\":\"member-of\",\"object\":{{\"node\":{eng}}},\"valid_from\":1704067200,\"valid_to\"")));
    }

    #[test]
    fn compile_errors_are_readable() {
        let mapping = Mapping {
            node_type: "P".into(),
            roles: vec![("id".into(), Role::Id), ("d".into(), Role::ValidFrom)],
            source: None,
        };
        let headers: Vec<String> = ["id", "d"].map(String::from).into();
        let rows = vec![vec!["1".into(), "banana".into()]];
        let e = compile(&mapping, &headers, &rows).unwrap_err();
        assert!(e.contains("row 2") && e.contains("valid_from"), "{e}");
    }
}
