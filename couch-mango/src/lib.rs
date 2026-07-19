//! Native Mango selector evaluation — a Rust port of CouchDB's
//! `mango_selector.erl` (normalization, negation pushing, matching),
//! `mango_doc:get_field/2` (dotted-path traversal with array indexing) and
//! `couch_ejson_compare` type-ordered collation.
//!
//! Known divergences from the server, all documented in the README:
//! - String ordering for `$lt`/`$gt`-style range operators is Unicode
//!   codepoint order, not ICU collation. Equality is unaffected.
//! - `$regex` uses Rust regex syntax rather than PCRE; patterns needing
//!   backreferences or lookaround are rejected at compile time instead of
//!   silently failing per document.

use serde_json::Value;
use std::borrow::Cow;
use std::cmp::Ordering;

/// A document the selector can be evaluated against.
///
/// The matcher never materializes a whole document to JSON; it only asks for
/// the fields the selector references. JSON documents resolve paths directly
/// (borrowing); a proto-backed document navigates the message and honors proto3
/// semantics — an unset scalar reads as its default value (no scalar presence),
/// so `{"field": ""}` matches a message whose empty field is absent on the wire.
pub trait Doc {
    /// Resolve a dotted path (already split into segments). Returns `None` when
    /// the path is genuinely absent (an unknown field, or an unset message
    /// field); a present-or-defaulted value is returned as `Some`.
    fn get_path(&self, path: &[String]) -> Option<Cow<'_, Value>>;
}

/// JSON documents resolve paths by direct tree traversal (CouchDB semantics:
/// an absent key is truly missing, matching only `{$exists: false}`).
impl Doc for Value {
    fn get_path(&self, path: &[String]) -> Option<Cow<'_, Value>> {
        get_path(self, path).map(Cow::Borrowed)
    }
}

/// A compiled selector, ready for repeated matching.
pub struct Selector {
    root: Node,
}

#[derive(Debug)]
enum Node {
    True,
    And(Vec<Node>),
    Or(Vec<Node>),
    Nor(Vec<Node>),
    Not(Box<Node>),
    Field { path: Vec<String>, cond: Cond },
}

#[derive(Debug)]
enum Cond {
    Lt(Value),
    Lte(Value),
    Eq(Value),
    Ne(Value),
    Gte(Value),
    Gt(Value),
    In(Vec<Value>),
    Nin(Vec<Value>),
    Exists(bool),
    Type(String),
    Mod(i64, i64),
    Regex(regex::Regex),
    BeginsWith(String),
    All(Vec<Value>),
    Size(usize),
    ElemMatch(Box<Node>),
    AllMatch(Box<Node>),
    KeyMapMatch(Box<Node>),
    Not(Box<Cond>),
}

impl Selector {
    pub fn compile(selector: &Value) -> Result<Selector, String> {
        let root = match selector {
            // An empty selector matches any document.
            Value::Object(m) if m.is_empty() => Node::True,
            Value::Object(_) => {
                let node = push_negations(parse(selector, &[])?);
                if let Node::Field { path, .. } = &node {
                    if path.is_empty() {
                        return Err(
                            "operator or bare value where a field name is expected".into()
                        );
                    }
                }
                node
            }
            _ => return Err("selector must be a JSON object".into()),
        };
        Ok(Selector { root })
    }

    pub fn matches(&self, doc: &dyn Doc) -> bool {
        match_node(&self.root, doc)
    }
}

// ---- normalization (norm_ops + norm_fields in one pass) --------------------

/// Parse a selector-position value into a Node, threading the field path down
/// to the terminals exactly like mango's `norm_fields`. Bare values mean `$eq`.
fn parse(v: &Value, path: &[String]) -> Result<Node, String> {
    let obj = match v {
        Value::Object(m) => m,
        // A bare value condition means equality.
        other => {
            return Ok(Node::Field {
                path: path.to_vec(),
                cond: Cond::Eq(other.clone()),
            })
        }
    };
    match obj.len() {
        // {"a": {}} means the field equals the empty object exactly.
        0 => Ok(Node::Field {
            path: path.to_vec(),
            cond: Cond::Eq(Value::Object(Default::default())),
        }),
        1 => {
            let (k, val) = obj.iter().next().unwrap();
            parse_pair(k, val, path)
        }
        // Multiple keys are an implicit $and over single-key selectors.
        _ => {
            let mut parts = Vec::with_capacity(obj.len());
            for (k, val) in obj {
                parts.push(parse_pair(k, val, path)?);
            }
            Ok(Node::And(parts))
        }
    }
}

fn parse_pair(key: &str, val: &Value, path: &[String]) -> Result<Node, String> {
    let arg_list = |op: &str| -> Result<Vec<Value>, String> {
        val.as_array()
            .map(|a| a.to_vec())
            .ok_or_else(|| format!("{op} requires an array argument"))
    };
    match key {
        "$and" | "$or" | "$nor" => {
            let mut children = Vec::new();
            for child in val
                .as_array()
                .ok_or_else(|| format!("{key} requires an array argument"))?
            {
                children.push(parse(child, path)?);
            }
            Ok(match key {
                "$and" => Node::And(children),
                "$or" => Node::Or(children),
                _ => Node::Nor(children),
            })
        }
        "$not" => {
            if !val.is_object() {
                return Err("$not requires an object argument".into());
            }
            Ok(Node::Not(Box::new(parse(val, path)?)))
        }
        "$elemMatch" | "$allMatch" | "$keyMapMatch" => {
            if !val.is_object() {
                return Err(format!("{key} requires an object argument"));
            }
            // The inner selector's field paths are relative to the element.
            let inner = Box::new(parse(val, &[])?);
            let cond = match key {
                "$elemMatch" => Cond::ElemMatch(inner),
                "$allMatch" => Cond::AllMatch(inner),
                _ => Cond::KeyMapMatch(inner),
            };
            Ok(Node::Field {
                path: path.to_vec(),
                cond,
            })
        }
        _ if key.starts_with('$') => {
            let cond = match key {
                "$lt" => Cond::Lt(val.clone()),
                "$lte" => Cond::Lte(val.clone()),
                "$eq" => Cond::Eq(val.clone()),
                "$ne" => Cond::Ne(val.clone()),
                "$gte" => Cond::Gte(val.clone()),
                "$gt" => Cond::Gt(val.clone()),
                "$in" => Cond::In(arg_list("$in")?),
                "$nin" => Cond::Nin(arg_list("$nin")?),
                "$all" => Cond::All(arg_list("$all")?),
                "$exists" => Cond::Exists(
                    val.as_bool()
                        .ok_or("$exists requires a boolean argument")?,
                ),
                "$type" => Cond::Type(
                    val.as_str()
                        .ok_or("$type requires a string argument")?
                        .to_string(),
                ),
                "$size" => Cond::Size(
                    val.as_u64()
                        .ok_or("$size requires a non-negative integer")?
                        as usize,
                ),
                "$mod" => {
                    let pair = val
                        .as_array()
                        .filter(|a| a.len() == 2)
                        .and_then(|a| Some((a[0].as_i64()?, a[1].as_i64()?)))
                        .ok_or("$mod requires [Divisor, Remainder] integers")?;
                    Cond::Mod(pair.0, pair.1)
                }
                "$regex" => {
                    let pat = val.as_str().ok_or("$regex requires a string argument")?;
                    Cond::Regex(
                        regex::Regex::new(pat)
                            .map_err(|e| format!("invalid $regex: {e}"))?,
                    )
                }
                "$beginsWith" => Cond::BeginsWith(
                    val.as_str()
                        .ok_or("$beginsWith requires a string argument")?
                        .to_string(),
                ),
                "$text" | "$default" => {
                    return Err("$text requires a server-side text index; \
                                not supported by couch-repl"
                        .into())
                }
                "$where" | "$geoWithin" | "$geoIntersects" | "$near" | "$nearSphere" => {
                    return Err(format!("{key} is not supported"))
                }
                other => return Err(format!("invalid operator: {other}")),
            };
            Ok(Node::Field {
                path: path.to_vec(),
                cond,
            })
        }
        field => {
            let mut new_path = path.to_vec();
            new_path.extend(parse_field(field)?);
            parse(val, &new_path)
        }
    }
}

/// Split a dotted field name into path segments; `\.` escapes a literal dot
/// and backslashes are stripped afterwards (mango_util:parse_field/1).
fn parse_field(field: &str) -> Result<Vec<String>, String> {
    let mut segs = Vec::new();
    let mut cur = String::new();
    let mut prev_backslash = false;
    for ch in field.chars() {
        if ch == '.' && !prev_backslash {
            segs.push(std::mem::take(&mut cur));
        } else if ch != '\\' {
            cur.push(ch);
        }
        prev_backslash = ch == '\\' && !prev_backslash;
    }
    segs.push(cur);
    if segs.iter().any(|s| s.is_empty()) {
        return Err(format!("invalid field name: {field}"));
    }
    Ok(segs)
}

// ---- negation pushing (norm_negations + negate) -----------------------------

/// Push `$not`/`$nor` down to the terminals through the boolean operators,
/// applying De Morgan's laws. Field-level conditions (including `$elemMatch`
/// bodies) are left untouched, same as the Erlang implementation.
fn push_negations(n: Node) -> Node {
    match n {
        Node::Not(inner) => negate(*inner),
        Node::Nor(cs) => Node::And(cs.into_iter().map(negate).collect()),
        Node::And(cs) => Node::And(cs.into_iter().map(push_negations).collect()),
        Node::Or(cs) => Node::Or(cs.into_iter().map(push_negations).collect()),
        other => other,
    }
}

fn negate(n: Node) -> Node {
    match n {
        Node::Not(inner) => push_negations(*inner),
        Node::Nor(cs) => Node::Or(cs.into_iter().map(push_negations).collect()),
        Node::And(cs) => Node::Or(cs.into_iter().map(negate).collect()),
        Node::Or(cs) => Node::And(cs.into_iter().map(negate).collect()),
        Node::Field { path, cond } => Node::Field {
            path,
            cond: negate_cond(cond),
        },
        Node::True => Node::Not(Box::new(Node::True)),
    }
}

fn negate_cond(c: Cond) -> Cond {
    match c {
        Cond::Lt(v) => Cond::Gte(v),
        Cond::Lte(v) => Cond::Gt(v),
        Cond::Eq(v) => Cond::Ne(v),
        Cond::Ne(v) => Cond::Eq(v),
        Cond::Gte(v) => Cond::Lt(v),
        Cond::Gt(v) => Cond::Lte(v),
        Cond::In(a) => Cond::Nin(a),
        Cond::Nin(a) => Cond::In(a),
        Cond::Exists(b) => Cond::Exists(!b),
        other => Cond::Not(Box::new(other)),
    }
}

// ---- matching ---------------------------------------------------------------

fn match_node(n: &Node, doc: &dyn Doc) -> bool {
    match n {
        Node::True => true,
        // Empty argument lists: $and, $or (and thus $nor) are vacuously true.
        Node::And(cs) => cs.iter().all(|c| match_node(c, doc)),
        Node::Or(cs) => cs.is_empty() || cs.iter().any(|c| match_node(c, doc)),
        Node::Nor(cs) => cs.is_empty() || !cs.iter().any(|c| match_node(c, doc)),
        Node::Not(c) => !match_node(c, doc),
        Node::Field { path, cond } => match doc.get_path(path) {
            // A missing field only ever matches a literal {$exists: false}.
            None => matches!(cond, Cond::Exists(false)),
            Some(sub) => {
                // _id comparisons use raw byte order, not collation.
                let raw = path.len() == 1 && path[0] == "_id";
                match_cond(cond, &sub, raw)
            }
        },
    }
}

fn match_cond(c: &Cond, v: &Value, raw: bool) -> bool {
    match c {
        Cond::Lt(a) => cmp(v, a, raw) == Ordering::Less,
        Cond::Lte(a) => cmp(v, a, raw) != Ordering::Greater,
        Cond::Eq(a) => cmp(v, a, raw) == Ordering::Equal,
        Cond::Ne(a) => cmp(v, a, raw) != Ordering::Equal,
        Cond::Gte(a) => cmp(v, a, raw) != Ordering::Less,
        Cond::Gt(a) => cmp(v, a, raw) == Ordering::Greater,
        // $in against an array field matches if any *element* equals any arg.
        Cond::In(args) => match v.as_array() {
            Some(elems) => args
                .iter()
                .any(|a| elems.iter().any(|e| cmp(e, a, raw) == Ordering::Equal)),
            None => args.iter().any(|a| cmp(v, a, raw) == Ordering::Equal),
        },
        Cond::Nin(args) => match v.as_array() {
            Some(elems) => !args
                .iter()
                .any(|a| elems.iter().any(|e| cmp(e, a, raw) == Ordering::Equal)),
            None => args.iter().all(|a| cmp(v, a, raw) != Ordering::Equal),
        },
        // Reached only when the field exists.
        Cond::Exists(should) => *should,
        Cond::Type(t) => type_of(v) == t,
        Cond::Mod(d, r) => match (v.as_i64(), v.is_f64()) {
            (Some(n), false) => *d != 0 && n % *d == *r,
            _ => false,
        },
        Cond::Regex(re) => v.as_str().map(|s| re.is_match(s)).unwrap_or(false),
        Cond::BeginsWith(p) => v.as_str().map(|s| s.starts_with(p.as_str())).unwrap_or(false),
        Cond::All(args) => match v.as_array() {
            Some(elems) => {
                if args.is_empty() {
                    return false;
                }
                let has_args = args.iter().all(|a| elems.contains(a));
                // A single array argument may also equal the whole field.
                let is_args = args.len() == 1
                    && args[0].as_array().map(|a| a == elems).unwrap_or(false);
                has_args || is_args
            }
            None => false,
        },
        Cond::Size(n) => v.as_array().map(|a| a.len() == *n).unwrap_or(false),
        Cond::ElemMatch(sel) => v
            .as_array()
            .map(|els| els.iter().any(|e| match_node(sel, e)))
            .unwrap_or(false),
        // $allMatch is false for empty arrays and non-arrays.
        Cond::AllMatch(sel) => v
            .as_array()
            .map(|els| !els.is_empty() && els.iter().all(|e| match_node(sel, e)))
            .unwrap_or(false),
        Cond::KeyMapMatch(sel) => v
            .as_object()
            .map(|m| {
                m.keys()
                    .any(|k| match_node(sel, &Value::String(k.clone())))
            })
            .unwrap_or(false),
        Cond::Not(inner) => !match_cond(inner, v, raw),
    }
}

/// mango_doc:get_field/2 — walk a path of object keys; a numeric segment
/// indexes into arrays. Missing keys and type mismatches both fail the match.
fn get_path<'a>(doc: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut cur = doc;
    for seg in path {
        cur = match cur {
            Value::Object(m) => m.get(seg)?,
            Value::Array(a) => a.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn type_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// couch_ejson_compare ordering, public for consumers that need to sort
/// EJSON values the way CouchDB does (e.g. index keys in couch-index).
pub fn collate(a: &Value, b: &Value) -> Ordering {
    cmp(a, b, false)
}

/// Extract a dotted-path field from a document (mango_doc:get_field) —
/// public so couch-index can build index keys with identical semantics.
pub fn get_field<'a>(doc: &'a Value, field: &str) -> Option<&'a Value> {
    let path = parse_field(field).ok()?;
    get_path(doc, &path)
}

/// couch_ejson_compare ordering: null < false < true < number < string <
/// array < object. Numbers compare numerically across int/float. Strings
/// compare by codepoint (the server uses ICU collation; see module docs).
fn cmp(a: &Value, b: &Value, _raw: bool) -> Ordering {
    let (ra, rb) = (rank(a), rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Number(x), Value::Number(y)) => {
            if let (Some(i), Some(j)) = (x.as_i64(), y.as_i64()) {
                i.cmp(&j)
            } else {
                x.as_f64()
                    .unwrap_or(f64::NAN)
                    .partial_cmp(&y.as_f64().unwrap_or(f64::NAN))
                    .unwrap_or(Ordering::Equal)
            }
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Array(x), Value::Array(y)) => {
            for (ex, ey) in x.iter().zip(y.iter()) {
                match cmp(ex, ey, _raw) {
                    Ordering::Equal => {}
                    other => return other,
                }
            }
            x.len().cmp(&y.len())
        }
        (Value::Object(x), Value::Object(y)) => {
            for ((kx, vx), (ky, vy)) in x.iter().zip(y.iter()) {
                match kx.cmp(ky) {
                    Ordering::Equal => {}
                    other => return other,
                }
                match cmp(vx, vy, _raw) {
                    Ordering::Equal => {}
                    other => return other,
                }
            }
            x.len().cmp(&y.len())
        }
        _ => unreachable!("rank() guarantees same variant"),
    }
}

fn rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(false) => 1,
        Value::Bool(true) => 2,
        Value::Number(_) => 3,
        Value::String(_) => 4,
        Value::Array(_) => 5,
        Value::Object(_) => 6,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn m(sel: serde_json::Value, doc: serde_json::Value) -> bool {
        Selector::compile(&sel).expect("compile").matches(&doc)
    }

    #[test]
    fn empty_selector_matches_everything() {
        assert!(m(json!({}), json!({"a": 1})));
        assert!(m(json!({}), json!({})));
    }

    #[test]
    fn implicit_and_explicit_eq() {
        assert!(m(json!({"a": 1}), json!({"a": 1})));
        assert!(m(json!({"a": {"$eq": 1}}), json!({"a": 1})));
        assert!(!m(json!({"a": 1}), json!({"a": 2})));
        assert!(!m(json!({"a": 1}), json!({"b": 1})));
        // int/float compare numerically like the server
        assert!(m(json!({"a": 1}), json!({"a": 1.0})));
        // arrays and objects compare wholesale
        assert!(m(json!({"a": [1, 2]}), json!({"a": [1, 2]})));
        assert!(!m(json!({"a": [1, 2]}), json!({"a": [1, 2, 3]})));
        assert!(m(json!({"a": {}}), json!({"a": {}})));
        assert!(!m(json!({"a": {}}), json!({"a": {"x": 1}})));
    }

    #[test]
    fn implicit_and_multiple_fields() {
        let sel = json!({"a": 1, "b": "x"});
        assert!(m(sel.clone(), json!({"a": 1, "b": "x"})));
        assert!(!m(sel, json!({"a": 1, "b": "y"})));
    }

    #[test]
    fn nested_fields_dotted_and_object_form() {
        assert!(m(json!({"a.b": 5}), json!({"a": {"b": 5}})));
        assert!(m(json!({"a": {"b": 5}}), json!({"a": {"b": 5}})));
        assert!(!m(json!({"a": {"b": 5}}), json!({"a": {"b": 6}})));
        // multi-op condition on one field
        assert!(m(json!({"a": {"$gt": 1, "$lt": 3}}), json!({"a": 2})));
        assert!(!m(json!({"a": {"$gt": 1, "$lt": 3}}), json!({"a": 3})));
        // array index in path
        assert!(m(json!({"a.1.b": 7}), json!({"a": [{"b": 0}, {"b": 7}]})));
        assert!(!m(json!({"a.5": 1}), json!({"a": [1]})));
        // escaped dot is a literal key
        assert!(m(json!({"a\\.b": 3}), json!({"a.b": 3})));
        assert!(!m(json!({"a\\.b": 3}), json!({"a": {"b": 3}})));
    }

    #[test]
    fn comparison_and_type_order() {
        assert!(m(json!({"a": {"$gt": 5}}), json!({"a": 6})));
        assert!(!m(json!({"a": {"$gt": 5}}), json!({"a": 5})));
        assert!(m(json!({"a": {"$gte": 5}}), json!({"a": 5})));
        assert!(m(json!({"a": {"$lt": "m"}}), json!({"a": "abc"})));
        // null < false < true < number < string < array < object
        assert!(m(json!({"a": {"$lt": false}}), json!({"a": null})));
        assert!(m(json!({"a": {"$lt": 0}}), json!({"a": true})));
        assert!(m(json!({"a": {"$gt": 99999}}), json!({"a": "str"})));
        assert!(m(json!({"a": {"$gt": "zzz"}}), json!({"a": []})));
        assert!(m(json!({"a": {"$gt": [9]}}), json!({"a": {}})));
    }

    #[test]
    fn and_or_not_nor() {
        let sel = json!({"$or": [{"a": 1}, {"b": 2}]});
        assert!(m(sel.clone(), json!({"a": 1})));
        assert!(m(sel.clone(), json!({"b": 2})));
        assert!(!m(sel, json!({"a": 2})));

        let sel = json!({"$and": [{"a": {"$gt": 0}}, {"a": {"$lt": 10}}]});
        assert!(m(sel.clone(), json!({"a": 5})));
        assert!(!m(sel, json!({"a": 10})));

        assert!(m(json!({"$not": {"a": 1}}), json!({"a": 2})));
        // $not($eq) becomes $ne, which does NOT match a missing field
        assert!(!m(json!({"a": {"$not": {"$eq": 1}}}), json!({"b": 1})));
        // ...but $not($exists true) becomes $exists false, which does
        assert!(m(json!({"a": {"$not": {"$exists": true}}}), json!({"b": 1})));

        let sel = json!({"$nor": [{"a": 1}, {"a": 2}]});
        assert!(m(sel.clone(), json!({"a": 3})));
        assert!(!m(sel, json!({"a": 2})));

        // empty combinators are vacuously true ($in/$all are not)
        assert!(m(json!({"$and": []}), json!({})));
        assert!(m(json!({"$or": []}), json!({})));
    }

    #[test]
    fn in_nin() {
        assert!(m(json!({"a": {"$in": [1, 2, 3]}}), json!({"a": 2})));
        assert!(!m(json!({"a": {"$in": [1, 2, 3]}}), json!({"a": 4})));
        // array field: any element may match
        assert!(m(json!({"a": {"$in": [9, 2]}}), json!({"a": [1, 2, 3]})));
        assert!(!m(json!({"a": {"$in": []}}), json!({"a": 1})));
        assert!(m(json!({"a": {"$nin": [1, 2]}}), json!({"a": 3})));
        assert!(!m(json!({"a": {"$nin": [1, 2]}}), json!({"a": [2, 5]})));
        assert!(m(json!({"a": {"$nin": []}}), json!({"a": 1})));
        // missing field never matches $in or $nin
        assert!(!m(json!({"a": {"$in": [1]}}), json!({})));
        assert!(!m(json!({"a": {"$nin": [1]}}), json!({})));
    }

    #[test]
    fn exists() {
        assert!(m(json!({"a": {"$exists": true}}), json!({"a": null})));
        assert!(!m(json!({"a": {"$exists": true}}), json!({"b": 1})));
        assert!(m(json!({"a": {"$exists": false}}), json!({"b": 1})));
        assert!(!m(json!({"a": {"$exists": false}}), json!({"a": 0})));
    }

    #[test]
    fn type_size_mod_regex_begins_with() {
        assert!(m(json!({"a": {"$type": "number"}}), json!({"a": 1.5})));
        assert!(m(json!({"a": {"$type": "null"}}), json!({"a": null})));
        assert!(!m(json!({"a": {"$type": "string"}}), json!({"a": 1})));

        assert!(m(json!({"a": {"$size": 2}}), json!({"a": [1, 2]})));
        assert!(!m(json!({"a": {"$size": 2}}), json!({"a": [1]})));
        assert!(!m(json!({"a": {"$size": 2}}), json!({"a": "xx"})));

        assert!(m(json!({"a": {"$mod": [3, 1]}}), json!({"a": 10})));
        assert!(!m(json!({"a": {"$mod": [3, 1]}}), json!({"a": 9})));
        // floats never match $mod, mirroring is_integer/1
        assert!(!m(json!({"a": {"$mod": [3, 1]}}), json!({"a": 10.0})));

        assert!(m(json!({"a": {"$regex": "^ab+c"}}), json!({"a": "abbbc"})));
        assert!(!m(json!({"a": {"$regex": "^ab+c"}}), json!({"a": "xabc"})));
        assert!(!m(json!({"a": {"$regex": "1"}}), json!({"a": 1})));

        assert!(m(json!({"a": {"$beginsWith": "foo"}}), json!({"a": "foobar"})));
        assert!(!m(json!({"a": {"$beginsWith": "foo"}}), json!({"a": 5})));
    }

    #[test]
    fn all() {
        assert!(m(json!({"a": {"$all": [1, 2]}}), json!({"a": [3, 2, 1]})));
        assert!(!m(json!({"a": {"$all": [1, 4]}}), json!({"a": [1, 2]})));
        assert!(!m(json!({"a": {"$all": []}}), json!({"a": [1]})));
        // single array argument may equal the whole field value
        assert!(m(json!({"a": {"$all": [[1, 2]]}}), json!({"a": [1, 2]})));
        assert!(m(json!({"a": {"$all": [[1, 2]]}}), json!({"a": [[1, 2]]})));
        assert!(!m(json!({"a": {"$all": [1]}}), json!({"a": 1})));
    }

    #[test]
    fn elem_match_all_match_key_map_match() {
        let sel = json!({"a": {"$elemMatch": {"$gte": 80, "$lt": 85}}});
        assert!(m(sel.clone(), json!({"a": [70, 82, 90]})));
        assert!(!m(sel.clone(), json!({"a": [70, 90]})));
        assert!(!m(sel, json!({"a": 82})));

        let sel = json!({"a": {"$elemMatch": {"b": 1}}});
        assert!(m(sel.clone(), json!({"a": [{"b": 0}, {"b": 1}]})));
        assert!(!m(sel, json!({"a": [{"b": 0}]})));

        let sel = json!({"a": {"$allMatch": {"$gt": 0}}});
        assert!(m(sel.clone(), json!({"a": [1, 2]})));
        assert!(!m(sel.clone(), json!({"a": [1, 0]})));
        assert!(!m(sel, json!({"a": []}))); // empty array fails $allMatch

        let sel = json!({"a": {"$keyMapMatch": {"$eq": "x"}}});
        assert!(m(sel.clone(), json!({"a": {"x": 1, "y": 2}})));
        assert!(!m(sel.clone(), json!({"a": {"y": 2}})));
        assert!(!m(sel, json!({"a": [1]})));
    }

    #[test]
    fn demorgan_pushdown() {
        // $not over $and -> $or of negations
        let sel = json!({"$not": {"$and": [{"a": {"$gt": 10}}, {"a": {"$lt": 5}}]}});
        assert!(m(sel.clone(), json!({"a": 7})));
        assert!(m(sel, json!({"a": 12}))); // fails $lt 5 arm
        // double negation cancels
        let sel = json!({"$not": {"$not": {"a": 1}}});
        assert!(m(sel.clone(), json!({"a": 1})));
        assert!(!m(sel, json!({"a": 2})));
    }

    #[test]
    fn compile_errors() {
        assert!(Selector::compile(&json!({"$eq": 5})).is_err()); // no field
        assert!(Selector::compile(&json!({"$unknown": 5})).is_err());
        assert!(Selector::compile(&json!({"a": {"$foo": 1}})).is_err());
        assert!(Selector::compile(&json!({"$where": "x"})).is_err());
        assert!(Selector::compile(&json!({"$text": "x"})).is_err());
        assert!(Selector::compile(&json!({"a": {"$in": 5}})).is_err());
        assert!(Selector::compile(&json!({"a": {"$exists": 1}})).is_err());
        assert!(Selector::compile(&json!({"a": {"$mod": [3]}})).is_err());
        assert!(Selector::compile(&json!({"a": {"$regex": "("}})).is_err());
        assert!(Selector::compile(&json!({"a..b": 1})).is_err());
        assert!(Selector::compile(&json!(5)).is_err());
        // but a field-less condition below a combinator is legal (mango quirk)
        assert!(Selector::compile(&json!({"$and": [{"$eq": 5}]})).is_ok());
    }

    #[test]
    fn deleted_doc_stub() {
        // What the filter sees for a tombstone in the changes feed.
        let stub = json!({"_id": "d1", "_rev": "2-abc", "_deleted": true});
        assert!(!m(json!({"type": "order"}), stub.clone()));
        assert!(m(json!({"_deleted": true}), stub));
    }
}
