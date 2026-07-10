//! Translate a restricted subset of JavaScript replication filter functions
//! into Mango selectors, so `filter: "ddoc/name"` replications work without
//! executing JavaScript (the same approach as the native soft-delete
//! validator: recognize the pattern, compile it to Rust-evaluated Mango).
//!
//! Supported shape:
//!
//! ```js
//! function(doc, req) { return doc.a.b === req.query.x && doc.type == 'field'; }
//! ```
//!
//! - comparisons: `===`/`==` (→ `$eq`) and `!==`/`!=` (→ `$ne`), one side a
//!   `doc.` path, the other `req.query.<name>` or a literal
//! - combinators: `&&` (→ `$and`), `||` (→ `$or`), parentheses
//!
//! Anything else fails with a descriptive error; the replication is rejected
//! instead of silently misfiltering.

use serde_json::{json, Map, Value};

/// Compile `src` (a JS filter function) to a Mango selector, substituting
/// `req.query.*` references from `query_params`.
pub fn js_filter_to_selector(src: &str, query_params: &Map<String, Value>) -> Result<Value, String> {
    let body = extract_return_expr(src)?;
    let mut p = Parser { s: body.as_bytes(), pos: 0, query_params };
    let sel = p.parse_or()?;
    p.skip_ws();
    if p.pos != p.s.len() {
        return Err(format!(
            "unsupported trailing filter code: {:?}",
            String::from_utf8_lossy(&p.s[p.pos..])
        ));
    }
    Ok(sel)
}

/// Pull the expression out of `function(doc, req) { return EXPR; }`.
fn extract_return_expr(src: &str) -> Result<String, String> {
    let open = src.find('{').ok_or("filter is not a function body")?;
    let close = src.rfind('}').ok_or("filter is not a function body")?;
    if close <= open {
        return Err("filter is not a function body".into());
    }
    let body = src[open + 1..close].trim();
    let body = body
        .strip_prefix("return")
        .ok_or("filter body must be a single return statement")?
        .trim();
    let body = body.strip_suffix(';').unwrap_or(body).trim();
    if body.is_empty() {
        return Err("filter returns nothing".into());
    }
    Ok(body.to_string())
}

struct Parser<'a> {
    s: &'a [u8],
    pos: usize,
    query_params: &'a Map<String, Value>,
}

/// One comparison operand.
enum Operand {
    DocPath(String),
    Val(Value),
}

impl<'a> Parser<'a> {
    fn skip_ws(&mut self) {
        while self.pos < self.s.len() && self.s[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn eat(&mut self, tok: &str) -> bool {
        self.skip_ws();
        if self.s[self.pos..].starts_with(tok.as_bytes()) {
            self.pos += tok.len();
            true
        } else {
            false
        }
    }

    fn parse_or(&mut self) -> Result<Value, String> {
        let mut terms = vec![self.parse_and()?];
        while self.eat("||") {
            terms.push(self.parse_and()?);
        }
        Ok(if terms.len() == 1 { terms.pop().unwrap() } else { json!({"$or": terms}) })
    }

    fn parse_and(&mut self) -> Result<Value, String> {
        let mut terms = vec![self.parse_unary()?];
        while self.eat("&&") {
            terms.push(self.parse_unary()?);
        }
        Ok(if terms.len() == 1 { terms.pop().unwrap() } else { json!({"$and": terms}) })
    }

    fn parse_unary(&mut self) -> Result<Value, String> {
        self.skip_ws();
        if self.eat("(") {
            let inner = self.parse_or()?;
            if !self.eat(")") {
                return Err("expected ')'".into());
            }
            return Ok(inner);
        }
        self.parse_cmp()
    }

    fn parse_cmp(&mut self) -> Result<Value, String> {
        let left = self.parse_operand()?;
        self.skip_ws();
        // Longest operators first.
        let (op_eq, len) = if self.s[self.pos..].starts_with(b"===") {
            (true, 3)
        } else if self.s[self.pos..].starts_with(b"!==") {
            (false, 3)
        } else if self.s[self.pos..].starts_with(b"==") {
            (true, 2)
        } else if self.s[self.pos..].starts_with(b"!=") {
            (false, 2)
        } else {
            return Err("expected a comparison (===, ==, !==, !=)".into());
        };
        self.pos += len;
        let right = self.parse_operand()?;
        let (path, val) = match (left, right) {
            (Operand::DocPath(p), Operand::Val(v)) | (Operand::Val(v), Operand::DocPath(p)) => (p, v),
            (Operand::DocPath(_), Operand::DocPath(_)) => {
                return Err("comparing two doc fields is not supported".into())
            }
            (Operand::Val(_), Operand::Val(_)) => {
                return Err("comparison must reference a doc field".into())
            }
        };
        let op = if op_eq { "$eq" } else { "$ne" };
        Ok(json!({ path: { op: val } }))
    }

    fn parse_operand(&mut self) -> Result<Operand, String> {
        self.skip_ws();
        if self.pos >= self.s.len() {
            return Err("unexpected end of filter expression".into());
        }
        match self.s[self.pos] {
            b'\'' | b'"' => {
                let quote = self.s[self.pos];
                self.pos += 1;
                let start = self.pos;
                while self.pos < self.s.len() && self.s[self.pos] != quote {
                    if self.s[self.pos] == b'\\' {
                        return Err("escape sequences in string literals are not supported".into());
                    }
                    self.pos += 1;
                }
                if self.pos >= self.s.len() {
                    return Err("unterminated string literal".into());
                }
                let v = String::from_utf8_lossy(&self.s[start..self.pos]).into_owned();
                self.pos += 1;
                Ok(Operand::Val(Value::String(v)))
            }
            b'0'..=b'9' | b'-' => {
                let start = self.pos;
                self.pos += 1;
                while self.pos < self.s.len()
                    && (self.s[self.pos].is_ascii_digit() || self.s[self.pos] == b'.')
                {
                    self.pos += 1;
                }
                let txt = String::from_utf8_lossy(&self.s[start..self.pos]).into_owned();
                let n: Value =
                    serde_json::from_str(&txt).map_err(|_| format!("bad number literal {txt:?}"))?;
                Ok(Operand::Val(n))
            }
            _ => {
                let ident = self.parse_path()?;
                match ident.as_str() {
                    "true" => return Ok(Operand::Val(Value::Bool(true))),
                    "false" => return Ok(Operand::Val(Value::Bool(false))),
                    "null" => return Ok(Operand::Val(Value::Null)),
                    _ => {}
                }
                if let Some(field) = ident.strip_prefix("doc.") {
                    return Ok(Operand::DocPath(field.to_string()));
                }
                if let Some(name) = ident.strip_prefix("req.query.") {
                    let v = self.query_params.get(name).cloned().ok_or(format!(
                        "filter references req.query.{name} but query_params has no {name:?}"
                    ))?;
                    return Ok(Operand::Val(v));
                }
                Err(format!("unsupported operand {ident:?} (only doc.*, req.query.*, and literals)"))
            }
        }
    }

    fn parse_path(&mut self) -> Result<String, String> {
        let start = self.pos;
        while self.pos < self.s.len()
            && (self.s[self.pos].is_ascii_alphanumeric()
                || self.s[self.pos] == b'_'
                || self.s[self.pos] == b'$'
                || self.s[self.pos] == b'.')
        {
            self.pos += 1;
        }
        if self.pos == start {
            return Err(format!(
                "unsupported filter syntax at {:?}",
                String::from_utf8_lossy(&self.s[self.pos..self.pos + 10.min(self.s.len() - self.pos)])
            ));
        }
        Ok(String::from_utf8_lossy(&self.s[start..self.pos]).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn qp(pairs: &[(&str, &str)]) -> Map<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), json!(v))).collect()
    }

    #[test]
    fn nxguide_by_user() {
        let sel = js_filter_to_selector(
            "function(doc, req) { return doc.userid === req.query.userid; }",
            &qp(&[("userid", "u-1")]),
        )
        .unwrap();
        assert_eq!(sel, json!({"userid": {"$eq": "u-1"}}));
    }

    #[test]
    fn literals_and_combinators() {
        let sel = js_filter_to_selector(
            "function(doc, req) {\n  return (doc.type == 'field' || doc.type == \"vehicle\") && doc.deleted !== true;\n}",
            &Map::new(),
        )
        .unwrap();
        assert_eq!(
            sel,
            json!({"$and": [
                {"$or": [{"type": {"$eq": "field"}}, {"type": {"$eq": "vehicle"}}]},
                {"deleted": {"$ne": true}},
            ]})
        );
    }

    #[test]
    fn nested_path_and_flipped_sides() {
        let sel = js_filter_to_selector(
            "function(doc, req) { return req.query.x === doc.meta.owner; }",
            &qp(&[("x", "o1")]),
        )
        .unwrap();
        assert_eq!(sel, json!({"meta.owner": {"$eq": "o1"}}));
    }

    #[test]
    fn rejects_unsupported() {
        for bad in [
            "function(doc, req) { if (doc.x) return true; }",
            "function(doc, req) { return doc.a > 3; }",
            "function(doc, req) { return doc.a === doc.b; }",
            "function(doc, req) { return req.query.a === 'x'; }",
            "function(doc, req) { return doc.userid === req.query.missing; }",
        ] {
            assert!(js_filter_to_selector(bad, &Map::new()).is_err(), "{bad}");
        }
    }
}
