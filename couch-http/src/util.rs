//! Query-string and body helpers shared by the handlers.

use crate::error::{ApiError, ApiResult};
use serde_json::Value;
use std::collections::HashMap;

pub type Q = HashMap<String, String>;

pub fn qbool(q: &Q, name: &str, default: bool) -> bool {
    match q.get(name).map(|s| s.as_str()) {
        Some("true") | Some("1") => true,
        Some("false") | Some("0") => false,
        _ => default,
    }
}

pub fn qu64(q: &Q, name: &str) -> Option<u64> {
    q.get(name).and_then(|s| s.parse().ok())
}

/// A query param that is itself JSON (startkey="abc", keys=["a","b"]).
pub fn qjson(q: &Q, name: &str) -> ApiResult<Option<Value>> {
    match q.get(name) {
        None => Ok(None),
        Some(s) => serde_json::from_str(s)
            .map(Some)
            .map_err(|e| ApiError::bad_request(format!("invalid JSON for `{name}`: {e}"))),
    }
}

pub fn parse_json(body: &[u8]) -> ApiResult<Value> {
    if body.is_empty() {
        return Ok(Value::Object(Default::default()));
    }
    serde_json::from_slice(body).map_err(|e| ApiError::bad_request(format!("invalid UTF-8 JSON: {e}")))
}

/// The winner rev string of a FullDocInfo.
pub fn winner_rev(fdi: &couch_store::db::FullDocInfo) -> Option<String> {
    fdi.rev_tree
        .winner()
        .map(|w| couch_store::doc::rev_str(w.pos, w.path[0]))
}

/// Parse `multipart/related` (RFC 2387) as CouchDB uses it for doc PUTs:
/// part 1 is the JSON doc, following parts are attachment bodies in the
/// order of `"follows": true` entries in `_attachments`.
pub fn parse_multipart_related(content_type: &str, body: &[u8]) -> ApiResult<Value> {
    let boundary = content_type
        .split(';')
        .filter_map(|p| p.trim().strip_prefix("boundary="))
        .next()
        .map(|b| b.trim_matches('"').to_string())
        .ok_or_else(|| ApiError::bad_request("multipart/related without boundary"))?;
    let delim = format!("--{boundary}");
    let mut parts: Vec<&[u8]> = Vec::new();
    let mut rest = body;
    // Find each boundary line; content between boundaries is a part.
    let positions: Vec<usize> = find_all(rest, delim.as_bytes());
    for w in positions.windows(2) {
        let start = w[0] + delim.len();
        let part = &rest[start..w[1]];
        parts.push(part);
    }
    if positions.len() == 1 {
        // Possibly terminated by --boundary-- on the same find
        rest = &rest[positions[0] + delim.len()..];
        parts.push(rest);
    }
    let mut bodies = Vec::new();
    for p in parts {
        // The final delimiter is "--boundary--": its part is just "--".
        if p.starts_with(b"--") {
            continue;
        }
        // A part is "\r\n<headers>\r\n\r\n<body>\r\n" (headers may be empty,
        // as in couch-repl's attachment parts, putting the blank line at
        // position 0). Only that framing may be stripped: the body itself
        // can begin or end with newlines that are attachment data.
        let body = match find(p, b"\r\n\r\n") {
            Some(i) => &p[i + 4..],
            None => match find(p, b"\n\n") {
                Some(i) => &p[i + 2..],
                None => strip_one_leading_crlf(p), // no headers at all
            },
        };
        bodies.push(strip_one_trailing_crlf(body).to_vec());
    }
    if bodies.is_empty() {
        return Err(ApiError::bad_request("empty multipart body"));
    }
    let mut doc: Value = serde_json::from_slice(&bodies[0])
        .map_err(|e| ApiError::bad_request(format!("bad JSON part: {e}")))?;
    // Attach the following parts to the follows-attachments in order.
    let mut next = 1;
    if let Some(Value::Object(atts)) = doc.get_mut("_attachments") {
        use base64::Engine;
        for (_, spec) in atts.iter_mut() {
            if matches!(spec.get("follows"), Some(Value::Bool(true))) {
                let Some(data) = bodies.get(next) else {
                    return Err(ApiError::bad_request("multipart part count mismatch"));
                };
                next += 1;
                let obj = spec.as_object_mut().unwrap();
                obj.remove("follows");
                obj.insert(
                    "data".into(),
                    Value::String(base64::engine::general_purpose::STANDARD.encode(data)),
                );
            }
        }
    }
    Ok(doc)
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn find_all(hay: &[u8], needle: &[u8]) -> Vec<usize> {
    let mut out = Vec::new();
    let mut off = 0;
    while let Some(i) = find(&hay[off..], needle) {
        out.push(off + i);
        off += i + needle.len();
    }
    out
}

fn strip_one_leading_crlf(p: &[u8]) -> &[u8] {
    p.strip_prefix(b"\r\n".as_slice())
        .or_else(|| p.strip_prefix(b"\n".as_slice()))
        .unwrap_or(p)
}

fn strip_one_trailing_crlf(p: &[u8]) -> &[u8] {
    p.strip_suffix(b"\r\n".as_slice())
        .or_else(|| p.strip_suffix(b"\n".as_slice()))
        .unwrap_or(p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn multipart_related_roundtrip() {
        let doc = json!({
            "_id": "d", "v": 1,
            "_attachments": {
                "a.bin": {"follows": true, "content_type": "application/octet-stream", "length": 3},
                "b.bin": {"follows": true, "content_type": "text/plain", "length": 4},
            }
        });
        let b = "xyz";
        let body = format!(
            "--{b}\r\ncontent-type: application/json\r\n\r\n{doc}\r\n--{b}\r\n\r\nAAA\r\n--{b}\r\n\r\nBBBB\r\n--{b}--",
        );
        let parsed =
            parse_multipart_related(&format!("multipart/related; boundary=\"{b}\""), body.as_bytes())
                .unwrap();
        use base64::Engine;
        assert_eq!(
            parsed["_attachments"]["a.bin"]["data"],
            json!(base64::engine::general_purpose::STANDARD.encode("AAA"))
        );
        assert_eq!(
            parsed["_attachments"]["b.bin"]["data"],
            json!(base64::engine::general_purpose::STANDARD.encode("BBBB"))
        );
        assert_eq!(parsed["v"], json!(1));
    }

    /// Attachment bytes may begin or end with newlines and contain blank
    /// lines; only the multipart framing CRLFs may be stripped. (A csv
    /// ending in "\n" used to lose it — real replicated data corruption.)
    #[test]
    fn multipart_related_preserves_newlines_in_attachments() {
        let doc = json!({
            "_id": "d",
            "_attachments": {
                "runs.csv": {"follows": true, "content_type": "text/csv", "length": 14},
            }
        });
        let data = "\n\na,b\r\n\r\n1,2\n\n";
        assert_eq!(data.len(), 14);
        let b = "xyz";
        let body =
            format!("--{b}\r\ncontent-type: application/json\r\n\r\n{doc}\r\n--{b}\r\n\r\n{data}\r\n--{b}--");
        let parsed =
            parse_multipart_related(&format!("multipart/related; boundary=\"{b}\""), body.as_bytes())
                .unwrap();
        use base64::Engine;
        assert_eq!(
            parsed["_attachments"]["runs.csv"]["data"],
            json!(base64::engine::general_purpose::STANDARD.encode(data))
        );
    }
}
