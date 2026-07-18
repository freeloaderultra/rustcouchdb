//! Selective field extraction from protobuf wire bytes.
//!
//! Index maintenance knows exactly which paths it needs (`db.OwnerId`,
//! `subTaskId`), and the wire format is a tag-length-value stream — so
//! instead of decoding a whole multi-megabyte message, walk its tags and
//! skip every unwanted field by its announced length, recursively at each
//! nesting level. Combined with chunked attachment storage (skips become
//! seeks over the chunk list) the cost is O(wanted values + tag headers),
//! not O(document size).
//!
//! Parity with the full-decode path is by construction: wanted fields'
//! wire bytes are copied verbatim into a per-level buffer and decoded
//! with the same `DynamicMessage`/serde machinery the augmenter's full
//! path uses, so scalars, enums, packed repeateds, int64-stringification
//! and merge semantics can't diverge. Anything the trie can't navigate
//! (numeric array indexes, repeated/map intermediates, well-known types,
//! groups) refuses at compile or extract time and the caller falls back
//! to full decode.

use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Minimal reader contract: sequential reads plus cheap forward skips.
/// The `&[u8]` impl is for in-memory buffers; couch-http adapts the
/// chunk-list attachment reader (where `skip` avoids disk reads).
pub trait SkipRead {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), String>;
    fn skip(&mut self, n: u64) -> Result<(), String>;
}

pub struct SliceReader<'a>(pub &'a [u8]);

impl SkipRead for SliceReader<'_> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), String> {
        if self.0.len() < buf.len() {
            return Err("unexpected end of message".into());
        }
        let (head, rest) = self.0.split_at(buf.len());
        buf.copy_from_slice(head);
        self.0 = rest;
        Ok(())
    }

    fn skip(&mut self, n: u64) -> Result<(), String> {
        if (self.0.len() as u64) < n {
            return Err("unexpected end of message".into());
        }
        self.0 = &self.0[n as usize..];
        Ok(())
    }
}

struct Child {
    fd: FieldDescriptor,
    /// None = terminal (whole field wanted); Some = descend for sub-paths.
    next: Option<Node>,
}

#[derive(Default)]
struct Node {
    children: HashMap<u32, Child>,
}

/// A set of dotted paths compiled against a message descriptor into a trie
/// keyed by field numbers.
pub struct PathTrie {
    desc: MessageDescriptor,
    root: Node,
}

impl PathTrie {
    /// Compile `paths` (protojson names, proto names accepted too) against
    /// `desc`. Returns None when any path requires navigation this extractor
    /// doesn't support — numeric array indexes, repeated or map fields as
    /// intermediate segments, well-known types with special JSON forms —
    /// meaning the caller must use full decode. Paths that simply don't
    /// resolve to a field are ignored: full decode would produce nothing
    /// for them either.
    pub fn compile(desc: &MessageDescriptor, paths: &[String]) -> Option<PathTrie> {
        let mut root = Node::default();
        for path in paths {
            let segs: Vec<&str> = path.split('.').collect();
            if segs.iter().any(|s| s.is_empty()) {
                return None;
            }
            insert(&mut root, desc, &segs)?;
        }
        // Paths whose leaf never resolved leave empty descend chains behind;
        // drop them so extraction doesn't fabricate empty objects.
        prune(&mut root);
        if root.children.is_empty() {
            return None;
        }
        Some(PathTrie {
            desc: desc.clone(),
            root,
        })
    }

    /// Extract the compiled paths from `len` bytes of wire data. The result
    /// is a JSON object holding only the wanted paths, shaped exactly as the
    /// full decode would shape them. Errors mean "fall back to full decode".
    pub fn extract(&self, r: &mut dyn SkipRead, len: u64) -> Result<Value, String> {
        let m = extract_msg(r, len, &self.root, &self.desc)?;
        Ok(Value::Object(m))
    }
}

fn insert(node: &mut Node, desc: &MessageDescriptor, segs: &[&str]) -> Option<()> {
    let seg = segs[0];
    if seg.chars().all(|c| c.is_ascii_digit()) {
        // numeric segment = array index; full decode handles those
        return None;
    }
    let Some(fd) = desc
        .fields()
        .find(|f| f.json_name() == seg || f.name() == seg)
    else {
        // unknown field: the path can never resolve, same as full decode
        return Some(());
    };
    if segs.len() == 1 {
        // terminal beats any previously-inserted descend (it wants more)
        node.children.insert(fd.number(), Child { fd, next: None });
        return Some(());
    }
    // descend: must be a plain singular message field with default JSON form
    if fd.is_list() || fd.is_map() {
        return None;
    }
    let Kind::Message(sub) = fd.kind() else {
        // path descends into a scalar: resolves to nothing, like full decode
        return Some(());
    };
    if sub.full_name().starts_with("google.protobuf.") {
        // wrappers/Timestamp/Struct/... serialize to special JSON forms
        return None;
    }
    let number = fd.number();
    let child = node.children.entry(number).or_insert_with(|| Child {
        fd,
        next: Some(Node::default()),
    });
    match &mut child.next {
        None => Some(()), // already terminal, which covers this sub-path
        Some(next) => insert(next, &sub, &segs[1..]),
    }
}

fn prune(node: &mut Node) {
    node.children.retain(|_, c| match &mut c.next {
        None => true,
        Some(n) => {
            prune(n);
            !n.children.is_empty()
        }
    });
}

fn read_varint(r: &mut dyn SkipRead, remaining: &mut u64) -> Result<u64, String> {
    let mut out: u64 = 0;
    let mut shift = 0u32;
    loop {
        if *remaining == 0 {
            return Err("varint runs past extent".into());
        }
        let mut b = [0u8; 1];
        r.read_exact(&mut b)?;
        *remaining -= 1;
        if shift >= 64 {
            return Err("varint too long".into());
        }
        out |= u64::from(b[0] & 0x7f) << shift;
        if b[0] & 0x80 == 0 {
            return Ok(out);
        }
        shift += 7;
    }
}

fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(b);
            break;
        }
        buf.push(b | 0x80);
    }
}

fn take(r: &mut dyn SkipRead, n: u64, remaining: &mut u64) -> Result<Vec<u8>, String> {
    if *remaining < n {
        return Err("field runs past extent".into());
    }
    let mut buf = vec![0u8; n as usize];
    r.read_exact(&mut buf)?;
    *remaining -= n;
    Ok(buf)
}

fn extract_msg(
    r: &mut dyn SkipRead,
    len: u64,
    node: &Node,
    desc: &MessageDescriptor,
) -> Result<Map<String, Value>, String> {
    let mut remaining = len;
    // Wanted-terminal wire bytes, decoded once per level at extent end.
    let mut term_buf: Vec<u8> = Vec::new();
    let mut out: Map<String, Value> = Map::new();

    while remaining > 0 {
        let tag = read_varint(r, &mut remaining)?;
        let field_no = (tag >> 3) as u32;
        let wire = (tag & 7) as u8;
        let child = node.children.get(&field_no);
        match wire {
            0 => {
                let start_remaining = remaining;
                let v = read_varint(r, &mut remaining)?;
                if matches!(child, Some(Child { next: None, .. })) {
                    put_varint(&mut term_buf, tag);
                    put_varint(&mut term_buf, v);
                }
                let _ = start_remaining;
            }
            1 | 5 => {
                let n = if wire == 1 { 8 } else { 4 };
                if matches!(child, Some(Child { next: None, .. })) {
                    put_varint(&mut term_buf, tag);
                    term_buf.extend_from_slice(&take(r, n, &mut remaining)?);
                } else {
                    if remaining < n {
                        return Err("field runs past extent".into());
                    }
                    r.skip(n)?;
                    remaining -= n;
                }
            }
            2 => {
                let flen = read_varint(r, &mut remaining)?;
                match child {
                    None => {
                        if remaining < flen {
                            return Err("field runs past extent".into());
                        }
                        r.skip(flen)?;
                        remaining -= flen;
                    }
                    Some(Child { next: None, .. }) => {
                        put_varint(&mut term_buf, tag);
                        put_varint(&mut term_buf, flen);
                        term_buf.extend_from_slice(&take(r, flen, &mut remaining)?);
                    }
                    Some(Child {
                        fd,
                        next: Some(sub_node),
                    }) => {
                        if remaining < flen {
                            return Err("field runs past extent".into());
                        }
                        remaining -= flen;
                        let Kind::Message(sub_desc) = fd.kind() else {
                            return Err("descend into non-message".into());
                        };
                        let sub = extract_msg(r, flen, sub_node, &sub_desc)?;
                        // A singular message field may occur multiple times
                        // on the wire; proto merges them.
                        match out.get_mut(fd.json_name()) {
                            Some(prev) => deep_merge(prev, Value::Object(sub)),
                            None => {
                                out.insert(fd.json_name().to_string(), Value::Object(sub));
                            }
                        }
                    }
                }
            }
            3 | 4 => return Err("group fields are not supported".into()),
            other => return Err(format!("bad wire type {other}")),
        }
    }

    if !term_buf.is_empty() {
        let msg = DynamicMessage::decode(desc.clone(), term_buf.as_slice())
            .map_err(|e| format!("terminal decode: {e}"))?;
        let v = serde_json::to_value(&msg).map_err(|e| format!("terminal json: {e}"))?;
        if let Value::Object(m) = v {
            for (k, v) in m {
                // terminal and descend children are disjoint by construction
                out.insert(k, v);
            }
        }
    }
    Ok(out)
}

/// Proto merge semantics in JSON form: objects merge recursively, arrays
/// concatenate, scalars last-wins.
fn deep_merge(a: &mut Value, b: Value) {
    match (a, b) {
        (Value::Object(am), Value::Object(bm)) => {
            for (k, bv) in bm {
                match am.get_mut(&k) {
                    Some(av) => deep_merge(av, bv),
                    None => {
                        am.insert(k, bv);
                    }
                }
            }
        }
        (Value::Array(aa), Value::Array(ba)) => aa.extend(ba),
        (a, b) => *a = b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Registry;
    use prost::Message as _;
    use std::collections::HashMap as StdHashMap;

    // Reuse the crate test schema: test.v1.Point / test.v1.FieldBoundary
    // (name=1 string, area=2 double, geo_points=3 repeated Point,
    //  big_count=4 int64, top_left=5 Point).
    fn registry() -> Registry {
        let (reg, problems) = Registry::build(
            &[crate::tests::test_descriptor_set().encode_to_vec()],
            &StdHashMap::new(),
        )
        .unwrap();
        assert!(problems.is_empty(), "{problems:?}");
        reg
    }

    fn full_decode(reg: &Registry, bytes: &[u8]) -> Value {
        reg.decode_doc("field_boundary", bytes).unwrap()
    }

    fn filter_paths(full: &Value, paths: &[&str]) -> Value {
        let mut out = Map::new();
        for p in paths {
            if let Some(v) = get_path(full, p) {
                set_path(&mut out, p, v.clone());
            }
        }
        Value::Object(out)
    }

    fn get_path<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
        let mut cur = v;
        for seg in path.split('.') {
            cur = cur.get(seg)?;
        }
        Some(cur)
    }

    fn set_path(out: &mut Map<String, Value>, path: &str, v: Value) {
        let mut segs: Vec<&str> = path.split('.').collect();
        let last = segs.pop().unwrap();
        let mut cur = out;
        for s in segs {
            cur = cur
                .entry(s.to_string())
                .or_insert_with(|| Value::Object(Map::new()))
                .as_object_mut()
                .unwrap();
        }
        cur.insert(last.to_string(), v);
    }

    fn extract_paths(reg: &Registry, bytes: &[u8], paths: &[&str]) -> Option<Value> {
        let desc = reg.resolve("field_boundary")?.clone();
        let owned: Vec<String> = paths.iter().map(|s| s.to_string()).collect();
        let trie = PathTrie::compile(&desc, &owned)?;
        trie.extract(&mut SliceReader(bytes), bytes.len() as u64).ok()
    }

    #[test]
    fn extraction_equals_filtered_full_decode() {
        let reg = registry();
        let bytes = crate::tests::encode_boundary(1234.5, 48.1, 11.5, 9007199254740993);
        for paths in [
            vec!["area"],
            vec!["name", "bigCount"],
            vec!["topLeft.latitude"],
            vec!["geoPoints"],
            vec!["area", "topLeft.longitude", "name"],
            vec!["topLeft"],
            vec!["absentField", "area"],
            vec!["topLeft.noSuchLeaf", "area"],
        ] {
            let got = extract_paths(&reg, &bytes, &paths).unwrap();
            let want = filter_paths(&full_decode(&reg, &bytes), &paths);
            assert_eq!(got, want, "paths {paths:?}");
        }
    }

    #[test]
    fn skips_unknown_and_huge_fields() {
        let reg = registry();
        let mut bytes = crate::tests::encode_boundary(7.0, 1.0, 2.0, 3);
        // Append a large unknown length-delimited field (number 900).
        let payload = vec![0xAB; 3_000_000];
        let tag = (900u64 << 3) | 2;
        let mut extra = Vec::new();
        put_varint(&mut extra, tag);
        put_varint(&mut extra, payload.len() as u64);
        extra.extend_from_slice(&payload);
        bytes.extend_from_slice(&extra);

        let got = extract_paths(&reg, &bytes, &["area"]).unwrap();
        assert_eq!(got["area"], serde_json::json!(7.0));
    }

    #[test]
    fn split_submessage_occurrences_merge() {
        let reg = registry();
        // Two occurrences of top_left (field 5): {latitude} then {longitude}.
        // Proto merge semantics combine them into one Point.
        let mut bytes = Vec::new();
        let mut p1 = Vec::new();
        put_varint(&mut p1, (1 << 3) | 1); // latitude, 64-bit
        p1.extend_from_slice(&48.5f64.to_le_bytes());
        put_varint(&mut bytes, (5 << 3) | 2);
        put_varint(&mut bytes, p1.len() as u64);
        bytes.extend_from_slice(&p1);
        let mut p2 = Vec::new();
        put_varint(&mut p2, (2 << 3) | 1); // longitude
        p2.extend_from_slice(&11.25f64.to_le_bytes());
        put_varint(&mut bytes, (5 << 3) | 2);
        put_varint(&mut bytes, p2.len() as u64);
        bytes.extend_from_slice(&p2);

        // Descend form (paths below the field) must merge occurrences...
        let got = extract_paths(&reg, &bytes, &["topLeft.latitude", "topLeft.longitude"]).unwrap();
        assert_eq!(got["topLeft"]["latitude"], serde_json::json!(48.5));
        assert_eq!(got["topLeft"]["longitude"], serde_json::json!(11.25));
        // ...and so must the terminal form (DynamicMessage handles it).
        let got = extract_paths(&reg, &bytes, &["topLeft"]).unwrap();
        assert_eq!(got["topLeft"]["latitude"], serde_json::json!(48.5));
        assert_eq!(got["topLeft"]["longitude"], serde_json::json!(11.25));
        // Matches full decode.
        let full = full_decode(&reg, &bytes);
        assert_eq!(got["topLeft"], full["topLeft"]);
    }

    #[test]
    fn unsupported_constructs_refuse_compilation() {
        let reg = registry();
        let desc = reg.resolve("field_boundary").unwrap().clone();
        // numeric array index
        assert!(PathTrie::compile(&desc, &["geoPoints.0.latitude".into()]).is_none());
        // descend through a repeated field
        assert!(PathTrie::compile(&desc, &["geoPoints.latitude".into()]).is_none());
        // nothing resolvable at all
        assert!(PathTrie::compile(&desc, &["nope".into()]).is_none());
        // empty segment
        assert!(PathTrie::compile(&desc, &["top..left".into()]).is_none());
    }

    #[test]
    fn truncated_input_errors() {
        let reg = registry();
        let bytes = crate::tests::encode_boundary(1.0, 2.0, 3.0, 4);
        let desc = reg.resolve("field_boundary").unwrap().clone();
        let trie = PathTrie::compile(&desc, &["area".into()]).unwrap();
        // Claim a longer extent than we have bytes.
        assert!(trie
            .extract(&mut SliceReader(&bytes), bytes.len() as u64 + 10)
            .is_err());
        // Truncate mid-field.
        assert!(trie
            .extract(&mut SliceReader(&bytes[..bytes.len() - 3]), bytes.len() as u64)
            .is_err());
    }
}
