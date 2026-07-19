//! Evaluate a Mango selector directly against a stored proto document.
//!
//! `couch_mango` matches a [`couch_mango::Doc`]; `ProtoDoc` is the proto-native
//! implementation. It navigates the message **on the wire**, following only the
//! path a selector clause references and skipping every other field by its
//! encoded length — the document is never fully decoded or materialized to JSON.
//! Matching cost is O(selector paths), independent of document size.
//!
//! It applies proto3 semantics: a scalar field has no presence, so an unset
//! scalar reads as its default value (empty string, `0`, `false`, the zero
//! enum) — exactly how the old JSON store spelled it, so `{"fieldId": ""}`
//! matches a message whose empty `fieldId` isn't on the wire. An unset *message*
//! field has presence and reads as absent (only `{$exists: false}` matches).
//!
//! Leaf conversion mirrors protojson (the convention the stored `$pb` bodies and
//! the selectors were written against): enums render as their value name,
//! 64-bit integers as decimal strings, bytes as base64. The actual value decode
//! is delegated to prost (a single field's wire bytes are re-decoded), so all
//! scalar/enum/packed/merge semantics match a full decode exactly.

use base64::Engine as _;
use couch_mango::Doc;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, Value as PdValue};
use serde_json::Value as JsonValue;
use std::borrow::Cow;

/// A stored proto document presented as a Mango-matchable document. `meta`
/// carries the couch metadata outside the proto body (`_id`, `_rev`,
/// `_deleted`, …); domain and `db.*` fields come from the wire `bytes`.
pub struct ProtoDoc<'a> {
    desc: MessageDescriptor,
    bytes: &'a [u8],
    meta: Option<&'a JsonValue>,
}

impl<'a> ProtoDoc<'a> {
    pub fn new(desc: MessageDescriptor, bytes: &'a [u8]) -> Self {
        ProtoDoc { desc, bytes, meta: None }
    }

    /// Attach the couch metadata object (typically the `$pb` envelope carrying
    /// `_id`/`_rev`/`_deleted`) that selectors on reserved fields resolve against.
    pub fn with_meta(desc: MessageDescriptor, bytes: &'a [u8], meta: &'a JsonValue) -> Self {
        ProtoDoc { desc, bytes, meta: Some(meta) }
    }
}

impl Doc for ProtoDoc<'_> {
    fn get_path(&self, path: &[String]) -> Option<Cow<'_, JsonValue>> {
        // Reserved (`_id`, `_rev`, `_deleted`, …) fields are couch metadata, not
        // part of the proto body — resolve them against `meta` as plain JSON.
        if path.first().is_some_and(|s| s.starts_with('_')) {
            return self.meta.and_then(|m| m.get_path(path));
        }
        resolve(self.bytes, &self.desc, path).map(Cow::Owned)
    }
}

/// Resolve a dotted path within a message's wire bytes, decoding only the field
/// it lands on. `None` means the path is genuinely absent (unknown field, or an
/// unset message field).
fn resolve(bytes: &[u8], desc: &MessageDescriptor, path: &[String]) -> Option<JsonValue> {
    let (seg, rest) = path.split_first()?;
    // Accept the protojson name or the raw proto name (the `db` envelope uses
    // PascalCase proto names that are also their json names).
    let field = desc
        .get_field_by_json_name(seg)
        .or_else(|| desc.get_field_by_name(seg))?;
    let num = field.number();

    if rest.is_empty() {
        // Terminal: collect this field's wire bytes (all occurrences), re-decode
        // via prost, convert. Empty ⇒ proto3 default (scalar/repeated) or absent
        // (singular message).
        let mut term: Vec<u8> = Vec::new();
        scan(bytes, |f, whole, _| {
            if f == num {
                term.extend_from_slice(whole);
            }
            true
        })?;
        let singular_msg = matches!(field.kind(), Kind::Message(_)) && !field.is_list() && !field.is_map();
        if term.is_empty() && singular_msg {
            return None;
        }
        let msg = DynamicMessage::decode(desc.clone(), term.as_slice()).ok()?;
        return Some(convert_field(&msg.get_field(&field), &field));
    }

    // Descend: the field must be a message. A numeric next segment indexes a
    // repeated message; a singular message merges its occurrences (proto merge).
    let Kind::Message(sub_desc) = field.kind() else {
        return None;
    };
    if field.is_list() {
        let idx: usize = rest[0].parse().ok()?;
        let mut payloads: Vec<Vec<u8>> = Vec::new();
        scan(bytes, |f, _, payload| {
            if f == num {
                payloads.push(payload.to_vec());
            }
            true
        })?;
        let p = payloads.get(idx)?;
        return resolve(p, &sub_desc, &rest[1..]);
    }
    let mut merged: Vec<u8> = Vec::new();
    let mut found = false;
    scan(bytes, |f, _, payload| {
        if f == num {
            merged.extend_from_slice(payload);
            found = true;
        }
        true
    })?;
    if !found {
        return None;
    }
    resolve(&merged, &sub_desc, rest)
}

/// Walk protobuf wire fields, calling `visit(field_number, whole_field_bytes,
/// payload_bytes)` for each. `whole_field_bytes` is the tag through the payload
/// (for re-decoding a scalar); `payload_bytes` is the length-delimited body
/// (a sub-message's contents). Returns `None` on malformed input or groups.
fn scan<'a>(mut b: &'a [u8], mut visit: impl FnMut(u32, &'a [u8], &'a [u8]) -> bool) -> Option<()> {
    while !b.is_empty() {
        let (tag, tn) = read_varint(b)?;
        let num = (tag >> 3) as u32;
        let wire = (tag & 7) as u8;
        let after_tag = &b[tn..];
        let (poff, plen) = match wire {
            0 => (0, varint_len(after_tag)?), // varint payload is the varint itself
            1 => (0, 8),
            5 => (0, 4),
            2 => {
                let (len, ln) = read_varint(after_tag)?;
                (ln, len as usize)
            }
            _ => return None, // groups (3/4) unsupported
        };
        let field_len = tn + poff + plen;
        if field_len > b.len() {
            return None;
        }
        if !visit(num, &b[..field_len], &b[tn + poff..field_len]) {
            return Some(());
        }
        b = &b[field_len..];
    }
    Some(())
}

/// Read a base-128 varint; returns (value, bytes consumed).
fn read_varint(b: &[u8]) -> Option<(u64, usize)> {
    let mut val: u64 = 0;
    let mut shift = 0u32;
    for (i, &byte) in b.iter().take(10).enumerate() {
        val |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some((val, i + 1));
        }
        shift += 7;
    }
    None
}

/// Byte length of the varint at `b[0..]` (without decoding its value).
fn varint_len(b: &[u8]) -> Option<usize> {
    read_varint(b).map(|(_, n)| n)
}

/// protojson renders every map key as a string.
fn map_key_string(k: &MapKey) -> String {
    match k {
        MapKey::Bool(b) => b.to_string(),
        MapKey::I32(n) => n.to_string(),
        MapKey::I64(n) => n.to_string(),
        MapKey::U32(n) => n.to_string(),
        MapKey::U64(n) => n.to_string(),
        MapKey::String(s) => s.clone(),
    }
}

/// Convert a field value to JSON, handling repeated and map fields.
fn convert_field(val: &PdValue, field: &FieldDescriptor) -> JsonValue {
    if field.is_list() {
        if let PdValue::List(items) = val {
            return JsonValue::Array(items.iter().map(|it| convert_scalar(it, field)).collect());
        }
    }
    if field.is_map() {
        if let PdValue::Map(map) = val {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(map_key_string(k), convert_scalar(v, field));
            }
            return JsonValue::Object(obj);
        }
    }
    convert_scalar(val, field)
}

/// Convert a single (non-repeated) proto value to a protojson-shaped JSON value.
fn convert_scalar(val: &PdValue, field: &FieldDescriptor) -> JsonValue {
    match val {
        PdValue::Bool(b) => JsonValue::Bool(*b),
        PdValue::I32(n) => JsonValue::from(*n),
        PdValue::U32(n) => JsonValue::from(*n),
        // 64-bit integers render as strings (protojson).
        PdValue::I64(n) => JsonValue::String(n.to_string()),
        PdValue::U64(n) => JsonValue::String(n.to_string()),
        PdValue::F32(f) => serde_json::Number::from_f64(*f as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        PdValue::F64(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        PdValue::String(s) => JsonValue::String(s.clone()),
        PdValue::Bytes(b) => JsonValue::String(base64::engine::general_purpose::STANDARD.encode(b)),
        PdValue::EnumNumber(n) => match field.kind() {
            Kind::Enum(ed) => ed
                .get_value(*n)
                .map(|v| JsonValue::String(v.name().to_string()))
                .unwrap_or_else(|| JsonValue::from(*n)),
            _ => JsonValue::from(*n),
        },
        PdValue::Message(m) => convert_message(m),
        PdValue::List(items) => {
            JsonValue::Array(items.iter().map(|it| convert_scalar(it, field)).collect())
        }
        PdValue::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(map_key_string(k), convert_scalar(v, field));
            }
            JsonValue::Object(obj)
        }
    }
}

/// Materialize a whole sub-message to a JSON object with proto3 defaults for
/// scalars (only when a selector targets an entire sub-object or `$elemMatch`
/// over a repeated message field). Unset singular message fields are omitted.
fn convert_message(msg: &DynamicMessage) -> JsonValue {
    use prost_reflect::ReflectMessage;
    let mut obj = serde_json::Map::new();
    for field in msg.descriptor().fields() {
        if matches!(field.kind(), Kind::Message(_))
            && !field.is_list()
            && !field.is_map()
            && !msg.has_field(&field)
        {
            continue;
        }
        obj.insert(
            field.json_name().to_string(),
            convert_field(&msg.get_field(&field), &field),
        );
    }
    JsonValue::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Registry;
    use couch_mango::Selector;
    use prost::Message as _;
    use serde_json::json;
    use std::collections::HashMap as StdHashMap;

    fn registry() -> Registry {
        let (reg, problems) = Registry::build(
            &[crate::tests::test_descriptor_set().encode_to_vec()],
            &StdHashMap::new(),
        )
        .unwrap();
        assert!(problems.is_empty(), "{problems:?}");
        reg
    }

    // test.v1.FieldBoundary: name=1 string, area=2 double, geo_points=3 repeated
    // Point, big_count=4 int64, top_left=5 Point. Point: latitude=1, longitude=2.
    fn matches(sel: serde_json::Value, desc: &MessageDescriptor, bytes: &[u8]) -> bool {
        Selector::compile(&sel)
            .unwrap()
            .matches(&ProtoDoc::new(desc.clone(), bytes))
    }

    // Cross-check the wire matcher against a full-decode reference. The oracle
    // serializes with proto3 defaults emitted (an unset scalar reads as its
    // zero value) — the semantics ProtoDoc applies on the wire. The default
    // protojson serialization skips defaults, which is right for the human
    // view but is not a valid oracle for default-honoring matching.
    fn ref_matches(sel: &serde_json::Value, reg: &Registry, bytes: &[u8]) -> bool {
        let desc = reg.resolve_full("test.v1.FieldBoundary").unwrap();
        let msg = DynamicMessage::decode(desc, bytes).unwrap();
        let mut buf = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut buf);
        msg.serialize_with_options(
            &mut ser,
            &prost_reflect::SerializeOptions::new().skip_default_fields(false),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        Selector::compile(sel).unwrap().matches(&v)
    }

    #[test]
    fn wire_matching_matches_proto3_defaults() {
        let reg = registry();
        let fb = reg.resolve_full("test.v1.FieldBoundary").unwrap();
        let pt = reg.resolve_full("test.v1.Point").unwrap();
        let mut top = DynamicMessage::new(pt.clone());
        top.set_field_by_name("latitude", PdValue::F64(48.5)); // longitude default
        let mut gp = DynamicMessage::new(pt);
        gp.set_field_by_name("latitude", PdValue::F64(1.0));
        gp.set_field_by_name("longitude", PdValue::F64(2.0));
        let mut msg = DynamicMessage::new(fb.clone());
        // name/big_count unset (defaults); area set; nested + repeated set.
        msg.set_field_by_name("area", PdValue::F64(7.0));
        msg.set_field_by_name("top_left", PdValue::Message(top));
        msg.set_field_by_name("geo_points", PdValue::List(vec![PdValue::Message(gp)]));
        let bytes = msg.encode_to_vec();

        let cases = [
            json!({"name": ""}),                                    // unset scalar → default
            json!({"name": "x"}),
            json!({"area": 7.0}),
            json!({"area": 0.0}),
            json!({"bigCount": "0"}),                               // unset int64 → "0"
            json!({"name": {"$exists": true}}),                     // scalar always exists
            json!({"name": {"$exists": false}}),
            json!({"topLeft.latitude": 48.5}),                      // nested set
            json!({"topLeft.longitude": 0.0}),                      // nested unset → default
            json!({"topLeft": {"$exists": true}}),                  // set message present
            json!({"geoPoints": {"$elemMatch": {"latitude": 1.0}}}),
            json!({"geoPoints": {"$elemMatch": {"latitude": 9.0}}}),
            json!({"geoPoints.0.longitude": 2.0}),                  // numeric index into repeated
            json!({"$and": [{"area": 7.0}, {"name": ""}]}),
            json!({"nope": {"$exists": false}}),                    // unknown field
        ];
        for sel in cases {
            assert_eq!(
                matches(sel.clone(), &fb, &bytes),
                ref_matches(&sel, &reg, &bytes),
                "wire matcher disagrees with full decode on {sel}"
            );
        }
    }

    #[test]
    fn unset_singular_message_is_absent() {
        let reg = registry();
        let fb = reg.resolve_full("test.v1.FieldBoundary").unwrap();
        let mut msg = DynamicMessage::new(fb.clone());
        msg.set_field_by_name("name", PdValue::String("n".into())); // top_left unset
        let bytes = msg.encode_to_vec();
        assert!(matches(json!({"topLeft": {"$exists": false}}), &fb, &bytes));
        assert!(!matches(json!({"topLeft": {"$exists": true}}), &fb, &bytes));
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use crate::Registry;
    use couch_mango::Selector;
    use prost::Message as _;
    use std::collections::HashMap as StdHashMap;
    use std::time::Instant;

    fn registry() -> Registry {
        let (reg, _p) = Registry::build(
            &[crate::tests::test_descriptor_set().encode_to_vec()],
            &StdHashMap::new(),
        )
        .unwrap();
        reg
    }

    fn big_boundary_bytes(reg: &Registry, points: usize) -> Vec<u8> {
        let fb = reg.resolve_full("test.v1.FieldBoundary").unwrap();
        let pt = reg.resolve_full("test.v1.Point").unwrap();
        let mut geo = Vec::with_capacity(points);
        for i in 0..points {
            let mut p = DynamicMessage::new(pt.clone());
            p.set_field_by_name("latitude", PdValue::F64(48.0 + i as f64 * 1e-6));
            p.set_field_by_name("longitude", PdValue::F64(11.0 + i as f64 * 1e-6));
            geo.push(PdValue::Message(p));
        }
        let mut msg = DynamicMessage::new(fb);
        msg.set_field_by_name("name", PdValue::String("west field".into()));
        msg.set_field_by_name("geo_points", PdValue::List(geo));
        msg.encode_to_vec()
    }

    // cargo test -p couch-proto --release -- --ignored --nocapture bench
    #[test]
    #[ignore]
    fn find_scan_matching_whole_doc_vs_wire() {
        let reg = registry();
        let fb = reg.resolve_full("test.v1.FieldBoundary").unwrap();
        let sel = Selector::compile(&serde_json::json!({"name": "west field"})).unwrap();
        let iters = 5000usize;
        for &points in &[0usize, 1_000, 10_000] {
            let bytes = big_boundary_bytes(&reg, points);
            eprintln!("--- geo_points={points}  ({} body bytes) ---", bytes.len());

            let t = Instant::now();
            for _ in 0..iters {
                let v = reg.decode_message("test.v1.FieldBoundary", &bytes).unwrap();
                assert!(sel.matches(&v));
            }
            let old = t.elapsed();

            let t = Instant::now();
            for _ in 0..iters {
                assert!(sel.matches(&ProtoDoc::new(fb.clone(), &bytes)));
            }
            let wire = t.elapsed();

            eprintln!(
                "  whole-doc JSON match: {old:?}  ({:.2} µs/doc)",
                old.as_micros() as f64 / iters as f64
            );
            eprintln!(
                "  wire-native match   : {wire:?}  ({:.2} µs/doc)  speedup {:.1}x",
                wire.as_micros() as f64 / iters as f64,
                old.as_secs_f64() / wire.as_secs_f64()
            );
        }
    }
}
