//! Schema registry and dynamic protobuf decode for proto-aware Mango.
//!
//! Applications like nxguide store their heavy payloads as "blob documents":
//! a small JSON head (replication/query metadata under `db.*`) plus one
//! attachment of content-type `application/protobuf` holding the whole
//! message. With a registered `FileDescriptorSet`, the server can decode
//! those attachments and let Mango selectors and indexes reach fields inside
//! them — without touching the stored bytes.
//!
//! Decoding follows protojson conventions (the encoding the same apps use
//! for their non-blob JSON documents): field keys use the descriptor's JSON
//! names, 64-bit integers serialize as strings, and unset/default fields are
//! omitted. That keeps blob-interior paths spelled exactly like their
//! JSON-doc counterparts (`boundingBox.topLeft.latitude`).

pub mod extract;
pub use extract::{PathTrie, SkipRead, SliceReader};

use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use serde_json::Value;
use std::collections::HashMap;

/// The attachment content types the registry will attempt to decode.
pub fn is_proto_content_type(ct: &str) -> bool {
    let mime = ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
    mime == "application/protobuf" || mime == "application/x-protobuf"
}

/// A set of registered message descriptors, addressable by the `db.DocType`
/// convention (snake_case of the message short name) or an explicit mapping.
pub struct Registry {
    by_doctype: HashMap<String, MessageDescriptor>,
}

impl Registry {
    /// Build a registry from encoded `FileDescriptorSet`s plus explicit
    /// doctype→full-name overrides. Sets may arrive in any order and may
    /// share dependency files (duplicates are skipped by the pool); a set
    /// whose dependencies live in another set is retried after the rest.
    ///
    /// Structural failures — bytes that aren't a `FileDescriptorSet`, or a
    /// set whose dependencies no other set provides — are hard errors: a
    /// registry must never be silently built from part of its inputs. The
    /// returned strings are advisories (doctype naming collisions, override
    /// targets not present) that don't affect what was registered.
    pub fn build(
        sets: &[Vec<u8>],
        overrides: &HashMap<String, String>,
    ) -> Result<(Registry, Vec<String>), String> {
        let mut problems = Vec::new();
        let mut pool = DescriptorPool::new();
        let mut pending: Vec<prost_types::FileDescriptorSet> = Vec::new();
        for (i, bytes) in sets.iter().enumerate() {
            let fds = prost_types::FileDescriptorSet::decode(bytes.as_slice())
                .map_err(|e| format!("descriptor set #{i}: not a FileDescriptorSet: {e}"))?;
            pending.push(fds);
        }
        // Cross-set dependencies resolve in <= sets.len() passes.
        loop {
            let mut progressed = false;
            let mut still: Vec<(prost_types::FileDescriptorSet, String)> = Vec::new();
            for fds in pending {
                match pool.add_file_descriptor_set(fds.clone()) {
                    Ok(()) => progressed = true,
                    Err(e) => still.push((fds, e.to_string())),
                }
            }
            if still.is_empty() {
                break;
            }
            if !progressed {
                let reasons: Vec<String> = still.into_iter().map(|(_, e)| e).collect();
                return Err(format!("descriptor set(s) rejected: {}", reasons.join("; ")));
            }
            pending = still.into_iter().map(|(fds, _)| fds).collect();
        }

        let mut by_doctype: HashMap<String, MessageDescriptor> = HashMap::new();
        for msg in pool.all_messages() {
            let key = snake_case(msg.name());
            // First registration wins; a collision across packages is
            // reported so the app can disambiguate with an override.
            if let Some(prev) = by_doctype.get(&key) {
                if prev.full_name() != msg.full_name() {
                    problems.push(format!(
                        "doctype {key:?} is ambiguous ({} vs {}); use an explicit doctypes mapping",
                        prev.full_name(),
                        msg.full_name()
                    ));
                }
                continue;
            }
            by_doctype.insert(key, msg);
        }
        for (doctype, full_name) in overrides {
            match pool.get_message_by_name(full_name) {
                Some(msg) => {
                    by_doctype.insert(doctype.clone(), msg);
                }
                None => problems.push(format!(
                    "doctypes mapping {doctype:?} -> {full_name:?}: no such message in the registered descriptors"
                )),
            }
        }
        Ok((Registry { by_doctype }, problems))
    }

    pub fn is_empty(&self) -> bool {
        self.by_doctype.is_empty()
    }

    pub fn len(&self) -> usize {
        self.by_doctype.len()
    }

    pub fn resolve(&self, doctype: &str) -> Option<&MessageDescriptor> {
        self.by_doctype.get(doctype)
    }

    /// Decode message bytes for `doctype` into a protojson-shaped JSON value.
    pub fn decode_doc(&self, doctype: &str, bytes: &[u8]) -> Result<Value, String> {
        let desc = self
            .resolve(doctype)
            .ok_or_else(|| format!("no schema registered for doctype {doctype:?}"))?;
        let msg = DynamicMessage::decode(desc.clone(), bytes)
            .map_err(|e| format!("cannot decode {doctype:?} as {}: {e}", desc.full_name()))?;
        serde_json::to_value(&msg).map_err(|e| format!("cannot JSON-encode {doctype:?}: {e}"))
    }
}

/// Cheap well-formedness check for a single descriptor-set upload: the
/// bytes must decode as a `FileDescriptorSet`. Dependency resolution is
/// deliberately NOT checked here — a set may depend on files provided by a
/// different `_schemas` doc; that resolves (or errors, loudly) at registry
/// build time.
pub fn validate_descriptor_set(bytes: &[u8]) -> Result<(), String> {
    prost_types::FileDescriptorSet::decode(bytes)
        .map(|_| ())
        .map_err(|e| format!("not a protobuf FileDescriptorSet: {e}"))
}

/// The `db.DocType` naming convention: snake_case of the message short name
/// (`FieldBoundary` -> `field_boundary`).
pub fn snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// The augmented view of a blob document: the decoded message's fields with
/// every top-level key of the stored head document overlaid on top. The head
/// is authoritative (it carries `_id`/`_rev`/`_attachments` and the app may
/// treat its `db` metadata as the source of truth); decoded fields only add
/// what the head doesn't have.
pub fn overlay(decoded: Value, head: &Value) -> Value {
    let Value::Object(mut base) = decoded else {
        return head.clone();
    };
    if let Value::Object(h) = head {
        for (k, v) in h {
            base.insert(k.clone(), v.clone());
        }
    } else {
        return head.clone();
    }
    Value::Object(base)
}

/// Convenience for callers deciding whether a doc is worth augmenting:
/// the first attachment stub in `doc` with a protobuf content type, paired
/// with the doc's `db.DocType`, if both exist.
pub fn blob_candidate(doc: &Value) -> Option<(&str, &str, u64)> {
    let doctype = doc.get("db")?.get("DocType")?.as_str()?;
    let atts = doc.get("_attachments")?.as_object()?;
    for (name, att) in atts {
        let ct = att.get("content_type").and_then(|c| c.as_str()).unwrap_or("");
        if is_proto_content_type(ct) {
            let len = att.get("length").and_then(|l| l.as_u64()).unwrap_or(0);
            return Some((name.as_str(), doctype, len));
        }
    }
    None
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use prost_reflect::Value as PbValue;
    use prost_types::{
        field_descriptor_proto, DescriptorProto, FieldDescriptorProto, FileDescriptorProto,
        FileDescriptorSet,
    };
    use serde_json::json;

    fn field(
        name: &str,
        number: i32,
        typ: field_descriptor_proto::Type,
        type_name: Option<&str>,
        repeated: bool,
    ) -> FieldDescriptorProto {
        FieldDescriptorProto {
            name: Some(name.into()),
            number: Some(number),
            r#type: Some(typ as i32),
            type_name: type_name.map(String::from),
            label: Some(if repeated {
                field_descriptor_proto::Label::Repeated as i32
            } else {
                field_descriptor_proto::Label::Optional as i32
            }),
            ..Default::default()
        }
    }

    /// message Point { double latitude = 1; double longitude = 2; }
    /// message FieldBoundary { string name = 1; double area = 2;
    ///                         repeated Point geo_points = 3; int64 big_count = 4;
    ///                         Point top_left = 5; }
    pub(crate) fn test_descriptor_set() -> FileDescriptorSet {
        use field_descriptor_proto::Type;
        let point = DescriptorProto {
            name: Some("Point".into()),
            field: vec![
                field("latitude", 1, Type::Double, None, false),
                field("longitude", 2, Type::Double, None, false),
            ],
            ..Default::default()
        };
        let boundary = DescriptorProto {
            name: Some("FieldBoundary".into()),
            field: vec![
                field("name", 1, Type::String, None, false),
                field("area", 2, Type::Double, None, false),
                field("geo_points", 3, Type::Message, Some(".test.v1.Point"), true),
                field("big_count", 4, Type::Int64, None, false),
                field("top_left", 5, Type::Message, Some(".test.v1.Point"), false),
            ],
            ..Default::default()
        };
        FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("test/v1/test.proto".into()),
                package: Some("test.v1".into()),
                message_type: vec![point, boundary],
                syntax: Some("proto3".into()),
                ..Default::default()
            }],
        }
    }

    pub(crate) fn encode_boundary(area: f64, lat: f64, lon: f64, big: i64) -> Vec<u8> {
        let (reg, problems) =
            Registry::build(&[test_descriptor_set().encode_to_vec()], &HashMap::new()).unwrap();
        assert!(problems.is_empty(), "{problems:?}");
        let desc = reg.resolve("field_boundary").unwrap().clone();
        let point = reg.resolve("point").unwrap().clone();
        let mut tl = DynamicMessage::new(point.clone());
        tl.set_field_by_name("latitude", PbValue::F64(lat));
        tl.set_field_by_name("longitude", PbValue::F64(lon));
        let mut p2 = DynamicMessage::new(point);
        p2.set_field_by_name("latitude", PbValue::F64(lat + 1.0));
        p2.set_field_by_name("longitude", PbValue::F64(lon + 1.0));
        let mut msg = DynamicMessage::new(desc);
        msg.set_field_by_name("name", PbValue::String("west field".into()));
        msg.set_field_by_name("area", PbValue::F64(area));
        msg.set_field_by_name(
            "geo_points",
            PbValue::List(vec![PbValue::Message(tl.clone()), PbValue::Message(p2)]),
        );
        msg.set_field_by_name("big_count", PbValue::I64(big));
        msg.set_field_by_name("top_left", PbValue::Message(tl));
        msg.encode_to_vec()
    }

    #[test]
    fn snake_case_matches_doctype_convention() {
        assert_eq!(snake_case("FieldBoundary"), "field_boundary");
        assert_eq!(snake_case("SurfaceElevationPoints"), "surface_elevation_points");
        assert_eq!(snake_case("GlobalPathElements"), "global_path_elements");
        assert_eq!(snake_case("WorkedAreaBlob"), "worked_area_blob");
        assert_eq!(snake_case("DataRecord"), "data_record");
        assert_eq!(snake_case("LogCaptureBundle"), "log_capture_bundle");
        assert_eq!(snake_case("field"), "field");
    }

    #[test]
    fn decode_uses_protojson_conventions() {
        let (reg, problems) =
            Registry::build(&[test_descriptor_set().encode_to_vec()], &HashMap::new()).unwrap();
        assert!(problems.is_empty(), "{problems:?}");
        let bytes = encode_boundary(1234.5, 48.1, 11.5, 9007199254740993);
        let v = reg.decode_doc("field_boundary", &bytes).unwrap();
        // snake_case proto field -> lowerCamelCase JSON name
        assert_eq!(v["geoPoints"][0]["latitude"], json!(48.1));
        assert_eq!(v["topLeft"]["longitude"], json!(11.5));
        assert_eq!(v["area"], json!(1234.5));
        // int64 stringifies, exactly like protojson
        assert_eq!(v["bigCount"], json!("9007199254740993"));
        // unknown doctype
        assert!(reg.decode_doc("no_such_type", &bytes).is_err());
        // garbage bytes: a length-delimited field running past the end
        assert!(reg
            .decode_doc("field_boundary", &[0x1a, 0xff, 0x01, 0x00])
            .is_err());
    }

    #[test]
    fn overrides_beat_convention() {
        let mut ov = HashMap::new();
        ov.insert("legacy_boundary".to_string(), "test.v1.FieldBoundary".to_string());
        ov.insert("missing".to_string(), "test.v1.Nope".to_string());
        let (reg, problems) = Registry::build(&[test_descriptor_set().encode_to_vec()], &ov).unwrap();
        assert!(reg.resolve("legacy_boundary").is_some());
        assert!(reg.resolve("field_boundary").is_some());
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("missing"));
    }

    #[test]
    fn overlay_head_wins() {
        let decoded = json!({"area": 1.0, "name": "from-blob", "db": {"DocType": "x"}});
        let head = json!({
            "_id": "d1", "_rev": "1-a",
            "db": {"DocType": "field_boundary", "OwnerId": "u1"},
            "_attachments": {"blob.data": {"length": 3}},
        });
        let v = overlay(decoded, &head);
        assert_eq!(v["_id"], json!("d1"));
        assert_eq!(v["area"], json!(1.0));
        assert_eq!(v["name"], json!("from-blob"));
        // head's db object replaces the decoded one wholesale
        assert_eq!(v["db"]["OwnerId"], json!("u1"));
        assert_eq!(v["db"]["DocType"], json!("field_boundary"));
    }

    #[test]
    fn blob_candidate_finds_proto_attachment() {
        let doc = json!({
            "_id": "d", "db": {"DocType": "field_boundary"},
            "_attachments": {
                "photo.jpg": {"content_type": "image/jpeg", "length": 10},
                "blob.data": {"content_type": "application/protobuf", "length": 42},
            }
        });
        let (name, doctype, len) = blob_candidate(&doc).unwrap();
        assert_eq!((name, doctype, len), ("blob.data", "field_boundary", 42));
        assert!(blob_candidate(&json!({"_id": "d"})).is_none());
        assert!(blob_candidate(&json!({
            "db": {"DocType": "x"},
            "_attachments": {"a.bin": {"content_type": "application/octet-stream"}}
        }))
        .is_none());
    }
}
