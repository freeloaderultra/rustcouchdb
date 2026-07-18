//! Perf sanity for selective extraction vs full decode, in-memory.
//! Run: cargo run --release -p couch-proto --example extract_bench

use prost::Message as _;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
};
use std::collections::HashMap;
use std::time::Instant;

fn field(name: &str, number: i32, typ: Type, type_name: Option<&str>, repeated: bool) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        r#type: Some(typ as i32),
        type_name: type_name.map(String::from),
        label: Some(if repeated { Label::Repeated } else { Label::Optional } as i32),
        ..Default::default()
    }
}

fn main() {
    // message Rec { Meta db = 1; repeated Point track = 2; }
    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("bench.proto".into()),
            package: Some("bench".into()),
            syntax: Some("proto3".into()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Meta".into()),
                    field: vec![
                        field("OwnerId", 1, Type::String, None, false),
                        field("DocType", 2, Type::String, None, false),
                    ],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Point".into()),
                    field: vec![
                        field("latitude", 1, Type::Double, None, false),
                        field("longitude", 2, Type::Double, None, false),
                    ],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Rec".into()),
                    field: vec![
                        field("db", 1, Type::Message, Some(".bench.Meta"), false),
                        field("track", 2, Type::Message, Some(".bench.Point"), true),
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
    };
    let (reg, _) = couch_proto::Registry::build(&[fds.encode_to_vec()], &HashMap::new()).unwrap();

    // ~50 MB message: tiny db envelope + 2.6M track points.
    let mut bytes = Vec::with_capacity(50 << 20);
    let mut meta = Vec::new();
    meta.extend_from_slice(&[0x0a, 3, b'u', b'-', b'1']); // OwnerId "u-1"
    meta.extend_from_slice(&[0x12, 3, b'r', b'e', b'c']); // DocType "rec"
    bytes.extend_from_slice(&[0x0a, meta.len() as u8]);
    bytes.extend_from_slice(&meta);
    let mut point = Vec::new();
    point.push(0x09);
    point.extend_from_slice(&48.1234f64.to_le_bytes());
    point.push(0x11);
    point.extend_from_slice(&11.5678f64.to_le_bytes());
    let mut framed = vec![0x12, point.len() as u8];
    framed.extend_from_slice(&point);
    while bytes.len() < 50 << 20 {
        bytes.extend_from_slice(&framed);
    }
    println!("message: {:.1} MB", bytes.len() as f64 / 1e6);

    let desc = reg.resolve("rec").unwrap().clone();
    let paths = vec!["db.OwnerId".to_string()];
    let trie = couch_proto::PathTrie::compile(&desc, &paths).unwrap();

    let t = Instant::now();
    let n = 20;
    for _ in 0..n {
        let v = trie
            .extract(&mut couch_proto::SliceReader(&bytes), bytes.len() as u64)
            .unwrap();
        assert_eq!(v["db"]["OwnerId"], serde_json::json!("u-1"));
    }
    let ex = t.elapsed() / n;

    let t = Instant::now();
    let v = reg.decode_doc("rec", &bytes).unwrap();
    assert_eq!(v["db"]["OwnerId"], serde_json::json!("u-1"));
    let full = t.elapsed();

    println!("extract db.OwnerId: {ex:?}   full decode: {full:?}   ({:.0}x)",
        full.as_secs_f64() / ex.as_secs_f64());
}
