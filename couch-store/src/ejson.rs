//! EJSON: CouchDB's Erlang representation of JSON.
//! Objects are `{[{KeyBin, Value}]}` (a 1-tuple holding a proplist),
//! arrays are lists, strings are binaries, `true`/`false`/`null` are atoms.

use crate::error::{corrupt, Result};
use crate::etf::Term;
use serde_json::{Map, Number, Value};

pub fn to_json(t: &Term) -> Result<Value> {
    crate::maybe_grow(|| to_json_inner(t))
}

fn to_json_inner(t: &Term) -> Result<Value> {
    Ok(match t {
        Term::Atom(a) => match a.as_str() {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            "null" => Value::Null,
            other => return Err(corrupt(format!("unexpected atom in EJSON: '{other}'"))),
        },
        Term::Int(i) => Value::Number((*i).into()),
        Term::Float(f) => Value::Number(
            Number::from_f64(*f).ok_or_else(|| corrupt("non-finite float in EJSON"))?,
        ),
        Term::Bin(b) => Value::String(
            String::from_utf8(b.clone()).map_err(|_| corrupt("non-UTF-8 string in EJSON"))?,
        ),
        Term::List(l) => Value::Array(l.iter().map(to_json).collect::<Result<_>>()?),
        Term::Tuple(tup) if tup.len() == 1 => {
            let mut map = Map::new();
            for pair in tup[0].as_list()? {
                let kv = pair.tuple_n(2)?;
                let key = String::from_utf8(kv[0].as_bin()?.to_vec())
                    .map_err(|_| corrupt("non-UTF-8 object key"))?;
                map.insert(key, to_json(&kv[1])?);
            }
            Value::Object(map)
        }
        other => return Err(corrupt(format!("unexpected term in EJSON: {other:?}"))),
    })
}

pub fn from_json(v: &Value) -> Term {
    crate::maybe_grow(|| from_json_inner(v))
}

fn from_json_inner(v: &Value) -> Term {
    match v {
        Value::Null => Term::atom("null"),
        Value::Bool(true) => Term::atom("true"),
        Value::Bool(false) => Term::atom("false"),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Term::Int(i)
            } else {
                Term::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => Term::Bin(s.as_bytes().to_vec()),
        Value::Array(a) => Term::List(a.iter().map(from_json).collect()),
        Value::Object(o) => Term::Tuple(vec![Term::List(
            o.iter()
                .map(|(k, v)| {
                    Term::Tuple(vec![Term::Bin(k.as_bytes().to_vec()), from_json(v)])
                })
                .collect(),
        )]),
    }
}

/// The JSON-serialized byte size — stands in for couch_ejson_size:encoded_size
/// when computing a leaf's "external" size.
pub fn external_size(v: &Value) -> usize {
    serde_json::to_string(v).map(|s| s.len()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let v: Value = serde_json::from_str(
            r#"{"a": 1, "b": [1, 2, 300, "x", null, true], "c": {"d": 1.5, "e": ""}, "f": false}"#,
        )
        .unwrap();
        let t = from_json(&v);
        let back = to_json(&t).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn array_of_small_ints_via_string_ext() {
        // decode(encode([1,2,3] as STRING_EXT)) must produce a JSON array
        let buf = [131u8, 107, 0, 3, 1, 2, 3];
        let t = crate::etf::decode(&buf).unwrap();
        let v = to_json(&t).unwrap();
        assert_eq!(v, serde_json::json!([1, 2, 3]));
    }
}
