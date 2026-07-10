//! Order-preserving index key encoding.
//!
//! Index keys are stored as ETF terms in a couch-store btree, which compares
//! by Erlang term order. To make that order equal CouchDB's EJSON collation
//! (null < false < true < number < string < array < object), each JSON value
//! is wrapped as `{Rank, Payload}`: the 2-tuples compare by rank first, and
//! payloads of equal rank compare correctly under term order (numbers
//! numerically, strings as UTF-8 binaries = codepoint order, arrays/objects
//! recursively). Min/Max sentinels bound range scans.

use couch_store::etf::Term;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq)]
pub enum Bound {
    Min,
    Val(Value),
    Max,
}

pub fn encode_bound(b: &Bound) -> Term {
    // Same arity as encoded values (term order compares tuple arity first),
    // with ranks outside the 0..=6 range real values use.
    match b {
        Bound::Min => Term::Tuple(vec![Term::Int(-1), Term::Int(0)]),
        Bound::Val(v) => encode_value(v),
        Bound::Max => Term::Tuple(vec![Term::Int(100), Term::Int(0)]),
    }
}

pub fn encode_value(v: &Value) -> Term {
    let (rank, payload) = match v {
        Value::Null => (0, Term::Int(0)),
        Value::Bool(false) => (1, Term::Int(0)),
        Value::Bool(true) => (2, Term::Int(0)),
        Value::Number(n) => (
            3,
            if let Some(i) = n.as_i64() {
                Term::Int(i)
            } else {
                Term::Float(n.as_f64().unwrap_or(0.0))
            },
        ),
        Value::String(s) => (4, Term::Bin(s.as_bytes().to_vec())),
        Value::Array(a) => (5, Term::List(a.iter().map(encode_value).collect())),
        Value::Object(o) => (
            6,
            Term::List(
                o.iter()
                    .map(|(k, val)| {
                        Term::Tuple(vec![
                            Term::Bin(k.as_bytes().to_vec()),
                            encode_value(val),
                        ])
                    })
                    .collect(),
            ),
        ),
    };
    Term::Tuple(vec![Term::Int(rank), payload])
}

/// Composite btree key: (encoded column values, doc id). The doc id breaks
/// ties so equal-valued rows are distinct keys.
pub fn btree_key(cols: &[Value], docid: &[u8]) -> Term {
    Term::Tuple(vec![
        Term::List(cols.iter().map(encode_value).collect()),
        Term::Bin(docid.to_vec()),
    ])
}

/// Range-scan bound: encoded bound values plus an id sentinel (empty id
/// sorts before every real id; Max id sentinel after).
pub fn bound_key(cols: &[Bound], max_id: bool) -> Term {
    Term::Tuple(vec![
        Term::List(cols.iter().map(encode_bound).collect()),
        if max_id {
            // Doc ids are UTF-8, so they never contain 0xFF bytes; this
            // binary sorts after every real id (binaries are the highest
            // rank in our term order).
            Term::Bin(vec![0xff; 4])
        } else {
            Term::Bin(Vec::new())
        },
    ])
}

/// Split a stored btree key back into (encoded column list, doc id).
pub fn decode_btree_key(t: &Term) -> Option<(&[Term], &[u8])> {
    let tup = t.as_tuple().ok()?;
    if tup.len() != 2 {
        return None;
    }
    let cols = tup[0].as_list().ok()?;
    let id = tup[1].as_bin().ok()?;
    Some((cols, id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use couch_mango::collate;
    use couch_store::etf::cmp;
    use serde_json::json;
    use std::cmp::Ordering;

    #[test]
    fn encoding_preserves_collation() {
        let values = vec![
            json!(null),
            json!(false),
            json!(true),
            json!(-10),
            json!(0),
            json!(1.5),
            json!(2),
            json!(1e100),
            json!(""),
            json!("a"),
            json!("aa"),
            json!("b"),
            json!("ä"),
            json!([]),
            json!([1]),
            json!([1, 2]),
            json!([2]),
            json!(["x", null]),
            json!({}),
            json!({"a": 1}),
            json!({"a": 1, "b": 2}),
            json!({"a": 2}),
            json!({"b": 1}),
        ];
        for a in &values {
            for b in &values {
                let expected = collate(a, b);
                let got = cmp(&encode_value(a), &encode_value(b));
                assert_eq!(expected, got, "collation mismatch for {a} vs {b}");
            }
        }
    }

    #[test]
    fn sentinels_bound_everything() {
        let vals = [json!(null), json!(1e308), json!({"z": "z"}), json!("x")];
        for v in &vals {
            assert_eq!(
                cmp(&encode_bound(&Bound::Min), &encode_value(v)),
                Ordering::Less
            );
            assert_eq!(
                cmp(&encode_bound(&Bound::Max), &encode_value(v)),
                Ordering::Greater
            );
        }
    }

    #[test]
    fn composite_key_id_tiebreak() {
        let k1 = btree_key(&[json!(1)], b"a");
        let k2 = btree_key(&[json!(1)], b"b");
        let k3 = btree_key(&[json!(2)], b"a");
        assert_eq!(cmp(&k1, &k2), Ordering::Less);
        assert_eq!(cmp(&k2, &k3), Ordering::Less);
        // range bounds embrace all ids of the same column values
        let lo = bound_key(&[Bound::Val(json!(1))], false);
        let hi = bound_key(&[Bound::Val(json!(1))], true);
        assert_eq!(cmp(&lo, &k1), Ordering::Less);
        assert_eq!(cmp(&k2, &hi), Ordering::Less);
        assert_eq!(cmp(&k3, &hi), Ordering::Greater);
    }
}
