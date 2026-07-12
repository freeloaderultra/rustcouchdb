//! Erlang External Term Format codec — exactly the subset CouchDB writes into
//! .couch files (`term_to_binary(T, [{minor_version, 1}])`): integers, floats,
//! atoms, tuples, proper lists, binaries. Decoding additionally accepts the
//! zlib-compressed form (tag 80) and legacy encodings (FLOAT_EXT, latin-1
//! atoms, STRING_EXT).

use crate::error::{corrupt, Error, Result};
use std::cmp::Ordering;
use std::fmt;
use std::io::Read;

pub enum Term {
    Int(i64),
    Float(f64),
    Atom(String),
    Bin(Vec<u8>),
    List(Vec<Term>),
    Tuple(Vec<Term>),
}

// Clone/PartialEq/Drop are hand-written instead of derived: the derived
// impls recurse once per nesting level and rev-tree terms nest once per
// revision, so a deep tree would overflow the stack. Each impl re-checks
// headroom per level via maybe_grow.
impl Clone for Term {
    fn clone(&self) -> Term {
        crate::maybe_grow(|| match self {
            Term::Int(i) => Term::Int(*i),
            Term::Float(x) => Term::Float(*x),
            Term::Atom(a) => Term::Atom(a.clone()),
            Term::Bin(b) => Term::Bin(b.clone()),
            Term::List(v) => Term::List(v.clone()),
            Term::Tuple(v) => Term::Tuple(v.clone()),
        })
    }
}

impl PartialEq for Term {
    fn eq(&self, other: &Term) -> bool {
        crate::maybe_grow(|| match (self, other) {
            (Term::Int(a), Term::Int(b)) => a == b,
            (Term::Float(a), Term::Float(b)) => a == b,
            (Term::Atom(a), Term::Atom(b)) => a == b,
            (Term::Bin(a), Term::Bin(b)) => a == b,
            (Term::List(a), Term::List(b)) => a == b,
            (Term::Tuple(a), Term::Tuple(b)) => a == b,
            _ => false,
        })
    }
}

impl Drop for Term {
    fn drop(&mut self) {
        match self {
            Term::List(v) | Term::Tuple(v) => {
                if !v.is_empty() {
                    let v = std::mem::take(v);
                    crate::maybe_grow(|| drop(v));
                }
            }
            _ => {}
        }
    }
}

impl Term {
    pub fn atom(s: &str) -> Term {
        Term::Atom(s.to_string())
    }

    pub fn nil() -> Term {
        Term::Atom("nil".into())
    }

    pub fn is_atom(&self, name: &str) -> bool {
        matches!(self, Term::Atom(a) if a == name)
    }

    pub fn as_i64(&self) -> Result<i64> {
        match self {
            Term::Int(i) => Ok(*i),
            _ => Err(corrupt(format!("expected integer, got {self:?}"))),
        }
    }

    pub fn as_u64(&self) -> Result<u64> {
        let i = self.as_i64()?;
        u64::try_from(i).map_err(|_| corrupt(format!("expected non-negative integer, got {i}")))
    }

    pub fn as_bin(&self) -> Result<&[u8]> {
        match self {
            Term::Bin(b) => Ok(b),
            _ => Err(corrupt(format!("expected binary, got {self:?}"))),
        }
    }

    pub fn as_list(&self) -> Result<&[Term]> {
        match self {
            Term::List(l) => Ok(l),
            _ => Err(corrupt(format!("expected list, got {self:?}"))),
        }
    }

    pub fn as_tuple(&self) -> Result<&[Term]> {
        match self {
            Term::Tuple(t) => Ok(t),
            _ => Err(corrupt(format!("expected tuple, got {self:?}"))),
        }
    }

    pub fn tuple_n(&self, n: usize) -> Result<&[Term]> {
        let t = self.as_tuple()?;
        if t.len() != n {
            return Err(corrupt(format!("expected {n}-tuple, got {self:?}")));
        }
        Ok(t)
    }
}

impl fmt::Debug for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        crate::maybe_grow(|| match self {
            Term::Int(i) => write!(f, "{i}"),
            Term::Float(x) => write!(f, "{x}"),
            Term::Atom(a) => write!(f, "'{a}'"),
            Term::Bin(b) => match std::str::from_utf8(b) {
                Ok(s) if b.len() <= 64 => write!(f, "<<{s:?}>>"),
                _ => write!(f, "<<{} bytes>>", b.len()),
            },
            Term::List(l) => f.debug_list().entries(l).finish(),
            Term::Tuple(t) => {
                write!(f, "{{")?;
                for (i, e) in t.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{e:?}")?;
                }
                write!(f, "}}")
            }
        })
    }
}

/// Erlang term order, restricted to the types we model:
/// number < atom < tuple < list < binary.
pub fn cmp(a: &Term, b: &Term) -> Ordering {
    fn rank(t: &Term) -> u8 {
        match t {
            Term::Int(_) | Term::Float(_) => 0,
            Term::Atom(_) => 1,
            Term::Tuple(_) => 2,
            Term::List(_) => 3,
            Term::Bin(_) => 4,
        }
    }
    crate::maybe_grow(|| match (a, b) {
        (Term::Int(x), Term::Int(y)) => x.cmp(y),
        (Term::Int(x), Term::Float(y)) => cmp_f64(*x as f64, *y),
        (Term::Float(x), Term::Int(y)) => cmp_f64(*x, *y as f64),
        (Term::Float(x), Term::Float(y)) => cmp_f64(*x, *y),
        (Term::Atom(x), Term::Atom(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Term::Tuple(x), Term::Tuple(y)) => match x.len().cmp(&y.len()) {
            Ordering::Equal => cmp_seq(x, y),
            o => o,
        },
        (Term::List(x), Term::List(y)) => cmp_seq(x, y),
        (Term::Bin(x), Term::Bin(y)) => x.cmp(y),
        _ => rank(a).cmp(&rank(b)),
    })
}

fn cmp_f64(x: f64, y: f64) -> Ordering {
    x.partial_cmp(&y).unwrap_or(Ordering::Equal)
}

fn cmp_seq(x: &[Term], y: &[Term]) -> Ordering {
    for (a, b) in x.iter().zip(y.iter()) {
        match cmp(a, b) {
            Ordering::Equal => continue,
            o => return o,
        }
    }
    x.len().cmp(&y.len())
}

// ---------------------------------------------------------------- decoding

pub fn decode(buf: &[u8]) -> Result<Term> {
    if buf.first() != Some(&131) {
        return Err(corrupt("ETF data missing version byte 131"));
    }
    if buf.get(1) == Some(&80) {
        // Compressed: <<131, 80, UncompressedSize:32, ZlibData>>
        if buf.len() < 6 {
            return Err(corrupt("truncated compressed ETF"));
        }
        let size = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]) as usize;
        let mut inner = Vec::with_capacity(size);
        flate2::read::ZlibDecoder::new(&buf[6..])
            .read_to_end(&mut inner)
            .map_err(|e| corrupt(format!("zlib inflate failed: {e}")))?;
        let mut r = Reader { buf: &inner, pos: 0 };
        let t = r.term()?;
        return Ok(t);
    }
    let mut r = Reader { buf, pos: 1 };
    let t = r.term()?;
    Ok(t)
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(corrupt("truncated ETF term"));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn term(&mut self) -> Result<Term> {
        crate::maybe_grow(|| self.term_inner())
    }

    fn term_inner(&mut self) -> Result<Term> {
        let tag = self.u8()?;
        match tag {
            97 => Ok(Term::Int(self.u8()? as i64)),
            98 => {
                let b = self.take(4)?;
                Ok(Term::Int(i32::from_be_bytes([b[0], b[1], b[2], b[3]]) as i64))
            }
            70 => {
                let b = self.take(8)?;
                Ok(Term::Float(f64::from_be_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ])))
            }
            99 => {
                // FLOAT_EXT: 31-byte zero-padded printf %.20e
                let b = self.take(31)?;
                let s = std::str::from_utf8(b)
                    .map_err(|_| corrupt("bad FLOAT_EXT"))?
                    .trim_end_matches('\0');
                s.parse::<f64>()
                    .map(Term::Float)
                    .map_err(|_| corrupt("bad FLOAT_EXT value"))
            }
            100 | 118 => {
                let n = self.u16()? as usize;
                let b = self.take(n)?;
                atom_from_bytes(tag, b)
            }
            115 | 119 => {
                let n = self.u8()? as usize;
                let b = self.take(n)?;
                atom_from_bytes(tag, b)
            }
            104 => {
                let n = self.u8()? as usize;
                self.seq(n).map(Term::Tuple)
            }
            105 => {
                let n = self.u32()? as usize;
                self.seq(n).map(Term::Tuple)
            }
            106 => Ok(Term::List(Vec::new())),
            107 => {
                // STRING_EXT: a list of integers 0..255
                let n = self.u16()? as usize;
                let b = self.take(n)?;
                Ok(Term::List(b.iter().map(|&c| Term::Int(c as i64)).collect()))
            }
            108 => {
                let n = self.u32()? as usize;
                let elems = self.seq(n)?;
                let tail = self.term()?;
                if !matches!(&tail, Term::List(l) if l.is_empty()) {
                    return Err(Error::Unsupported("improper list in ETF".into()));
                }
                Ok(Term::List(elems))
            }
            109 => {
                let n = self.u32()? as usize;
                Ok(Term::Bin(self.take(n)?.to_vec()))
            }
            110 | 111 => {
                let n = if tag == 110 {
                    self.u8()? as usize
                } else {
                    self.u32()? as usize
                };
                let sign = self.u8()?;
                let digits = self.take(n)?;
                if n > 8 {
                    return Err(Error::Unsupported("big integer exceeds 64 bits".into()));
                }
                let mut mag: u128 = 0;
                for (i, &d) in digits.iter().enumerate() {
                    mag |= (d as u128) << (8 * i);
                }
                let val = if sign == 0 { mag as i128 } else { -(mag as i128) };
                i64::try_from(val)
                    .map(Term::Int)
                    .map_err(|_| Error::Unsupported("big integer exceeds i64".into()))
            }
            t => Err(Error::Unsupported(format!("ETF tag {t} not supported"))),
        }
    }

    fn seq(&mut self, n: usize) -> Result<Vec<Term>> {
        let mut v = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            v.push(self.term()?);
        }
        Ok(v)
    }
}

fn atom_from_bytes(tag: u8, b: &[u8]) -> Result<Term> {
    // 100/115 are latin-1, 118/119 are UTF-8. CouchDB atoms are ASCII either way.
    let s = if tag == 118 || tag == 119 {
        String::from_utf8(b.to_vec()).map_err(|_| corrupt("bad UTF-8 atom"))?
    } else {
        b.iter().map(|&c| c as char).collect()
    };
    Ok(Term::Atom(s))
}

// ---------------------------------------------------------------- encoding

pub fn encode(t: &Term) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(131);
    enc(t, &mut out);
    out
}

/// Byte size of the encoded term without the version byte — the moral
/// equivalent of `erlang:external_size/1`, used for btree chunk thresholds.
pub fn external_size(t: &Term) -> usize {
    let mut out = Vec::with_capacity(64);
    enc(t, &mut out);
    out.len()
}

fn enc(t: &Term, out: &mut Vec<u8>) {
    crate::maybe_grow(|| match t {
        Term::Int(i) => {
            let i = *i;
            if (0..=255).contains(&i) {
                out.push(97);
                out.push(i as u8);
            } else if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                out.push(98);
                out.extend_from_slice(&(i as i32).to_be_bytes());
            } else {
                // SMALL_BIG_EXT
                let (sign, mag) = if i < 0 {
                    (1u8, (i as i128).unsigned_abs() as u128)
                } else {
                    (0u8, i as u128)
                };
                let mut digits = Vec::new();
                let mut m = mag;
                while m > 0 {
                    digits.push((m & 0xff) as u8);
                    m >>= 8;
                }
                out.push(110);
                out.push(digits.len() as u8);
                out.push(sign);
                out.extend_from_slice(&digits);
            }
        }
        Term::Float(x) => {
            out.push(70);
            out.extend_from_slice(&x.to_be_bytes());
        }
        Term::Atom(a) => {
            debug_assert!(a.len() <= 255);
            out.push(119); // SMALL_ATOM_UTF8_EXT
            out.push(a.len() as u8);
            out.extend_from_slice(a.as_bytes());
        }
        Term::Bin(b) => {
            out.push(109);
            out.extend_from_slice(&(b.len() as u32).to_be_bytes());
            out.extend_from_slice(b);
        }
        Term::List(l) => {
            if l.is_empty() {
                out.push(106);
            } else {
                out.push(108);
                out.extend_from_slice(&(l.len() as u32).to_be_bytes());
                for e in l {
                    enc(e, out);
                }
                out.push(106);
            }
        }
        Term::Tuple(t) => {
            if t.len() <= 255 {
                out.push(104);
                out.push(t.len() as u8);
            } else {
                out.push(105);
                out.extend_from_slice(&(t.len() as u32).to_be_bytes());
            }
            for e in t {
                enc(e, out);
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(t: &Term) {
        let enc = encode(t);
        let dec = decode(&enc).unwrap();
        assert!(*t == dec, "{t:?} != {dec:?}");
    }

    #[test]
    fn roundtrips() {
        roundtrip(&Term::Int(0));
        roundtrip(&Term::Int(255));
        roundtrip(&Term::Int(-1));
        roundtrip(&Term::Int(1 << 40));
        roundtrip(&Term::Int(i64::MAX));
        roundtrip(&Term::Int(i64::MIN + 1));
        roundtrip(&Term::Float(1.5));
        roundtrip(&Term::atom("db_header"));
        roundtrip(&Term::Bin(vec![1, 2, 3]));
        roundtrip(&Term::List(vec![]));
        roundtrip(&Term::List(vec![Term::Int(1), Term::Bin(vec![9])]));
        roundtrip(&Term::Tuple(vec![
            Term::atom("kv_node"),
            Term::List(vec![Term::Tuple(vec![
                Term::Bin(b"doc1".to_vec()),
                Term::Int(42),
            ])]),
        ]));
    }

    #[test]
    fn decodes_string_ext_as_int_list() {
        // term_to_binary("abc") = <<131,107,0,3,"abc">>
        let buf = [131u8, 107, 0, 3, b'a', b'b', b'c'];
        let t = decode(&buf).unwrap();
        assert!(
            t == Term::List(vec![Term::Int(97), Term::Int(98), Term::Int(99)]),
            "{t:?}"
        );
    }

    #[test]
    fn decodes_latin1_atoms() {
        // <<131,100,0,3,"nil">>
        let buf = [131u8, 100, 0, 3, b'n', b'i', b'l'];
        assert!(decode(&buf).unwrap().is_atom("nil"));
    }

    #[test]
    fn term_order() {
        use std::cmp::Ordering::*;
        assert_eq!(cmp(&Term::Int(1), &Term::Int(2)), Less);
        assert_eq!(cmp(&Term::Int(5), &Term::Bin(vec![0])), Less);
        assert_eq!(cmp(&Term::atom("a"), &Term::Bin(vec![])), Less);
        assert_eq!(
            cmp(&Term::Bin(b"abc".to_vec()), &Term::Bin(b"abcd".to_vec())),
            Less
        );
        assert_eq!(cmp(&Term::Int(1), &Term::Float(1.0)), Equal);
    }
}
