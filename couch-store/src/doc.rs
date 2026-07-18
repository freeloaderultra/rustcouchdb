//! Document summaries and attachment metadata.
//!
//! A stored revision's `ptr` points at a checksummed chunk holding
//! `term_to_binary({Body, Atts})` where Body and Atts are each either an
//! (individually compressed) binary or a direct term. Body decompresses to
//! the EJSON body; Atts to a list of attachment disk terms.

use crate::compress;
use crate::error::{corrupt, Error, Result};
use crate::etf::{self, Term};
use crate::file::CouchFile;
use base64::Engine;
use serde_json::{Map, Value};

pub struct Summary {
    pub body: Term,
    pub atts: Vec<AttInfo>,
}

#[derive(Clone, Debug)]
pub struct AttInfo {
    pub name: String,
    pub content_type: String,
    /// Stream chunks: (pos, len). Legacy files may store bare positions
    /// (len = 0 marks "unknown, read the chunk to find out").
    pub chunks: Vec<(u64, Option<u64>)>,
    pub att_len: u64,
    pub disk_len: u64,
    pub revpos: u64,
    pub md5: Vec<u8>,
    pub encoding: String, // "identity" | "gzip"
}

/// A stored document body: either the classic EJSON term or a protobuf
/// message (`{pb, TypeName, Bytes}` in the summary body slot — a
/// rustcouchdb extension; stock CouchDB cannot interpret these files).
pub enum DocBody<'a> {
    Json(&'a Term),
    Proto { type_name: String, bytes: &'a [u8] },
}

pub fn classify_body(t: &Term) -> Result<DocBody<'_>> {
    if let Ok(tup) = t.as_tuple() {
        if tup.len() == 3 && tup[0].is_atom("pb") {
            return Ok(DocBody::Proto {
                type_name: bin_str(&tup[1])?,
                bytes: tup[2].as_bin()?,
            });
        }
    }
    Ok(DocBody::Json(t))
}

/// Render a stored body as JSON. EJSON bodies decode as before; proto
/// bodies become the lossless envelope `{"$pb_type": ..., "$pb_body":
/// <base64>}` — the single representation every layer above (rendering,
/// validation, replication, Mango augmentation) works from.
pub fn body_json(t: &Term) -> Result<Value> {
    use base64::engine::general_purpose::STANDARD;
    match classify_body(t)? {
        DocBody::Json(term) => crate::ejson::to_json(term),
        DocBody::Proto { type_name, bytes } => Ok(serde_json::json!({
            "$pb_type": type_name,
            "$pb_body": STANDARD.encode(bytes),
        })),
    }
}

/// Build the disk term for a proto body.
pub fn proto_body_term(type_name: &str, bytes: Vec<u8>) -> Term {
    Term::Tuple(vec![
        Term::atom("pb"),
        Term::Bin(type_name.as_bytes().to_vec()),
        Term::Bin(bytes),
    ])
}

pub fn read_summary(file: &CouchFile, ptr: u64) -> Result<Summary> {
    let bin = file.read_chunk(ptr)?;
    let outer = etf::decode(&bin)?;
    let pair = outer.tuple_n(2)?;
    let body = unwrap_member(&pair[0])?;
    let atts_term = unwrap_member(&pair[1])?;
    let mut atts = Vec::new();
    for a in atts_term.as_list()? {
        atts.push(att_from_disk_term(a)?);
    }
    Ok(Summary { body, atts })
}

/// Body/Atts members are compressed binaries for current files, or direct
/// terms for files written before compression (couch_compress handles both).
fn unwrap_member(t: &Term) -> Result<Term> {
    match t {
        Term::Bin(b) => compress::decompress(b),
        other => Ok(other.clone()),
    }
}

fn att_from_disk_term(t: &Term) -> Result<AttInfo> {
    // Either the 8-tuple, the {Base, Extended} wrapper, or legacy shapes.
    let tup = t.as_tuple()?;
    if tup.len() == 2 && tup[0].as_tuple().is_ok() && tup[1].as_list().is_ok() {
        // {Base, Extended}: extended props don't affect what we report.
        return att_from_disk_term(&tup[0]);
    }
    match tup.len() {
        8 => Ok(AttInfo {
            name: bin_str(&tup[0])?,
            content_type: bin_str(&tup[1])?,
            chunks: stream_chunks(&tup[2])?,
            att_len: tup[3].as_u64()?,
            disk_len: tup[4].as_u64()?,
            revpos: tup[5].as_u64()?,
            md5: match &tup[6] {
                Term::Bin(b) => b.clone(),
                _ => Vec::new(),
            },
            encoding: match &tup[7] {
                Term::Atom(a) => a.clone(),
                _ => "identity".into(),
            },
        }),
        6 => Ok(AttInfo {
            name: bin_str(&tup[0])?,
            content_type: bin_str(&tup[1])?,
            chunks: stream_chunks(&tup[2])?,
            att_len: tup[3].as_u64()?,
            disk_len: tup[3].as_u64()?,
            revpos: tup[4].as_u64()?,
            md5: match &tup[5] {
                Term::Bin(b) => b.clone(),
                _ => Vec::new(),
            },
            encoding: "identity".into(),
        }),
        _ => Err(corrupt(format!("bad attachment disk term: {t:?}"))),
    }
}

fn bin_str(t: &Term) -> Result<String> {
    String::from_utf8(t.as_bin()?.to_vec()).map_err(|_| corrupt("non-UTF-8 attachment field"))
}

fn stream_chunks(t: &Term) -> Result<Vec<(u64, Option<u64>)>> {
    let mut out = Vec::new();
    for c in t.as_list()? {
        match c {
            Term::Int(p) => out.push((*p as u64, None)),
            Term::Tuple(pair) if pair.len() == 2 => {
                out.push((pair[0].as_u64()?, Some(pair[1].as_u64()?)))
            }
            other => return Err(corrupt(format!("bad stream pointer: {other:?}"))),
        }
    }
    Ok(out)
}

/// Concatenate an attachment's stream chunks (stored/encoded form).
pub fn read_att_data(file: &CouchFile, att: &AttInfo) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(att.att_len as usize);
    for (pos, _len) in &att.chunks {
        out.extend_from_slice(&file.read_chunk(*pos)?);
    }
    Ok(out)
}

/// Sequential reader over an attachment's stored chunk stream with cheap
/// forward skips: chunks whose data length is known from the sp list are
/// skipped by arithmetic — their bytes are never read from disk. Serves
/// selective protobuf field extraction; use on identity-encoded
/// attachments only (gzip streams can't skip).
pub struct AttReader<'a> {
    file: &'a CouchFile,
    chunks: &'a [(u64, Option<u64>)],
    idx: usize,
    off: usize,
    cur: Option<Vec<u8>>,
    /// Chunks actually read from disk (observability + tests).
    pub chunks_read: usize,
}

impl<'a> AttReader<'a> {
    pub fn new(file: &'a CouchFile, att: &'a AttInfo) -> AttReader<'a> {
        AttReader {
            file,
            chunks: &att.chunks,
            idx: 0,
            off: 0,
            cur: None,
            chunks_read: 0,
        }
    }

    fn load(&mut self) -> Result<&[u8]> {
        if self.cur.is_none() {
            let data = self.file.read_chunk(self.chunks[self.idx].0)?;
            if let Some(expect) = self.chunks[self.idx].1 {
                if data.len() as u64 != expect {
                    return Err(corrupt(format!(
                        "attachment chunk {} length {} != sp length {expect}",
                        self.idx,
                        data.len()
                    )));
                }
            }
            self.chunks_read += 1;
            self.cur = Some(data);
        }
        Ok(self.cur.as_deref().unwrap())
    }

    fn advance_chunk(&mut self) {
        self.idx += 1;
        self.off = 0;
        self.cur = None;
    }

    pub fn read_exact(&mut self, mut buf: &mut [u8]) -> Result<()> {
        while !buf.is_empty() {
            if self.idx >= self.chunks.len() {
                return Err(Error::BadRequest("read past end of attachment".into()));
            }
            let off = self.off;
            let data = self.load()?;
            let avail = data.len() - off;
            if avail == 0 {
                self.advance_chunk();
                continue;
            }
            let take = avail.min(buf.len());
            buf[..take].copy_from_slice(&data[off..off + take]);
            buf = &mut buf[take..];
            self.off += take;
        }
        Ok(())
    }

    pub fn skip(&mut self, mut n: u64) -> Result<()> {
        while n > 0 {
            if self.idx >= self.chunks.len() {
                return Err(Error::BadRequest("skip past end of attachment".into()));
            }
            // Known-length chunk not yet loaded: skip by arithmetic.
            if self.cur.is_none() {
                if let Some(len) = self.chunks[self.idx].1 {
                    let rest = len - self.off as u64;
                    if n >= rest {
                        n -= rest;
                        self.advance_chunk();
                        continue;
                    }
                    self.off += n as usize;
                    return Ok(());
                }
            }
            // Loaded (or unknown-length legacy) chunk: consume its data.
            let off = self.off;
            let data_len = self.load()?.len();
            let rest = (data_len - off) as u64;
            if n >= rest {
                n -= rest;
                self.advance_chunk();
            } else {
                self.off += n as usize;
                n = 0;
            }
        }
        Ok(())
    }
}

/// Attachment data in identity form — gunzips server-side gzip encoding,
/// which is what every HTTP-facing read path serves.
pub fn read_att_data_decoded(file: &CouchFile, att: &AttInfo) -> Result<Vec<u8>> {
    let data = read_att_data(file, att)?;
    if att.encoding == "gzip" {
        let mut out = Vec::with_capacity(att.disk_len as usize);
        use std::io::Read;
        flate2::read::GzDecoder::new(&data[..])
            .read_to_end(&mut out)
            .map_err(|e| corrupt(format!("gunzip attachment failed: {e}")))?;
        return Ok(out);
    }
    Ok(data)
}

/// Attachment JSON as CouchDB emits it (stub or inline).
pub fn att_json(file: &CouchFile, att: &AttInfo, inline_data: bool) -> Result<Value> {
    let mut m = Map::new();
    m.insert(
        "content_type".into(),
        Value::String(att.content_type.clone()),
    );
    m.insert("revpos".into(), Value::Number(att.revpos.into()));
    if !att.md5.is_empty() {
        let d = base64::engine::general_purpose::STANDARD.encode(&att.md5);
        m.insert("digest".into(), Value::String(format!("md5-{d}")));
    }
    if inline_data {
        // Inline data is always the decoded (identity) form, and CouchDB
        // omits length/stub/encoding fields when inlining.
        let data = read_att_data_decoded(file, att)?;
        m.insert(
            "data".into(),
            Value::String(base64::engine::general_purpose::STANDARD.encode(&data)),
        );
    } else {
        m.insert("length".into(), Value::Number(att.disk_len.into()));
        if att.encoding != "identity" {
            m.insert("encoding".into(), Value::String(att.encoding.clone()));
            m.insert("encoded_length".into(), Value::Number(att.att_len.into()));
        }
        m.insert("stub".into(), Value::Bool(true));
    }
    Ok(Value::Object(m))
}

/// Format a rev id binary as CouchDB does: 16-byte binaries as hex,
/// anything else as its (UTF-8) bytes.
pub fn revid_str(revid: &[u8]) -> String {
    if revid.len() == 16 {
        let mut s = String::with_capacity(32);
        for b in revid {
            s.push_str(&format!("{b:02x}"));
        }
        s
    } else {
        String::from_utf8_lossy(revid).into_owned()
    }
}

pub fn rev_str(pos: u64, revid: &[u8]) -> String {
    format!("{pos}-{}", revid_str(revid))
}

/// Parse "N-hexorstring" into (pos, revid bytes) — couch_doc:parse_rev.
pub fn parse_rev(s: &str) -> Result<(u64, Vec<u8>)> {
    let (pos, rest) = s
        .split_once('-')
        .ok_or_else(|| crate::error::Error::BadRequest(format!("invalid rev: {s}")))?;
    let pos: u64 = pos
        .parse()
        .map_err(|_| crate::error::Error::BadRequest(format!("invalid rev: {s}")))?;
    Ok((pos, parse_revid(rest)))
}

pub fn parse_revid(s: &str) -> Vec<u8> {
    if s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        let mut out = Vec::with_capacity(16);
        let b = s.as_bytes();
        for i in (0..32).step_by(2) {
            let hi = (b[i] as char).to_digit(16).unwrap() as u8;
            let lo = (b[i + 1] as char).to_digit(16).unwrap() as u8;
            out.push(hi << 4 | lo);
        }
        out
    } else {
        s.as_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rev_roundtrip() {
        let (pos, id) = parse_rev("3-a5c2b1f77c03fca03d517d5e32f942bd").unwrap();
        assert_eq!(pos, 3);
        assert_eq!(id.len(), 16);
        assert_eq!(rev_str(pos, &id), "3-a5c2b1f77c03fca03d517d5e32f942bd");

        let (pos, id) = parse_rev("0-1").unwrap();
        assert_eq!(pos, 0);
        assert_eq!(id, b"1");
        assert_eq!(rev_str(1, b"custom-rev"), "1-custom-rev");
    }
}
