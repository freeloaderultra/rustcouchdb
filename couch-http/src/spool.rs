//! Attachment spooling: request bodies stream to anonymous temp files
//! (md5 computed on the way through) so no attachment is ever held in RAM,
//! and a streaming multipart/related parser that spools each attachment
//! part while buffering only the JSON document part.

use crate::error::{ApiError, ApiResult};
use axum::body::Body;
use bytes::BytesMut;
use futures::StreamExt;
use md5::{Digest, Md5};
use serde_json::Value;
use std::path::Path;
use tokio::io::AsyncWriteExt;

/// The JSON part of a multipart doc PUT must fit in memory (the doc body
/// always does); attachment parts are unbounded and never buffered.
const JSON_PART_CAP: usize = 256 * 1024 * 1024;
/// Part headers are framing, not payload.
const HEADER_CAP: usize = 64 * 1024;

/// A spooled body: anonymous temp file (auto-deleted), length and md5.
pub struct Spool {
    pub file: std::fs::File,
    pub len: u64,
    pub md5: Vec<u8>,
}

impl Spool {
    pub fn into_att(self, name: String, content_type: String) -> couch_store::writer::SpooledAtt {
        couch_store::writer::SpooledAtt {
            name,
            content_type,
            revpos: None,
            file: self.file,
            len: self.len,
            md5: self.md5,
        }
    }
}

fn spool_file(dir: &Path) -> ApiResult<tokio::fs::File> {
    let f = tempfile::tempfile_in(dir)
        .or_else(|_| tempfile::tempfile())
        .map_err(|e| ApiError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "spool_failed", format!("cannot create attachment spool: {e}")))?;
    Ok(tokio::fs::File::from_std(f))
}

async fn finish_spool(mut file: tokio::fs::File, len: u64, md5: Md5) -> ApiResult<Spool> {
    file.flush()
        .await
        .map_err(|e| ApiError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "spool_failed", format!("spool write failed: {e}")))?;
    Ok(Spool {
        file: file.into_std().await,
        len,
        md5: md5.finalize().to_vec(),
    })
}

/// Stream a whole request body to a spool.
pub async fn spool_body(body: Body, dir: &Path) -> ApiResult<Spool> {
    let mut file = spool_file(dir)?;
    let mut md5 = Md5::new();
    let mut len = 0u64;
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| ApiError::bad_request(format!("body read failed: {e}")))?;
        md5.update(&chunk);
        len += chunk.len() as u64;
        file.write_all(&chunk)
            .await
            .map_err(|e| ApiError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "spool_failed", format!("spool write failed: {e}")))?;
    }
    finish_spool(file, len, md5).await
}

/// A parsed multipart/related doc PUT: the JSON document and one spool per
/// body part after it, in on-the-wire order.
pub struct MultipartDoc {
    pub doc: Value,
    pub parts: Vec<Spool>,
}

/// Incremental multipart/related parser over a request body stream.
///
/// Wire shape (couch_doc / couch-repl):
///   --B\r\n<headers>\r\n\r\n{json}\r\n--B\r\n\r\n<att bytes>\r\n--B--
/// Only the framing is interpreted; attachment bytes flow to spools as they
/// arrive, with a small carry buffer for boundaries straddling chunk edges.
pub async fn parse_multipart_stream(
    content_type: &str,
    body: Body,
    dir: &Path,
) -> ApiResult<MultipartDoc> {
    let boundary = content_type
        .split(';')
        .filter_map(|p| p.trim().strip_prefix("boundary="))
        .next()
        .map(|b| b.trim_matches('"').to_string())
        .ok_or_else(|| ApiError::bad_request("multipart/related without boundary"))?;
    let delim: Vec<u8> = format!("--{boundary}").into_bytes();

    let mut r = Reader {
        stream: body.into_data_stream(),
        buf: BytesMut::new(),
        eof: false,
    };

    // Preamble: everything up to and including the first delimiter.
    r.skip_past_delim(&delim).await?;

    let mut doc: Option<Value> = None;
    let mut parts: Vec<Spool> = Vec::new();
    loop {
        // After a delimiter: "--" closes the multipart, CRLF opens a part.
        r.fill(2).await?;
        if r.buf.starts_with(b"--") {
            break;
        }
        // Part headers end at the first blank line (couch-repl's attachment
        // parts have no headers: the blank line comes immediately).
        let body_start = loop {
            if let Some(i) = find(&r.buf, b"\r\n\r\n") {
                break i + 4;
            }
            if let Some(i) = find(&r.buf, b"\n\n") {
                break i + 2;
            }
            if r.buf.len() > HEADER_CAP {
                return Err(ApiError::bad_request("multipart part headers too large"));
            }
            if !r.fill_more().await? {
                return Err(ApiError::bad_request("truncated multipart body"));
            }
        };
        let _ = r.buf.split_to(body_start);

        if doc.is_none() {
            // JSON part: buffered (it is the document body).
            let data = r.read_part_buffered(&delim, JSON_PART_CAP).await?;
            doc = Some(
                serde_json::from_slice(&data)
                    .map_err(|e| ApiError::bad_request(format!("bad JSON part: {e}")))?,
            );
        } else {
            // Attachment part: streamed to a spool.
            let mut file = spool_file(dir)?;
            let mut md5 = Md5::new();
            let mut len = 0u64;
            r.read_part_streaming(&delim, |piece| {
                md5.update(piece);
                len += piece.len() as u64;
            }, &mut file)
            .await?;
            parts.push(finish_spool(file, len, md5).await?);
        }
    }

    let doc = doc.ok_or_else(|| ApiError::bad_request("empty multipart body"))?;
    Ok(MultipartDoc { doc, parts })
}

struct Reader {
    stream: axum::body::BodyDataStream,
    buf: BytesMut,
    eof: bool,
}

impl Reader {
    async fn fill_more(&mut self) -> ApiResult<bool> {
        if self.eof {
            return Ok(false);
        }
        match self.stream.next().await {
            Some(chunk) => {
                let chunk =
                    chunk.map_err(|e| ApiError::bad_request(format!("body read failed: {e}")))?;
                self.buf.extend_from_slice(&chunk);
                Ok(true)
            }
            None => {
                self.eof = true;
                Ok(false)
            }
        }
    }

    async fn fill(&mut self, min: usize) -> ApiResult<()> {
        while self.buf.len() < min {
            if !self.fill_more().await? {
                return Err(ApiError::bad_request("truncated multipart body"));
            }
        }
        Ok(())
    }

    /// Drop everything up to and including the next delimiter, keeping
    /// nothing (preamble skip).
    async fn skip_past_delim(&mut self, delim: &[u8]) -> ApiResult<()> {
        loop {
            if let Some(i) = find(&self.buf, delim) {
                let _ = self.buf.split_to(i + delim.len());
                return Ok(());
            }
            // Keep a tail that could hold a partial delimiter.
            let keep = delim.len().saturating_sub(1);
            if self.buf.len() > keep {
                let cut = self.buf.len() - keep;
                let _ = self.buf.split_to(cut);
            }
            if !self.fill_more().await? {
                return Err(ApiError::bad_request("multipart boundary not found"));
            }
        }
    }

    /// Read one part body fully into memory (JSON part), consuming through
    /// the trailing delimiter. Strips the framing CRLF before the delimiter.
    async fn read_part_buffered(&mut self, delim: &[u8], cap: usize) -> ApiResult<Vec<u8>> {
        let mut out: Vec<u8> = Vec::new();
        let keep = delim.len() + 2;
        loop {
            if let Some(i) = find(&self.buf, delim) {
                out.extend_from_slice(&self.buf[..i]);
                let _ = self.buf.split_to(i + delim.len());
                strip_framing_crlf(&mut out);
                return Ok(out);
            }
            if self.buf.len() > keep {
                let cut = self.buf.len() - keep;
                out.extend_from_slice(&self.buf[..cut]);
                let _ = self.buf.split_to(cut);
            }
            if out.len() > cap {
                return Err(ApiError::bad_request("multipart JSON part too large"));
            }
            if !self.fill_more().await? {
                return Err(ApiError::bad_request("truncated multipart body"));
            }
        }
    }

    /// Stream one part body into `file`, consuming through the trailing
    /// delimiter. `observe` sees exactly the payload bytes (for md5/len);
    /// the framing CRLF before the delimiter is never emitted because a
    /// tail of `delim.len() + 2` bytes always stays in the buffer until the
    /// delimiter is found.
    async fn read_part_streaming(
        &mut self,
        delim: &[u8],
        mut observe: impl FnMut(&[u8]),
        file: &mut tokio::fs::File,
    ) -> ApiResult<()> {
        let keep = delim.len() + 2;
        loop {
            if let Some(i) = find(&self.buf, delim) {
                let mut end = i;
                if end >= 1 && self.buf[end - 1] == b'\n' {
                    end -= 1;
                    if end >= 1 && self.buf[end - 1] == b'\r' {
                        end -= 1;
                    }
                }
                if end > 0 {
                    observe(&self.buf[..end]);
                    file.write_all(&self.buf[..end])
                        .await
                        .map_err(|e| ApiError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "spool_failed", format!("spool write failed: {e}")))?;
                }
                let _ = self.buf.split_to(i + delim.len());
                return Ok(());
            }
            if self.buf.len() > keep {
                let cut = self.buf.len() - keep;
                observe(&self.buf[..cut]);
                file.write_all(&self.buf[..cut])
                    .await
                    .map_err(|e| ApiError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "spool_failed", format!("spool write failed: {e}")))?;
                let _ = self.buf.split_to(cut);
            }
            if !self.fill_more().await? {
                return Err(ApiError::bad_request("truncated multipart body"));
            }
        }
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

/// The single CRLF (or LF) before a delimiter is framing, not payload.
fn strip_framing_crlf(out: &mut Vec<u8>) {
    if out.last() == Some(&b'\n') {
        out.pop();
        if out.last() == Some(&b'\r') {
            out.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use md5::Digest;
    use std::io::{Read, Seek, SeekFrom};

    /// Feed a body in n-byte chunks so every boundary straddles chunk edges.
    fn chunked_body(data: Vec<u8>, n: usize) -> Body {
        let chunks: Vec<Result<bytes::Bytes, std::io::Error>> = data
            .chunks(n)
            .map(|c| Ok(bytes::Bytes::copy_from_slice(c)))
            .collect();
        Body::from_stream(futures::stream::iter(chunks))
    }

    fn read_spool(mut s: Spool) -> Vec<u8> {
        let mut out = Vec::new();
        s.file.seek(SeekFrom::Start(0)).unwrap();
        s.file.read_to_end(&mut out).unwrap();
        assert_eq!(out.len() as u64, s.len);
        assert_eq!(Md5::digest(&out).to_vec(), s.md5);
        out
    }

    #[tokio::test]
    async fn multipart_stream_parses_hostile_payloads() {
        // Attachment payloads chosen to break naive framing: leading and
        // trailing newlines, an interior blank line, "--" runs, and a byte
        // string containing the delimiter's prefix.
        let att1 = b"\n\nlat,lon\r\n\r\n1,2\n\n".to_vec();
        let att2: Vec<u8> = Vec::new(); // zero-length attachment
        let att3 = b"--xy binary -- \xff\x00\r\n--x almost".to_vec();
        let doc = serde_json::json!({
            "_id": "d",
            "_attachments": {
                "a.csv": {"follows": true, "content_type": "text/csv", "length": att1.len()},
                "b.bin": {"follows": true, "content_type": "application/octet-stream", "length": 0},
                "c.bin": {"follows": true, "content_type": "application/octet-stream", "length": att3.len()},
            }
        });
        let b = "xyz";
        let mut wire: Vec<u8> = Vec::new();
        wire.extend_from_slice(format!("--{b}\r\ncontent-type: application/json\r\n\r\n{doc}").as_bytes());
        for att in [&att1, &att2, &att3] {
            wire.extend_from_slice(format!("\r\n--{b}\r\n\r\n").as_bytes());
            wire.extend_from_slice(att);
        }
        wire.extend_from_slice(format!("\r\n--{b}--").as_bytes());

        // Parse at several chunk sizes, including 1 byte.
        for n in [1, 3, 7, 4096] {
            let parsed = parse_multipart_stream(
                &format!("multipart/related; boundary=\"{b}\""),
                chunked_body(wire.clone(), n),
                std::env::temp_dir().as_path(),
            )
            .await
            .map_err(|e| format!("chunk size {n}: {e:?}"))
            .unwrap();
            assert_eq!(parsed.doc["_id"], serde_json::json!("d"));
            let parts: Vec<Vec<u8>> = parsed.parts.into_iter().map(read_spool).collect();
            assert_eq!(parts.len(), 3, "chunk size {n}");
            assert_eq!(parts[0], att1, "chunk size {n}");
            assert_eq!(parts[1], att2, "chunk size {n}");
            assert_eq!(parts[2], att3, "chunk size {n}");
        }
    }

    #[tokio::test]
    async fn multipart_stream_rejects_truncated() {
        let wire = b"--xyz\r\ncontent-type: application/json\r\n\r\n{\"_id\":\"d\"}\r\n--xyz\r\n\r\ndata-without-terminator".to_vec();
        let err = parse_multipart_stream(
            "multipart/related; boundary=xyz",
            chunked_body(wire, 5),
            std::env::temp_dir().as_path(),
        )
        .await;
        assert!(err.is_err(), "truncated body must be rejected"); // any 400; must not hang or panic
    }
}
