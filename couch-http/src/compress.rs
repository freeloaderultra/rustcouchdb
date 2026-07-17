//! Transparent HTTP gzip, both directions, negotiated per request so every
//! client that doesn't ask for it sees exactly the bytes it sees today.
//!
//! Requests: `Content-Encoding: gzip` bodies are inflated before any handler
//! runs (stock parity — chttpd has accepted gzipped bodies since 1.x). The
//! decoded stream still passes through the extractor body limit, so the
//! decompressed size is capped like a plain body.
//!
//! Responses: compressed only when the client sent `Accept-Encoding: gzip`,
//! the content type is compressible (text/*, json, xml, javascript — stock's
//! `compressible_types` shape), nothing upstream already set an encoding, and
//! the response isn't an unbounded stream (feed=continuous marks itself with
//! [`NoCompress`]: a compressor buffering its heartbeat newlines would stall
//! replicator liveness checks).

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures::TryStreamExt;
use tokio_util::io::{ReaderStream, StreamReader};

/// Response-extension marker: never compress this response.
#[derive(Clone, Copy)]
pub struct NoCompress;

/// Responses with a known size below this stay identity: the gzip header
/// alone eats most of the saving and the CPU buys nothing.
const MIN_COMPRESS_BYTES: u64 = 512;

fn io_err<E: std::fmt::Display>(e: E) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

pub async fn decompress_request(req: Request, next: Next) -> Response {
    let encoding = req
        .headers()
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_ascii_lowercase());
    match encoding.as_deref() {
        None | Some("") | Some("identity") => next.run(req).await,
        Some("gzip") | Some("x-gzip") => {
            let (mut parts, body) = req.into_parts();
            parts.headers.remove(header::CONTENT_ENCODING);
            // The advertised length describes the compressed bytes; the
            // handlers must see the stream as unsized.
            parts.headers.remove(header::CONTENT_LENGTH);
            let reader = StreamReader::new(body.into_data_stream().map_err(io_err));
            let decoded =
                async_compression::tokio::bufread::GzipDecoder::new(tokio::io::BufReader::new(reader));
            let req = Request::from_parts(parts, Body::from_stream(ReaderStream::new(decoded)));
            next.run(req).await
        }
        Some(_) => (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            axum::Json(serde_json::json!({
                "error": "unsupported_media_type",
                "reason": "Only gzip and identity content-encodings are supported",
            })),
        )
            .into_response(),
    }
}

/// Does an Accept-Encoding header value accept gzip (with nonzero q)?
fn accepts_gzip(value: &str) -> bool {
    value.split(',').any(|part| {
        let mut it = part.trim().split(';');
        let coding = it.next().unwrap_or("").trim();
        if !coding.eq_ignore_ascii_case("gzip") && !coding.eq_ignore_ascii_case("x-gzip") {
            return false;
        }
        for param in it {
            let param = param.trim();
            if let Some(q) = param.strip_prefix("q=").or_else(|| param.strip_prefix("Q=")) {
                return q.trim().parse::<f32>().map(|q| q > 0.0).unwrap_or(false);
            }
        }
        true
    })
}

fn compressible_type(ct: Option<&HeaderValue>) -> bool {
    let Some(ct) = ct.and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let mime = ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
    mime.starts_with("text/")
        || mime.contains("json")
        || mime.contains("xml")
        || mime.contains("javascript")
}

pub async fn compress_response(req: Request, next: Next) -> Response {
    let wants_gzip = req
        .headers()
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(accepts_gzip);
    let is_head = req.method() == Method::HEAD;

    let resp = next.run(req).await;

    if !wants_gzip || is_head {
        return resp;
    }
    if resp.extensions().get::<NoCompress>().is_some() {
        return resp;
    }
    if resp.headers().contains_key(header::CONTENT_ENCODING) {
        return resp;
    }
    if !compressible_type(resp.headers().get(header::CONTENT_TYPE)) {
        return resp;
    }
    {
        use http_body::Body as _;
        if let Some(exact) = resp.body().size_hint().exact() {
            if exact < MIN_COMPRESS_BYTES {
                return resp;
            }
        }
    }

    let (mut parts, body) = resp.into_parts();
    parts.headers.remove(header::CONTENT_LENGTH);
    parts
        .headers
        .insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    parts
        .headers
        .append(header::VARY, HeaderValue::from_static("accept-encoding"));
    let reader = StreamReader::new(body.into_data_stream().map_err(io_err));
    let encoded = async_compression::tokio::bufread::GzipEncoder::with_quality(
        tokio::io::BufReader::new(reader),
        async_compression::Level::Fastest,
    );
    Response::from_parts(parts, Body::from_stream(ReaderStream::new(encoded)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_encoding_parsing() {
        assert!(accepts_gzip("gzip"));
        assert!(accepts_gzip("gzip, deflate, br"));
        assert!(accepts_gzip("deflate, gzip;q=0.5"));
        assert!(accepts_gzip("GZIP"));
        assert!(!accepts_gzip("deflate"));
        assert!(!accepts_gzip("gzip;q=0"));
        assert!(!accepts_gzip("identity"));
        assert!(!accepts_gzip("*")); // only an explicit gzip opts in
    }

    #[test]
    fn compressible_types() {
        let hv = |s: &str| HeaderValue::from_str(s).unwrap();
        assert!(compressible_type(Some(&hv("application/json"))));
        assert!(compressible_type(Some(&hv("application/json; charset=utf-8"))));
        assert!(compressible_type(Some(&hv("text/plain"))));
        assert!(compressible_type(Some(&hv("text/html"))));
        assert!(compressible_type(Some(&hv("image/svg+xml"))));
        assert!(compressible_type(Some(&hv("application/javascript"))));
        assert!(!compressible_type(Some(&hv("image/jpeg"))));
        assert!(!compressible_type(Some(&hv("application/octet-stream"))));
        assert!(!compressible_type(None));
    }
}
