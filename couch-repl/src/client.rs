use crate::error::{Error, Result};
use crate::retry::{with_retry, RetryPolicy};
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Method, RequestBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::time::Duration;
use url::Url;

/// One CouchDB database endpoint (source or target).
#[derive(Clone)]
pub struct Endpoint {
    pub label: &'static str,
    db_url: Url,
    client: reqwest::Client,
    headers: HeaderMap,
    pub retry: RetryPolicy,
    request_timeout: Duration,
}

impl Endpoint {
    pub fn new(
        label: &'static str,
        raw_url: &str,
        extra_headers: &[(String, String)],
        insecure: bool,
        timeout_secs: u64,
        retry: RetryPolicy,
    ) -> Result<Endpoint> {
        let mut url = Url::parse(raw_url).map_err(|e| Error::Url(format!("{raw_url}: {e}")))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Error::Url(format!("{raw_url}: scheme must be http or https")));
        }
        if url.path() == "/" || url.path().is_empty() {
            return Err(Error::Url(format!("{raw_url}: missing database name in path")));
        }

        let mut headers = HeaderMap::new();
        // Credentials come from URL userinfo; strip them from the URL we log/use.
        if !url.username().is_empty() || url.password().is_some() {
            let user = percent_decode(url.username());
            let pass = percent_decode(url.password().unwrap_or(""));
            let token = base64_std(format!("{user}:{pass}").as_bytes());
            let value = HeaderValue::from_str(&format!("Basic {token}"))
                .map_err(|_| Error::Url("invalid credentials in URL".into()))?;
            headers.insert(AUTHORIZATION, value);
            let _ = url.set_username("");
            let _ = url.set_password(None);
        }
        for (k, v) in extra_headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|_| Error::Url(format!("invalid header name: {k}")))?;
            let value = HeaderValue::from_str(v)
                .map_err(|_| Error::Url(format!("invalid header value for {k}")))?;
            headers.insert(name, value);
        }

        // Strip a trailing slash so we can push path segments uniformly.
        if url.path().ends_with('/') {
            let trimmed = url.path().trim_end_matches('/').to_string();
            url.set_path(&trimmed);
        }

        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .danger_accept_invalid_certs(insecure)
            .pool_max_idle_per_host(128)
            .pool_idle_timeout(Duration::from_secs(90))
            .connect_timeout(Duration::from_secs(10))
            .tcp_nodelay(true)
            .gzip(true)
            .build()
            .map_err(Error::Net)?;

        Ok(Endpoint {
            label,
            db_url: url,
            client,
            headers,
            retry,
            request_timeout: Duration::from_secs(timeout_secs),
        })
    }

    /// Database name (last path segment), for display.
    pub fn db_name(&self) -> String {
        self.db_url
            .path_segments()
            .and_then(|s| s.last())
            .unwrap_or("?")
            .to_string()
    }

    /// URL with credentials stripped, for replication-id generation and logs.
    pub fn normalized_url(&self) -> String {
        let mut u = self.db_url.clone();
        if let Some(host) = u.host_str().map(|h| h.to_ascii_lowercase()) {
            let _ = u.set_host(Some(&host));
        }
        u.to_string()
    }

    /// Build a URL under the database: segments are percent-encoded.
    pub fn url(&self, segments: &[&str]) -> Url {
        let mut u = self.db_url.clone();
        {
            let mut path = u.path_segments_mut().expect("http(s) URLs have paths");
            for s in segments {
                path.push(s);
            }
        }
        u
    }

    pub fn request(&self, method: Method, url: Url) -> RequestBuilder {
        self.client
            .request(method, url)
            .headers(self.headers.clone())
    }

    /// Send a request and map non-2xx statuses to errors.
    pub async fn send(&self, rb: RequestBuilder) -> Result<Response> {
        let resp = rb.send().await.map_err(Error::Net)?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let url = resp.url().to_string();
        let retry_after = resp
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let body = resp.text().await.unwrap_or_default();
        let mut snippet: String = body.chars().take(500).collect();
        if status == StatusCode::TOO_MANY_REQUESTS {
            if let Some(ra) = retry_after {
                snippet = format!("retry-after:{ra};{snippet}");
            }
        }
        Err(Error::Http {
            status: status.as_u16(),
            url,
            body: snippet,
        })
    }

    pub async fn get_json<T: DeserializeOwned>(
        &self,
        segments: &[&str],
        query: &[(&str, String)],
    ) -> Result<T> {
        let url = self.url(segments);
        let what = format!("GET {}/{}", self.label, segments.join("/"));
        with_retry(&self.retry, &what, || async {
            let rb = self
                .request(Method::GET, url.clone())
                .header(reqwest::header::ACCEPT, "application/json")
                .query(query)
                .timeout(self.request_timeout);
            let resp = self.send(rb).await?;
            resp.json::<T>().await.map_err(Error::Net)
        })
        .await
    }

    /// GET returning the raw body bytes (still JSON, parsed by the caller with RawValue).
    pub async fn get_bytes(&self, segments: &[&str], query: &[(&str, String)]) -> Result<Bytes> {
        let url = self.url(segments);
        let what = format!("GET {}/{}", self.label, segments.join("/"));
        with_retry(&self.retry, &what, || async {
            let rb = self
                .request(Method::GET, url.clone())
                .header(reqwest::header::ACCEPT, "application/json")
                .query(query)
                .timeout(self.request_timeout);
            let resp = self.send(rb).await?;
            resp.bytes().await.map_err(Error::Net)
        })
        .await
    }

    pub async fn post_json<B: Serialize, T: DeserializeOwned>(
        &self,
        segments: &[&str],
        query: &[(&str, String)],
        body: &B,
    ) -> Result<T> {
        let url = self.url(segments);
        let what = format!("POST {}/{}", self.label, segments.join("/"));
        with_retry(&self.retry, &what, || async {
            let rb = self
                .request(Method::POST, url.clone())
                .header(reqwest::header::ACCEPT, "application/json")
                .query(query)
                .json(body)
                .timeout(self.request_timeout);
            let resp = self.send(rb).await?;
            resp.json::<T>().await.map_err(Error::Net)
        })
        .await
    }

    /// POST with a pre-serialized JSON body (zero re-serialization path).
    pub async fn post_raw<T: DeserializeOwned>(
        &self,
        segments: &[&str],
        query: &[(&str, String)],
        body: Bytes,
        timeout: Duration,
    ) -> Result<T> {
        let url = self.url(segments);
        let what = format!("POST {}/{}", self.label, segments.join("/"));
        with_retry(&self.retry, &what, || async {
            let rb = self
                .request(Method::POST, url.clone())
                .header(reqwest::header::ACCEPT, "application/json")
                .query(query)
                .header(CONTENT_TYPE, "application/json")
                .body(body.clone())
                .timeout(timeout);
            let resp = self.send(rb).await?;
            resp.json::<T>().await.map_err(Error::Net)
        })
        .await
    }

    pub async fn put_json<B: Serialize, T: DeserializeOwned>(
        &self,
        segments: &[&str],
        query: &[(&str, String)],
        body: &B,
    ) -> Result<T> {
        let url = self.url(segments);
        let what = format!("PUT {}/{}", self.label, segments.join("/"));
        with_retry(&self.retry, &what, || async {
            let rb = self
                .request(Method::PUT, url.clone())
                .header(reqwest::header::ACCEPT, "application/json")
                .query(query)
                .json(body)
                .timeout(self.request_timeout);
            let resp = self.send(rb).await?;
            resp.json::<T>().await.map_err(Error::Net)
        })
        .await
    }

    /// PUT with no body (e.g. database creation).
    pub async fn put_empty(&self, segments: &[&str]) -> Result<serde_json::Value> {
        let url = self.url(segments);
        let what = format!("PUT {}/{}", self.label, segments.join("/"));
        with_retry(&self.retry, &what, || async {
            let rb = self
                .request(Method::PUT, url.clone())
                .header(reqwest::header::ACCEPT, "application/json")
                .timeout(self.request_timeout);
            let resp = self.send(rb).await?;
            resp.json().await.map_err(Error::Net)
        })
        .await
    }

    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }
}

#[derive(serde::Deserialize, Debug)]
pub struct DbInfo {
    pub update_seq: serde_json::Value,
    #[serde(default)]
    pub doc_count: u64,
}

impl Endpoint {
    pub async fn db_info(&self) -> Result<DbInfo> {
        self.get_json(&[], &[]).await
    }

    /// Ensure the database exists; create it when `create` is set.
    pub async fn ensure_db(&self, create: bool) -> Result<DbInfo> {
        match self.db_info().await {
            Ok(info) => Ok(info),
            Err(e) if e.status() == Some(404) && create => {
                match self.put_empty(&[]).await {
                    Ok(_) => {}
                    // Lost a race with another creator; that's fine.
                    Err(e) if e.status() == Some(412) => {}
                    Err(e) => return Err(e),
                }
                self.db_info().await
            }
            Err(e) => Err(e),
        }
    }
}

fn percent_decode(s: &str) -> String {
    // Minimal %XX decoding for URL userinfo.
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn base64_std(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}
