//! Admin auth: HTTP basic and CouchDB cookie sessions (_session).
//! One admin account (from --admin); without it the server is in
//! admin-party mode like a fresh CouchDB.

use crate::error::{ApiError, ApiResult};
use crate::state::{hex, App};
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::Engine;
use md5::{Digest, Md5};
use serde_json::json;

const SESSION_TTL_SECS: u64 = 24 * 3600;

fn token(state: &App, name: &str, expiry: u64) -> String {
    let mut h = Md5::new();
    h.update(state.secret);
    h.update(name.as_bytes());
    h.update(expiry.to_le_bytes());
    let sig = hex(&h.finalize());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{name}:{expiry}:{sig}"))
}

fn check_token(state: &App, tok: &str) -> Option<String> {
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(tok)
        .ok()?;
    let raw = String::from_utf8(raw).ok()?;
    let mut parts = raw.rsplitn(3, ':');
    let sig = parts.next()?;
    let expiry: u64 = parts.next()?.parse().ok()?;
    let name = parts.next()?;
    if expiry < crate::state::now_secs() {
        return None;
    }
    let mut h = Md5::new();
    h.update(state.secret);
    h.update(name.as_bytes());
    h.update(expiry.to_le_bytes());
    if hex(&h.finalize()) == sig {
        Some(name.to_string())
    } else {
        None
    }
}

/// Who is making this request, if anyone we recognize.
fn authenticate(state: &App, headers: &HeaderMap) -> Option<String> {
    let (admin_user, admin_pass) = state.admin.as_ref()?;
    // Basic
    if let Some(v) = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        if let Some(b64) = v.strip_prefix("Basic ") {
            if let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(b64.trim()) {
                if let Ok(s) = String::from_utf8(raw) {
                    if let Some((u, p)) = s.split_once(':') {
                        if u == admin_user && p == admin_pass {
                            return Some(u.to_string());
                        }
                    }
                }
            }
        }
    }
    // Cookie
    for cookie in headers.get_all(header::COOKIE) {
        if let Ok(s) = cookie.to_str() {
            for part in s.split(';') {
                if let Some(tok) = part.trim().strip_prefix("AuthSession=") {
                    if let Some(name) = check_token(state, tok) {
                        if name == *admin_user {
                            return Some(name);
                        }
                    }
                }
            }
        }
    }
    None
}

/// Middleware: everything except /, /_up and /_session requires the admin
/// (when one is configured).
pub async fn require_admin(
    State(state): State<App>,
    req: Request,
    next: Next,
) -> Response {
    if state.admin.is_none() {
        return next.run(req).await;
    }
    let path = req.uri().path();
    // /_utils is the static admin UI shell: it must load unauthenticated so
    // its login page can render (the data endpoints stay guarded).
    if path == "/" || path == "/_up" || path == "/_session" || path == "/_utils"
        || path.starts_with("/_utils/")
    {
        return next.run(req).await;
    }
    if authenticate(&state, req.headers()).is_some() {
        return next.run(req).await;
    }
    ApiError::unauthorized().into_response()
}

fn user_ctx(state: &App, headers: &HeaderMap) -> (Option<String>, serde_json::Value) {
    match &state.admin {
        None => (
            Some("admin-party".into()),
            json!({"name": null, "roles": ["_admin"]}),
        ),
        Some(_) => match authenticate(state, headers) {
            Some(name) => (Some(name.clone()), json!({"name": name, "roles": ["_admin"]})),
            None => (None, json!({"name": null, "roles": []})),
        },
    }
}

pub async fn session_get(State(state): State<App>, headers: HeaderMap) -> Json<serde_json::Value> {
    let (_, ctx) = user_ctx(&state, &headers);
    Json(json!({
        "ok": true,
        "userCtx": ctx,
        "info": {
            "authentication_handlers": ["cookie", "default"],
            "authenticated": "default",
        }
    }))
}

pub async fn session_post(
    State(state): State<App>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let (name, password) = parse_credentials(&headers, &body)?;
    let Some((admin_user, admin_pass)) = state.admin.as_ref() else {
        // Admin party: any login succeeds.
        return Ok(session_response(&state, &name));
    };
    if &name == admin_user && &password == admin_pass {
        Ok(session_response(&state, &name))
    } else {
        Err(ApiError::unauthorized())
    }
}

pub async fn session_delete() -> Response {
    let mut resp = Json(json!({"ok": true})).into_response();
    resp.headers_mut().insert(
        header::SET_COOKIE,
        header::HeaderValue::from_static("AuthSession=; Version=1; Path=/; HttpOnly; Max-Age=0"),
    );
    resp
}

fn session_response(state: &App, name: &str) -> Response {
    let tok = token(state, name, crate::state::now_secs() + SESSION_TTL_SECS);
    let mut resp = (
        StatusCode::OK,
        Json(json!({"ok": true, "name": name, "roles": ["_admin"]})),
    )
        .into_response();
    resp.headers_mut().insert(
        header::SET_COOKIE,
        header::HeaderValue::from_str(&format!(
            "AuthSession={tok}; Version=1; Path=/; HttpOnly"
        ))
        .expect("valid cookie"),
    );
    resp
}

/// _session accepts both JSON and form-encoded bodies.
fn parse_credentials(headers: &HeaderMap, body: &[u8]) -> ApiResult<(String, String)> {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if ct.starts_with("application/json") {
        let v: serde_json::Value = serde_json::from_slice(body)
            .map_err(|e| ApiError::bad_request(format!("invalid json: {e}")))?;
        let name = v.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let password = v.get("password").and_then(|x| x.as_str()).unwrap_or("").to_string();
        Ok((name, password))
    } else {
        let s = String::from_utf8_lossy(body);
        let mut name = String::new();
        let mut password = String::new();
        for pair in s.split('&') {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let v = urldecode(v);
            match k {
                "name" => name = v,
                "password" => password = v,
                _ => {}
            }
        }
        Ok((name, password))
    }
}

fn urldecode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                if let Ok(x) = u8::from_str_radix(
                    std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("zz"),
                    16,
                ) {
                    out.push(x);
                    i += 2;
                } else {
                    out.push(b'%');
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}
