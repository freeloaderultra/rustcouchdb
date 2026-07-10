//! CouchDB-style error responses: every error is `{"error": .., "reason": ..}`
//! with the status code the Erlang chttpd would use.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub error: String,
    pub reason: String,
}

pub type ApiResult<T> = std::result::Result<T, ApiError>;

impl ApiError {
    pub fn new(status: StatusCode, error: impl Into<String>, reason: impl Into<String>) -> ApiError {
        ApiError {
            status,
            error: error.into(),
            reason: reason.into(),
        }
    }

    pub fn not_found(reason: impl Into<String>) -> ApiError {
        ApiError::new(StatusCode::NOT_FOUND, "not_found", reason)
    }

    pub fn db_not_found() -> ApiError {
        ApiError::not_found("Database does not exist.")
    }

    pub fn missing() -> ApiError {
        ApiError::not_found("missing")
    }

    pub fn deleted() -> ApiError {
        ApiError::not_found("deleted")
    }

    pub fn conflict() -> ApiError {
        ApiError::new(StatusCode::CONFLICT, "conflict", "Document update conflict.")
    }

    pub fn bad_request(reason: impl Into<String>) -> ApiError {
        ApiError::new(StatusCode::BAD_REQUEST, "bad_request", reason)
    }

    pub fn unauthorized() -> ApiError {
        ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Name or password is incorrect.",
        )
    }

    /// Map a per-doc save error string onto its HTTP status.
    pub fn from_save(error: &str, reason: &str) -> ApiError {
        let status = match error {
            "conflict" => StatusCode::CONFLICT,
            "forbidden" => StatusCode::FORBIDDEN,
            "missing_stub" => StatusCode::PRECONDITION_FAILED,
            _ => StatusCode::BAD_REQUEST,
        };
        ApiError::new(status, error, reason)
    }
}

impl From<couch_store::error::Error> for ApiError {
    fn from(e: couch_store::error::Error) -> ApiError {
        use couch_store::error::Error as E;
        match e {
            E::BadRequest(m) => ApiError::bad_request(m),
            E::Io(io) => ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_server_error",
                io.to_string(),
            ),
            E::Corrupt(m) | E::Unsupported(m) => ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_server_error",
                m,
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut resp = (
            self.status,
            Json(json!({"error": self.error, "reason": self.reason})),
        )
            .into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            resp.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                axum::http::HeaderValue::from_static("Basic realm=\"server\""),
            );
        }
        resp
    }
}
