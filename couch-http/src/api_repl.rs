//! POST /_replicate: transient replication jobs on the embedded couch-repl,
//! CouchDB-shaped — continuous jobs return `_local_id` immediately, one-shot
//! jobs block until complete, `cancel: true` stops a running transient job.

use crate::error::{ApiError, ApiResult};
use crate::state::App;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::Value;

pub async fn replicate(State(app): State<App>, Json(body): Json<Value>) -> ApiResult<Response> {
    if !body.is_object() {
        return Err(ApiError::bad_request("a JSON replication document is required"));
    }
    if body.get("cancel").and_then(|c| c.as_bool()).unwrap_or(false) {
        return match crate::repl::cancel_transient(&app, &body).await {
            Ok(Some(resp)) => Ok(Json(resp).into_response()),
            Ok(None) => Err(ApiError::missing()),
            Err(e) => Err(ApiError::bad_request(e)),
        };
    }
    match crate::repl::start_transient(app, body).await {
        Ok(resp) => Ok(Json(resp).into_response()),
        Err(e) => Err(ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "replication_error",
            e,
        )),
    }
}
