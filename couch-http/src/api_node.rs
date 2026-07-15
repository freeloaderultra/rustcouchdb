//! /_node/{node} endpoints. Single-node server: any node name is accepted
//! and means this node, the way upstream resolves `_local`.

use crate::state::App;
use axum::extract::{Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};

pub async fn prometheus(State(app): State<App>, Path(_node): Path<String>) -> Response {
    (
        [(CONTENT_TYPE, "text/plain; version=2.0")],
        crate::metrics::render(&app),
    )
        .into_response()
}
