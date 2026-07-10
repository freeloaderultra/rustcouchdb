//! /_utils — the embedded admin UI: a single self-contained page (no external
//! assets, no build step) compiled into the binary. A Fauxton-style interface
//! covering exactly what this server supports: session login, databases,
//! document browsing/editing, Mango queries, index management, `_replicator`
//! status/creation and `_active_tasks`. Cluster and multi-user administration
//! don't exist here, so the UI has no trace of them.
//!
//! Served for `/_utils` and everything below it (the shell routes via the URL
//! hash, so every path gets the same page; trailing slashes are already
//! stripped by the router middleware).

use axum::http::header;
use axum::response::{IntoResponse, Response};

const INDEX_HTML: &str = include_str!("../ui/index.html");

pub async fn utils() -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        INDEX_HTML,
    )
        .into_response()
}
