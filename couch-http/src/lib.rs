//! rustcouchdb server library: the CouchDB HTTP API on couch-store +
//! couch-index, with couch-repl embedded. The binary in main.rs is a thin
//! CLI around `router` + `ServerState`.

pub mod api_bulk;
pub mod api_changes;
pub mod api_db;
pub mod api_docs;
pub mod api_mango;
pub mod api_node;
pub mod api_repl;
pub mod api_root;
pub mod auth;
pub mod error;
pub mod jsfilter;
pub mod metrics;
pub mod repl;
pub mod state;
pub mod ui;
pub mod util;
pub mod validate;

use axum::extract::{DefaultBodyLimit, Request};
use axum::middleware::Next;
use axum::response::Response;
use axum::routing::{delete, get, post};
use axum::Router;
use state::App;

/// CouchDB clients (the Erlang replicator among them) address databases
/// with a trailing slash (`GET /db/`). Route matching happens before router
/// layers run, so this middleware must wrap the router itself (see `serve`).
async fn strip_trailing_slash(mut req: Request, next: Next) -> Response {
    let uri = req.uri();
    let path = uri.path();
    if path.len() > 1 && path.ends_with('/') {
        let stripped = path.trim_end_matches('/');
        let new = match uri.query() {
            Some(q) => format!("{stripped}?{q}"),
            None => stripped.to_string(),
        };
        if let Ok(new_uri) = new.parse() {
            *req.uri_mut() = new_uri;
        }
    }
    next.run(req).await
}

pub fn router(app: App) -> Router {
    Router::new()
        .route("/", get(api_root::welcome))
        .route("/_up", get(api_root::up))
        .route(
            "/_session",
            get(auth::session_get)
                .post(auth::session_post)
                .delete(auth::session_delete),
        )
        .route("/_all_dbs", get(api_root::all_dbs))
        .route("/_uuids", get(api_root::uuids))
        .route("/_active_tasks", get(api_root::active_tasks))
        .route("/_replicate", post(api_repl::replicate))
        .route("/_utils", get(ui::utils))
        .route("/_utils/{*path}", get(ui::utils))
        .route("/_node/{node}/_prometheus", get(api_node::prometheus))
        .route("/_scheduler/jobs", get(api_root::scheduler_jobs))
        .route("/_scheduler/docs", get(api_root::scheduler_docs))
        .route("/_scheduler/docs/_replicator", get(api_root::scheduler_docs))
        .route(
            "/_scheduler/docs/_replicator/{docid}",
            get(api_root::scheduler_doc),
        )
        .route(
            "/{db}",
            get(api_db::db_info)
                .put(api_db::db_create)
                .delete(api_db::db_delete)
                .post(api_docs::doc_post),
        )
        .route("/{db}/_ensure_full_commit", post(api_db::ensure_full_commit))
        .route("/{db}/_purge", post(api_db::purge))
        .route("/{db}/_compact", post(api_db::compact_db))
        .route("/{db}/_compact/{ddoc}", post(api_db::view_cleanup))
        .route("/{db}/_view_cleanup", post(api_db::accepted_noop))
        .route(
            "/{db}/_security",
            get(api_db::security_get).put(api_db::security_put),
        )
        .route("/{db}/_revs_limit", get(api_db::revs_limit_get))
        .route("/{db}/_shards", get(api_db::shards))
        .route("/{db}/_design_docs", get(api_db::design_docs))
        .route(
            "/{db}/_all_docs",
            get(api_bulk::all_docs_get).post(api_bulk::all_docs_post),
        )
        .route("/{db}/_bulk_docs", post(api_bulk::bulk_docs))
        .route("/{db}/_bulk_get", post(api_bulk::bulk_get))
        .route("/{db}/_revs_diff", post(api_bulk::revs_diff))
        .route("/{db}/_missing_revs", post(api_bulk::missing_revs))
        .route(
            "/{db}/_changes",
            get(api_changes::changes_get).post(api_changes::changes_post),
        )
        .route("/{db}/_find", post(api_mango::find))
        .route("/{db}/_explain", post(api_mango::explain))
        .route(
            "/{db}/_index",
            post(api_mango::index_create).get(api_mango::index_list),
        )
        .route(
            "/{db}/_index/{ddoc}/json/{name}",
            delete(api_mango::index_delete),
        )
        .route(
            "/{db}/_local/{*docid}",
            get(api_docs::local_get)
                .put(api_docs::local_put)
                .delete(api_docs::local_delete),
        )
        .route(
            "/{db}/_design/{name}",
            get(api_docs::design_get)
                .put(api_docs::design_put)
                .delete(api_docs::design_delete),
        )
        .route(
            "/{db}/_design/{name}/{*att}",
            get(api_docs::design_att_get)
                .put(api_docs::design_att_put)
                .delete(api_docs::design_att_delete),
        )
        .route(
            "/{db}/{docid}",
            get(api_docs::doc_get)
                .put(api_docs::doc_put)
                .delete(api_docs::doc_delete),
        )
        .route(
            "/{db}/{docid}/{*att}",
            get(api_docs::att_get)
                .put(api_docs::att_put)
                .delete(api_docs::att_delete),
        )
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024))
        .layer(axum::middleware::from_fn_with_state(
            app.clone(),
            auth::require_admin,
        ))
        // Outermost: sees every request, auth rejections included.
        .layer(axum::middleware::from_fn(metrics::track))
        .with_state(app)
}

/// Serve the API on `listener` until `shutdown` resolves.
pub async fn serve(
    listener: tokio::net::TcpListener,
    app: App,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    use tower::Layer;
    let svc = axum::middleware::from_fn(strip_trailing_slash).layer(router(app));
    axum::serve(listener, axum::ServiceExt::into_make_service(svc))
        .with_graceful_shutdown(shutdown)
        .await
}
