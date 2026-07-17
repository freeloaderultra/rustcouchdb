# rustcouchdb

A Rust-native, CouchDB-compatible database stack. No Erlang, no JavaScript —
the couchjs query server and the Erlang runtime are replaced module by
module with Rust that speaks the same protocols and the same on-disk format.

## Modules

| crate | what it is | status |
| --- | --- | --- |
| [`couch-repl`](couch-repl/) | Standalone replicator speaking the CouchDB Replication Protocol over HTTP, with **native Mango selector filtering** (no couchjs). One-shot, continuous, and supervised server mode. | benchmarked: 1.6× the Erlang replicator unfiltered, **32× faster than JS-filtered replication** |
| [`couch-store`](couch-store/) | Storage engine for `.couch` shard files — couch_file / couch_btree / couch_key_tree ported to Rust, read **and** write, plus a compactor. | verified bidirectionally against CouchDB 3.5.1 (server opens, updates, compacts and replicates out of Rust-written files — and reads files couch-store compacted); bulk ingest ~2.5× the server's HTTP rate |
| [`couch-index`](couch-index/) | Mango JSON indexes on couch-store btrees: planner, incremental updater and `_find` execution ported from the mango application. Plus **spatial (bounding-box) indexes** — a rustcouchdb extension CouchDB never had natively: linear-quadtree keys on the same btrees, chosen automatically for plain Mango bbox-range selectors. | oracle-tested vs CouchDB `_find` (identical results on an nxguide-shaped 100k-doc suite); index builds ~4× faster, queries 1.3–4× faster; bbox queries 228× faster than the doctype-index scan they replace (50k docs: 7 ms vs 1.7 s, identical results) |
| [`couch-mango`](couch-mango/) | Shared library: the Mango selector engine and EJSON collation (used by couch-repl filtering and couch-index keys/post-filtering). | 19-selector parity suite vs CouchDB |
| [`couch-http`](couch-http/) | **The rustcouchdb server**: CouchDB's HTTP API in one Rust binary — docs, attachments, `_changes`, `_find`/`_index`, `_replicator`/`_scheduler` (embedded couch-repl), cookie auth, native validate_doc_update, auto-compaction. Ships as a ~160 MB no-dependency container. | 59/59 HTTP parity checks vs CouchDB 3.5.1; 11/11 replication interop incl. the stock Erlang replicator pushing to **and** pulling from it |

All crates build from the workspace with `cargo build --release` (rustls only, no
OpenSSL; runs on ARM Linux).

## Compatibility

- Replication interoperates with any CouchDB 2.x/3.x server — `couch-repl`
  is a drop-in alternative to `POST /_replicate`.
- `couch-store` reads and writes CouchDB 3.x disk format v8 (v6/v7
  readable): a file written here can be installed as a shard of a live
  CouchDB, and vice versa.
- Mango selectors are evaluated with a clause-by-clause port of the
  server's `mango_selector` — parity-tested against CouchDB's own
  `_selector` filtering (19-selector suite, identical results).
- Spatial indexes need **no new query syntax**: `POST /_index` with
  `"type": "spatial"` and the four west/south/east/north document paths;
  the planner then serves ordinary Mango `$gte`/`$lte` bbox-intersection
  selectors (the pattern apps already send to stock CouchDB) from the
  quadtree instead of a full scan. The definition lives in a
  `language: "query"` design doc section that stock CouchDB ignores, so
  databases carrying spatial indexes still replicate to Erlang peers.
- Attachments stream disk-to-disk in both directions like couch_stream:
  uploads (raw PUT and replicator multipart) spool to a temp file and
  land in the .couch file in 1 MiB chunks; downloads stream the stored
  chunk list. A 300 MB attachment transfer holds the server around
  25–50 MB RSS. Size is unbounded (stock's max_attachment_size default).
- HTTP gzip, negotiated per request (stock CouchDB compresses neither
  direction of replication traffic). Responses compress only for clients
  sending `Accept-Encoding: gzip` (`feed=continuous` always stays identity
  so heartbeats flow); `Content-Encoding: gzip` request bodies are
  inflated like chttpd does. `couch-repl` compresses `_bulk_docs`,
  `_bulk_get` and compressible-attachment uploads only after the peer's
  welcome message proves support (the `"gzip"` feature flag, or stock
  CouchDB by vendor) — servers without it, older rustcouchdb included,
  receive byte-identical requests to before. ~3.5× smaller transfers on
  coordinate-heavy documents. Compressible types extend stock's list
  (text/*, json, xml, javascript) with protobuf — compact wire format
  but not compressed; gzip measures 1.6–3.4× on real payloads. Transport
  only: stored attachment bytes are never altered.

## Server performance

Same host (96-core aarch64), HTTP vs HTTP, 100k nxguide-shaped documents,
nxguide's 7 Mango indexes, medians of 5:

| operation | CouchDB 3.5.1 | rustcouchdb | speedup |
| --- | --- | --- | --- |
| bulk ingest 100k docs | 22.7 s | **6.9 s** (14.6k docs/s) | 3.3× |
| build all 7 indexes | 34.1 s | **15.4 s** | 2.2× |
| `_find` point lookup | 11.8 ms | **6.9 ms** | 1.7× |
| `_find` 8.3k-row range | 2.30 s | **0.84 s** | 2.7× |
| `_find` sorted 50-row page | 20.9 ms | **10.2 ms** | 2.0× |
| `_find` ownership query (500 rows) | 138 ms | **54 ms** | 2.5× |
| `_changes` full drain (100k rows) | 5.5 s | **1.3 s** | 4.2× |

## Roadmap

1. ~~Replication (protocol, checkpoints, attachments, server mode)~~ ✓
2. ~~Filtered replication without JavaScript~~ ✓
3. ~~Storage engine (shard files, B+trees, rev trees, compaction)~~ ✓
4. ~~Native indexes: Mango indexes as B+trees on couch-store, reusing the
   selector engine — queries without couchjs~~ ✓
5. ~~HTTP API layer on top of couch-store + couch-index (the server)~~ ✓
6. Clustering stays out of scope: single node + replication topologies
   instead of fabric/mem3 quorum sharding

## License

Apache-2.0, derived from and interoperating with
[Apache CouchDB](https://github.com/apache/couchdb).
