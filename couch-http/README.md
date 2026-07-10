# couch-http

The rustcouchdb server: CouchDB's HTTP API served by pure Rust — couch-store
shard files underneath, couch-index for Mango queries, couch-repl embedded
for `_replicator` jobs. No Erlang, no JavaScript, one binary.

```sh
couch-http --data-dir ./data --listen 0.0.0.0:5984 \
  --admin admin:password --soft-delete-validator
```

or as a container (multi-arch, ~160 MB, no runtime deps):

```sh
docker build -t rustcouchdb .     # repo root
docker run -p 5984:5984 -v rustcouchdb-data:/data \
  -e COUCH_HTTP_ADMIN=admin:password rustcouchdb
```

## What it serves

- **Server**: `/`, `_up`, `_all_dbs`, `_uuids`, `_session` (cookie + basic
  auth, single admin), `_active_tasks`, `_scheduler/jobs`,
  `_scheduler/docs[/_replicator/{id}]`
- **Databases**: create/delete/info, `_security`, `_compact` (plus a
  smoosh-style auto-compaction daemon), `_ensure_full_commit`,
  `_revs_limit`, `_design_docs`, `_shards`
- **Documents**: full interactive semantics (conflict checks, deterministic
  new revids, attachment stub inheritance), `new_edits=false` replicated
  writes, `open_revs` (JSON **and** `multipart/mixed`), `latest`/`revs`/
  `conflicts`/`atts_since`, attachments (GET/PUT/DELETE, `multipart/related`
  doc PUT), `_local` docs
- **Batch/replication**: `_bulk_docs`, `_bulk_get`, `_revs_diff`,
  `_missing_revs`, `_all_docs` (ranges, keys, include_docs), `_changes`
  (normal/longpoll/continuous, `style=all_docs`, `_selector` and `_doc_ids`
  filters, heartbeats)
- **Mango**: `_index` create/list/delete, `_find`, `_explain` via
  couch-index (indexes update lazily before each query, like CouchDB)
- **`_replicator`**: docs become embedded couch-repl jobs — selector
  filtering, `winning_revs_only`, `create_target`, continuous with
  supervised restarts; states written back to the doc, live stats in
  `_scheduler/docs` and `_active_tasks`
- **validate_doc_update, natively**: `--soft-delete-validator` enforces
  nxguide's soft-delete metadata rule as compiled Rust; the JS design doc is
  accepted and stored as inert data.

## Verification (against CouchDB 3.5.1, aarch64)

- **59/59 HTTP parity checks**: both servers seeded with byte-identical docs
  (deterministic revs, conflict trees, tombstones, deep histories, unicode,
  number edge cases, attachments), then every read endpoint compared —
  document reads in all option combinations, `_all_docs` ranges,
  `_changes` with filters and styles, `_revs_diff`, `_bulk_get`,
  attachment bytes, interactive status-code flows, `_local`, `_security`,
  14 Mango `_find` shapes on 4 000 nxguide-shaped docs with nxguide's real
  7 indexes, and the soft-delete validator vs the real JS one.
- **11/11 replication interop checks**: couch-repl in both directions
  (fingerprint-identical rev trees, conflicts, tombstones, attachments);
  the **stock Erlang replicator pushing into and pulling from rustcouchdb**
  (including `multipart/mixed` open_revs with `atts_since`); the embedded
  `_replicator` completing a selector + `winning_revs_only` job with a
  conflict-free target.

Known divergences: CouchDB gzips `text/*` attachments at rest so its stub
`digest` is over the encoded bytes (served bytes are identical); `update_seq`
grows per doc-batch member in a different order than fabric's grouping
(seqs are opaque tokens; resume semantics verified equivalent).

## Performance (same host, HTTP vs HTTP, 100k nxguide-shaped docs)

Ingest 3.3× (14.6k docs/s), index builds 2.2×, `_find` 1.7–2.7×,
`_changes` drain 4.2× — full table in the repo README.

## nxguide notes

kivik v3's cookie auth (`POST /_session`), its 7 `_index` definitions, all
`_find` query shapes, blob attachments, `_replicator` docs with the
ownership `$or` selector + `winning_revs_only`, `_scheduler/docs` polling
and `_active_tasks` are covered by the suites above. `EnsureSoftDeleteValidator`
can keep PUT-ing its design doc (stored, never executed) — run the server
with `--soft-delete-validator` for the same enforcement natively.
