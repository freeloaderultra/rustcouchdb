# couch-repl

A standalone, high-throughput CouchDB replicator in Rust. It speaks the
CouchDB Replication Protocol over HTTP against existing CouchDB servers
(`_changes` → `_revs_diff` → `_bulk_get` → `_bulk_docs`, `_local` checkpoint
docs), so nothing changes on your servers — it is a drop-in alternative to
`POST /_replicate` for jobs where the built-in Erlang replicator is too slow.

## Why it is faster

- 32 concurrent `_bulk_get` and 8 concurrent `_bulk_docs` requests in flight
  (vs. 4 workers doing fetch→write serially in the Erlang replicator), all
  stages pipelined through bounded channels with end-to-end backpressure.
- Document bodies pass source→target as raw JSON slices — parsed once at the
  envelope level, never re-serialized.
- Docs with small attachments stay on the bulk path (inline base64); large
  attachments stream chunk-by-chunk through a multipart/related PUT with
  constant memory, 16 docs at a time (the Erlang replicator PUTs every
  attachment doc individually).

## Build

```sh
cargo build --release        # needs rustls only, no OpenSSL; fine on ARM
```

## Usage

```sh
# one-shot
couch-repl replicate https://user:pass@src:5984/db https://user:pass@tgt:5984/db --create-target

# continuous (ctrl-c drains in-flight work and writes a final checkpoint)
couch-repl replicate $SRC $TGT --continuous

# filtered — the selector runs natively in couch-repl, never on the server
couch-repl replicate $SRC $TGT --doc-ids a,b,c
couch-repl replicate $SRC $TGT --selector '{"type":"order"}'

# where is my checkpoint?
couch-repl id $SRC $TGT

# benchmark dataset generator
couch-repl gen http://admin:pass@host:5984/bench_a --docs 200000 --doc-kb 1

# server mode: run many supervised jobs, controlled over an HTTP API
couch-repl serve --listen 127.0.0.1:7984 --config jobs.json
```

## Server mode

`couch-repl serve` runs any number of replication jobs concurrently, restarts
failed jobs with exponential backoff (30 s → 5 min cap), and exposes a small
HTTP API. Jobs can be declared in a JSON config file and/or managed at runtime:

```json
{ "jobs": [ { "name": "live-mirror",
              "source": "http://user:pass@a:5984/db",
              "target": "http://user:pass@b:5984/db",
              "continuous": true, "create_target": true } ] }
```

| Endpoint | Meaning |
| --- | --- |
| `GET /_up` | health: `{"status":"ok","jobs":N}` |
| `GET /_jobs` | all jobs with state (`running`/`retrying`/`completed`/`failed`/`cancelled`) and live stats counters |
| `GET /_jobs/{name}` | one job |
| `POST /_jobs` | add a job (same fields as the config file); 409 if the name is already running |
| `DELETE /_jobs/{name}` | cancel a job (drains in-flight work, writes a final checkpoint) |

Job fields beyond the four above: `since`, `doc_ids`, `selector`,
`fetch_concurrency`, `write_concurrency`, `att_concurrency`, `batch_size`,
`max_batch_bytes`, `inline_att_threshold`, `checkpoint_interval_ms`,
`no_checkpoints`, `no_bulk_get`, `timeout_secs`, `max_retries`,
`continue_on_error`, `insecure`, `changes_limit`.

Credentials never appear in API responses. The API itself has no auth — bind
it to loopback (the default) or firewall it. SIGINT cancels all jobs
gracefully and stops the server.

Replication is resumable: checkpoints are written to `_local/<rep-id>` docs on
both endpoints every `--checkpoint-interval` (default 30 s) and on shutdown.
A SIGKILL costs at most the un-checkpointed window; the rerun re-verifies via
`_revs_diff`, which is idempotent because all writes use `new_edits:false`.
Conflicts, deletions, and full revision histories are preserved
(`style=all_docs`, `revs=true`, `latest=true`).

Tuning knobs: `--fetch-concurrency`, `--write-concurrency`, `--att-concurrency`,
`--batch-size`, `--max-batch-bytes`, `--inline-att-threshold`. Defaults are
already aggressive; raise them for high-latency links.

## Filtered replication — native Rust, zero JavaScript

CouchDB's classic filtered replication pipes every change through an external
`couchjs` process (the JS query server), which caps filtered feeds at a few
hundred rows per second regardless of hardware. couch-repl contains no
JavaScript at all: `--selector` takes a standard Mango selector and evaluates
it **inside couch-repl** with a native Rust port of the server's
`mango_selector` engine (`src/mango.rs`). The source server never runs a
filter — no couchjs, no per-row server-side matching, no server CPU spent on
filtering. The selector is applied in the fetch stage, to every revision that
comes back from the 32-way-concurrent `_bulk_get`, so the changes feed stays
lean and body reads are spread across all fetch connections instead of being
serialized through a filtered feed.

- Full operator support: `$eq/$ne/$lt/$lte/$gt/$gte`, `$in/$nin`, `$exists`,
  `$type`, `$size`, `$mod`, `$regex`, `$beginsWith`, `$all`, `$elemMatch`,
  `$allMatch`, `$keyMapMatch`, `$and/$or/$not/$nor`, dotted paths with array
  indexing and `\.` escapes — verified field-by-field against CouchDB's own
  `_selector` results (19-selector parity suite, identical doc sets).
- Filtered-out revisions complete immediately in the sequence ledger, so
  checkpoints advance through skipped regions at full speed and resume works
  exactly as in unfiltered mode. The replication id includes the selector, so
  changing it starts a fresh checkpoint lineage.
- Semantics notes: the selector is applied per leaf revision (like the
  server's `style=all_docs` filtered changes); deletions are
  `{_id,_rev,_deleted}` stubs and typically do not match typed selectors —
  the same caveat CouchDB's own filtered replication has. Two deliberate
  divergences from the server: string *ordering* for range operators uses
  Unicode codepoints, not ICU collation (equality is unaffected), and
  `$regex` uses Rust regex syntax instead of PCRE (backreferences/lookaround
  are rejected up front).

To migrate a JS filter, express its predicate as a Mango selector. If a
predicate genuinely cannot be a selector, replicate unfiltered — with the
throughput below, full replication is usually still faster than couchjs.

## Not in v1 (by design)

- JS/design-doc filter functions and `_view` filters — deliberately, not as a
  gap: JS filtering is the slow path this tool replaces. Use `--selector` /
  `--doc-ids`.
- `_replicator` db integration and multi-job scheduling — one job per process;
  use systemd or a supervisor for daemons.
- Checkpoint interop with the Erlang replicator (couch-repl uses its own
  replication-id scheme, `rust-1`; the two never clobber each other).
- Cookie/session auth (basic auth via URL userinfo, plus arbitrary
  `--source-header`/`--target-header`).

## Benchmark

`bench/bench.sh <url-with-creds> <couch-repl-binary> [runs]` replicates
generated datasets source-db → target-db on the same server with the built-in
replicator (default and tuned configs) and with couch-repl, and prints wall
times.

Measured 2026-07-10 against CouchDB 3.5.1 (official docker image) on a 96-core
aarch64 Ubuntu 24.04 VM, source and target databases on the same server over
loopback (best of 2 runs each):

| | Erlang default (4 workers / 20 conns) | Erlang tuned (16 / 100) | couch-repl (defaults) |
| --- | --- | --- | --- |
| A: 200,000 × 1 KB docs | 27.1 s (7.4k docs/s) | 24.5 s (8.2k docs/s) | **17.1 s (11.7k docs/s)** |
| B: 2,000 docs × 3 × 200 KB attachments (1.2 GB) | 15.0 s (80 MB/s) | 13.7 s (88 MB/s) | **12.4 s (97 MB/s)** |

On dataset A couch-repl is 1.6× the default Erlang replicator — and its 11.7k
docs/s equals this server's raw `_bulk_docs` ingest rate, i.e. the target
CouchDB, not the replicator, is now the bottleneck. On dataset B all
contenders converge on the server's attachment I/O ceiling (~90 MB/s over
loopback). The gap widens on real deployments where network latency exists:
the Erlang replicator's 4 serial workers stall on round-trips, while
couch-repl keeps 32 fetches and 8 writes in flight.

Filtered replication (dataset A, selector `{"group":{"$lt":10}}` passing 10%
= 20,000 docs; the JS baseline is the equivalent design-doc filter function
executed by the server's couchjs query server):

| | time | vs couch-repl |
| --- | --- | --- |
| Erlang replicator + JS filter (couchjs) | 404.5–407.6 s (~490 changes/s) | 32× slower |
| Erlang replicator + server-side `_selector` | 17.5–18.5 s | 1.4× slower |
| **couch-repl `--selector` (native Rust)** | **12.5–13.4 s** | — |

The JS row is the path this feature replaces: the query server grinds through
every change at couchjs speed no matter the hardware. couch-repl filters
200k revisions natively in ~1 s of CPU spread across the fetch pool and is
bounded by transfer + write speed instead.

## Correctness notes

- `source_last_seq` only ever advances to the highest *contiguous* completed
  change (see `src/seq.rs`); out-of-order batch completions can never cause a
  checkpoint to skip unreplicated changes.
- Attachment digests are preserved end-to-end and verified by the target,
  except for attachments stored gzip-encoded at the source (the digest covers
  the encoded form; couch-repl transfers identity bytes and lets the target
  recompute).
