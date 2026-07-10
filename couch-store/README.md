# couch-store

A native Rust storage engine for CouchDB `.couch` files — a from-scratch port
of `couch_file`, `couch_btree`, `couch_key_tree` and the `couch_bt_engine`
disk formats. It reads and writes real shard files, byte-compatible with
CouchDB 3.x (disk format v8; v6/v7 readable): CouchDB opens, updates,
compacts and replicates out of files couch-store wrote, and couch-store reads
files CouchDB wrote — verified both directions against a live CouchDB 3.5.1.

This is the second module (after [couch-repl](../couch-repl)) of the
Rust-only CouchDB stack: zero Erlang, zero JavaScript in the deliverable.

## What's implemented

- **File layer** (`file.rs`): 4096-byte block layout with boundary markers,
  plain and checksummed chunks, header scan-from-EOF. Verifies both XXH3-128
  (3.4+ default) and MD5 (legacy) checksums; writes MD5, which every CouchDB
  release accepts.
- **Term codec** (`etf.rs`): the Erlang External Term Format subset CouchDB
  uses, including zlib-compressed terms and legacy encodings; `compress.rs`
  adds the couch_compress framing (snappy by default, deflate readable,
  zstd rejected with a clear error).
- **B+tree** (`btree.rs`): lookup, ordered fold with start key, and
  copy-on-write `add_remove` with the couch_btree chunkify policy and the
  id/seq-tree reduction functions (doc counts and size accounting).
- **Rev trees** (`revtree.rs`): the couch_key_tree structure with
  merge (new_edits:false semantics), stemming, winner election
  (`{not deleted, rev}` descending) and full path extraction.
- **Read engine** (`db.rs`, `doc.rs`): db info, all_docs, changes since seq,
  specific-rev/winner doc reads with `_revisions`/`_conflicts`, `_local`
  docs, `_security`, attachment streams (gzip-encoded attachments decoded
  exactly like the HTTP API serves them).
- **Write engine** (`writer.rs`): create or append to a shard file —
  attachment streams, checksummed doc summaries, rev-tree merge, id/seq
  btree updates and the header commit protocol (fsync, header, fsync).

## CLI

```sh
couch-store info    db.couch                  # counts, seqs, sizes as JSON
couch-store dump    db.couch [--deleted --conflicts --revs --attachments]
couch-store get     db.couch DOCID [--rev R --revs --conflicts --attachments]
couch-store changes db.couch [--since N]
couch-store local   db.couch                  # _local docs (checkpoints!)
couch-store att     db.couch DOCID NAME > file
couch-store security db.couch
couch-store verify  db.couch                  # walk + checksum everything
couch-store create  new.couch --from docs.ndjson   # build a shard from JSON
couch-store append  db.couch  --from more.ndjson   # merge more docs in
```

`create`/`append` take one JSON doc per line and honor `_rev`, `_revisions`
(new_edits:false merge — conflicts and tombstones representable), `_deleted`,
inline `_attachments` data, and `_local/` ids. Docs without a rev get a
deterministic generated `1-` rev.

A clustered database with `q` shards is `q` of these files; per-shard doc
counts and changes sum to the database totals (`_all_docs` order requires a
merge across shards — trivial since each file's id tree is already sorted).

## Verification (against CouchDB 3.5.1, aarch64)

- **Oracle read test**: a database with conflicts (multi-branch rev trees),
  deletions, deep rev histories, unicode/escape/number edge cases, gzipped
  and identity attachments, `_local` docs and a `_security` object was
  created via HTTP; couch-store read the shard file directly and produced
  **byte-identical JSON** to the HTTP API for every doc (with
  `revs/conflicts/attachments`), every leaf rev, the changes feed, local
  docs, security and raw attachment bytes — 34/34 checks.
- **Server readback test**: couch-store wrote a 2000+ doc file (same edge
  cases) which was installed as a shard; CouchDB served every doc/conflict/
  tombstone/attachment identically to the input, then **wrote into the
  file, compacted it** (a full structural walk + rewrite by the Erlang
  engine), and **replicated 2008 docs out of it** — all clean, and
  couch-store reads the post-compaction file back.
- Unit tests port the tricky semantics: block-marker round trips at every
  offset, btree modify orderings, key-tree merges of stemmed paths,
  winner election with deleted branches.

## Performance (96-core aarch64 VM, same host as the server)

| operation | couch-store | CouchDB 3.5.1 |
| --- | --- | --- |
| walk + verify 200k docs (233 MB file) | 1.5 s | — |
| dump 200k docs to NDJSON (211 MB) | 2.6 s | — |
| ingest 200k × 1 KB docs | **8.2 s** (24k docs/s, `create`) | 21 s over HTTP (`_bulk_docs`, batched) |

Same file format, no server between you and the disk.

## Not yet

- zstd `file_compression` (CouchDB writes snappy by default; snappy/deflate/
  none all supported).
- Purge trees are read (purge_seq) but not written.
- No compactor — rewrite via `dump | create`, or let CouchDB compact.
- Views/indexes: `.view` files use the same couch_file/couch_btree layer
  this crate implements; a native Mango index on top of it is the natural
  next module (couch-repl already has the selector engine).

## Build

```sh
cargo build --release   # pure Rust, no C deps beyond bundled miniz/snap
cargo test
```
