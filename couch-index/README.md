# couch-index

Native Mango JSON indexes for CouchDB databases — the query path with zero
JavaScript and zero Erlang. Indexes are btree files (couch-store's
couch_file layout) living next to the `.couch` database file, updated
incrementally from its changes feed, and queried with full Mango `_find`
semantics: the planner, `is_usable` rules, range extraction and collation
are ported from `mango_idx_view.erl` / `mango_cursor_view.erl`, and every
candidate row is post-filtered with the parity-tested couch-mango selector
engine.

## Usage

```sh
couch-index create db.couch --fields db.DocType,db.CreatedAtMs --name idx_doctype_created
couch-index create db.couch --fields status --partial-filter-selector '{"db.DocType":"task"}'
couch-index list   db.couch
couch-index update db.couch          # bring all indexes to the db's update_seq
couch-index find   db.couch '{"selector": {"db.DocType": "task", "db.CreatedAtMs": {"$gt": 0}},
                              "sort": [{"db.DocType": "desc"}, {"db.CreatedAtMs": "desc"}],
                              "limit": 50, "fields": ["_id", "db.CreatedAtMs"]}'
couch-index find   db.couch @query.json --explain     # plan + rejected indexes
couch-index delete db.couch idx_doctype_created
```

`find` supports selector / limit (default 25, like `_find`) / skip / fields
(dotted-path projection) / sort (all-asc or all-desc, index-order) /
use_index, picks the best usable index by equality-prefix ranking, falls
back to a full scan when nothing fits, and updates the chosen index before
querying unless `--stale` is passed.

## Semantics

- **JSON index rules match CouchDB**: only docs containing *all* indexed
  fields are indexed (null counts, missing doesn't); an index is usable
  only if the selector requires all its fields; sort fields must be a
  prefix of the columns (constant-prefix skipping supported);
  `partial_filter_selector` indexes a subset and is only chosen when the
  query selector contains the filter clauses.
- **Correctness comes from post-filtering**: index ranges narrow the scan,
  the full selector decides membership — the same contract as
  mango_cursor_view.
- Index keys collate like `couch_ejson_compare` (null < false < true <
  number < string < array < object) via an order-preserving key encoding;
  string order within a type is codepoint order, not ICU (same documented
  divergence as couch-repl's selector ranges; equality unaffected).

## Verification & performance

Oracle-tested against CouchDB 3.5.1 on a 100k-doc dataset shaped like the
prime consumer's (nxguide) schema, with its exact 7 indexes created in both
engines: 16 query shapes — equality, compound ranges, `$in`,
`$elemMatch`-based ownership `$or`, null values, desc sort, projection,
skip/limit — returned **identical results** from `_find` and couch-index.

Same host, same data (best/median of 5):

| | CouchDB 3.5.1 | couch-index | speedup |
| --- | --- | --- | --- |
| build 7 nxguide indexes (100k docs) | ~76 s (5–30 s each) | 19 s (1.9–4.9 s each) | ~4× |
| point lookup | 5.8 ms | 4.5 ms | 1.3× |
| 2k-doc query | 728 ms | 178 ms | 4.1× |
| 20k-doc query | 6.9 s | 1.7 s | 4.0× |
| sorted 50-doc page (desc) | 24.7 ms | 8.8 ms | 2.8× |

couch-index numbers include full process start + file open per query;
a resident server would cut the small-query latencies further.

## Files

`<db>.couch.indexes/<name>.fidx` — a couch-store block file with a key
btree (composite key + doc id), an id btree (doc id → its keys, for
incremental cleanup), and a header carrying the definition, the source db
uuid, and the update_seq the index has seen. Updating folds
`_changes since=<seq>` exactly like `couch_index_updater`.
