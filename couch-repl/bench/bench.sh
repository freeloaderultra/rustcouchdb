#!/usr/bin/env bash
# Benchmark: couch-repl vs CouchDB's built-in (Erlang) replicator.
# Both replicate over HTTP against the same CouchDB server, source db -> target db.
# Usage: bench.sh <couch-base-url-with-creds> <path-to-couch-repl> [runs]
set -euo pipefail

C="${1:?usage: bench.sh http://admin:pass@host:5984 /path/to/couch-repl [runs]}"
BIN="${2:?path to couch-repl binary}"
RUNS="${3:-2}"
CRED_URL="$C" # e.g. http://admin:pass@127.0.0.1:5984

now() { date +%s.%N; }
elapsed() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.1f", b-a}'; }
doc_count() { curl -s "$C/$1" | python3 -c 'import json,sys;print(json.load(sys.stdin)["doc_count"])'; }

set_repl_config() { # key value
  curl -s -X PUT "$C/_node/_local/_config/replicator/$1" -d "\"$2\"" >/dev/null
}

erlang_run() { # src tgt [extra-json-fields]
  local src=$1 tgt=$2 extra="${3:-}" t0 t1
  [ -n "$extra" ] && extra=",$extra"
  curl -s -X DELETE "$C/$tgt" >/dev/null || true
  t0=$(now)
  curl -s -m 7200 -X POST "$C/_replicate" -H content-type:application/json \
    -d "{\"source\":\"$CRED_URL/$src\",\"target\":\"$CRED_URL/$tgt\",\"create_target\":true$extra}" \
    >/tmp/erl_result.json
  t1=$(now)
  local ok
  ok=$(python3 -c 'import json;print(json.load(open("/tmp/erl_result.json")).get("ok"))')
  echo "$(elapsed "$t0" "$t1")s (ok=$ok, tgt_docs=$(doc_count "$tgt"))"
}

rust_run() { # src tgt [extra flags...]
  local src=$1 tgt=$2; shift 2
  curl -s -X DELETE "$C/$tgt" >/dev/null || true
  local t0 t1
  t0=$(now)
  "$BIN" replicate "$C/$src" "$C/$tgt" --create-target "$@" >/tmp/rust_result.log 2>&1
  t1=$(now)
  echo "$(elapsed "$t0" "$t1")s (tgt_docs=$(doc_count "$tgt"))"
}

bench_dataset() { # name src
  local name=$1 src=$2
  echo ""
  echo "=== dataset $name ($(doc_count "$src") docs) ==="

  set_repl_config worker_processes 4
  set_repl_config http_connections 20
  for i in $(seq 1 "$RUNS"); do
    echo "  erlang (default 4 workers/20 conns) run $i: $(erlang_run "$src" "${src}_tgt_erl")"
  done

  set_repl_config worker_processes 16
  set_repl_config http_connections 100
  for i in $(seq 1 "$RUNS"); do
    echo "  erlang (tuned 16 workers/100 conns)  run $i: $(erlang_run "$src" "${src}_tgt_erl")"
  done
  set_repl_config worker_processes 4
  set_repl_config http_connections 20

  for i in $(seq 1 "$RUNS"); do
    echo "  couch-repl (defaults)                run $i: $(rust_run "$src" "${src}_tgt_rust")"
  done
}

# Filtered replication: mango _selector evaluated by the server (Erlang
# replicator) vs couch-repl's native Rust selector evaluation. Docs need the
# "group" field that `couch-repl gen` writes (0..99).
bench_filtered() { # src
  local src=$1
  local sel='{"group":{"$lt":10}}'
  echo ""
  echo "=== filtered dataset $src ($(doc_count "$src") docs, filter passes 10%) ==="

  set_repl_config worker_processes 4
  set_repl_config http_connections 20
  for i in $(seq 1 "$RUNS"); do
    echo "  erlang + server _selector             run $i: $(erlang_run "$src" "${src}_tgt_erlsel" "\"selector\":$sel")"
  done
  for i in $(seq 1 "$RUNS"); do
    echo "  couch-repl --selector (native rust)   run $i: $(rust_run "$src" "${src}_tgt_rsel" --selector "$sel")"
  done
}

bench_dataset A bench_a
bench_dataset B bench_b
bench_filtered bench_a
