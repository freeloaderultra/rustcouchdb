# rustcouchdb — CouchDB-compatible server, pure Rust, no Erlang, no JavaScript.
#
#   docker build -t rustcouchdb .
#   docker run -p 5984:5984 -v rustcouchdb-data:/data \
#     -e COUCH_HTTP_ADMIN=admin:password rustcouchdb
#
# The image contains two binaries: `couch-http` (the server: storage, Mango
# queries, _replicator with native selector filtering, auto-compaction) and
# `couch-repl` (standalone replicator CLI, also embedded in the server).

FROM rust:1-slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p couch-http -p couch-repl

FROM debian:bookworm-slim
COPY --from=build /src/target/release/couch-http /usr/local/bin/couch-http
COPY --from=build /src/target/release/couch-repl /usr/local/bin/couch-repl
RUN useradd -r -d /data rustcouchdb && mkdir -p /data && chown rustcouchdb /data
USER rustcouchdb
VOLUME /data
EXPOSE 5984
# Admin credentials (user:password) — override in production.
ENV COUCH_HTTP_ADMIN=""
ENTRYPOINT ["couch-http"]
CMD ["--data-dir", "/data", "--listen", "0.0.0.0:5984", "--soft-delete-validator"]
