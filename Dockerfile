# rustcouchdb — CouchDB-compatible server, pure Rust, no Erlang, no JavaScript.
#
#   docker buildx build --platform linux/amd64,linux/arm64 -t rustcouchdb .
#   docker run -p 5984:5984 -v rustcouchdb-data:/data \
#     -e COUCH_HTTP_ADMIN=admin:password rustcouchdb
#
# The image contains two binaries: `couch-http` (the server: storage, Mango
# queries, _replicator with native selector filtering, auto-compaction) and
# `couch-repl` (standalone replicator CLI, also embedded in the server).
#
# The build stage always runs on the build host's native platform and
# cross-compiles for $TARGETARCH, so multi-arch builds need no emulation
# (ring is the only C dependency; the cross gcc covers it).

FROM --platform=$BUILDPLATFORM rust:1-slim AS build
ARG TARGETARCH
# Release builds default to max optimization (fat LTO, one codegen unit).
# For fast development iteration pass e.g.
#   --build-arg CARGO_PROFILE_RELEASE_LTO=false \
#   --build-arg CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
ARG CARGO_PROFILE_RELEASE_LTO=true
ARG CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
ENV CARGO_PROFILE_RELEASE_LTO=$CARGO_PROFILE_RELEASE_LTO \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=$CARGO_PROFILE_RELEASE_CODEGEN_UNITS
WORKDIR /src
COPY . .
RUN set -eux; \
    gccbin=gcc; \
    case "$TARGETARCH" in \
      amd64) target=x86_64-unknown-linux-gnu ;; \
      arm64) target=aarch64-unknown-linux-gnu ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    if [ "$TARGETARCH" != "$(dpkg --print-architecture)" ]; then \
      apt-get update; \
      case "$TARGETARCH" in \
        amd64) apt-get install -y --no-install-recommends gcc-x86-64-linux-gnu libc6-dev-amd64-cross; \
               gccbin=x86_64-linux-gnu-gcc; \
               export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
                      CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc ;; \
        arm64) apt-get install -y --no-install-recommends gcc-aarch64-linux-gnu libc6-dev-arm64-cross; \
               gccbin=aarch64-linux-gnu-gcc; \
               export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
                      CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc ;; \
      esac; \
      rm -rf /var/lib/apt/lists/*; \
      rustup target add "$target"; \
    fi; \
    cargo build --release --target "$target" -p couch-http -p couch-repl; \
    mkdir /out; \
    cp "target/$target/release/couch-http" "target/$target/release/couch-repl" /out/; \
    cp "$("$gccbin" -print-file-name=libgcc_s.so.1)" /out/

# busybox:glibc has glibc + NSS/DNS libs; the binaries additionally need
# libgcc_s (unwinding), copied from the build sysroot. TLS roots are compiled
# in (webpki-roots), so no ca-certificates package is required.
FROM busybox:glibc
COPY --from=build /out/libgcc_s.so.1 /lib/
COPY --from=build /out/couch-http /out/couch-repl /usr/local/bin/
RUN adduser -D -H -h /data rustcouchdb && mkdir -p /data && chown rustcouchdb /data
USER rustcouchdb
VOLUME /data
EXPOSE 5984
# Admin credentials (user:password) — override in production.
ENV COUCH_HTTP_ADMIN=""
ENTRYPOINT ["couch-http"]
CMD ["--data-dir", "/data", "--listen", "0.0.0.0:5984", "--soft-delete-validator"]
