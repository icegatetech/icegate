# syntax=docker/dockerfile:1.7
FROM rust:bookworm AS builder
ARG PROFILE=release
WORKDIR /app
# `protobuf-compiler` provides `protoc`; `libprotobuf-dev` ships the
# well-known `.proto` files (any.proto, empty.proto, ...) imported by
# the `substrait` build script (transitive of
# `datafusion-flight-sql-server`).
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
ENV RUSTFLAGS="--cfg tokio_unstable"

COPY Cargo.toml Cargo.lock ./
COPY ./crates ./crates

# The cache mounts are what makes an incremental rebuild possible: they survive
# layer invalidation, so cargo keeps `target/` fingerprints and the downloaded
# registry/git sources across builds. A touched crate — or a bumped `jobmanager`
# rev — then recompiles only that crate and its reverse dependencies instead of
# the whole graph. `sharing=locked` serialises concurrent service builds, which
# would otherwise race on the same `target/`.
#
# A mount is not part of the resulting image, so the binaries must be copied out
# of `target/` inside this same RUN.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build --locked $([ "$PROFILE" = "release" ] && echo "--release") \
        --workspace --bins --features icegate-catalog-s3/rest \
    && mkdir -p /out \
    && cp target/$PROFILE/query target/$PROFILE/ingest \
          target/$PROFILE/maintain target/$PROFILE/catalog /out/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/ /usr/local/bin/
USER nobody
ENTRYPOINT ["/bin/sh", "-c", "exec \"$@\"", "--"]
