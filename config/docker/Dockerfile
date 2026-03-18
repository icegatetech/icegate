# syntax=docker/dockerfile:1.7
FROM rust:bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates/icegate-common/Cargo.toml crates/icegate-common/Cargo.toml
COPY crates/icegate-common/src/lib.rs crates/icegate-common/src/lib.rs
COPY crates/icegate-ingest/Cargo.toml crates/icegate-ingest/Cargo.toml
COPY crates/icegate-ingest/src/lib.rs crates/icegate-ingest/src/lib.rs
COPY crates/icegate-ingest/src/main.rs crates/icegate-ingest/src/main.rs
COPY crates/icegate-jobmanager/Cargo.toml crates/icegate-jobmanager/Cargo.toml
COPY crates/icegate-jobmanager/src/lib.rs crates/icegate-jobmanager/src/lib.rs
COPY crates/icegate-maintain/Cargo.toml crates/icegate-maintain/Cargo.toml
COPY crates/icegate-maintain/src/lib.rs crates/icegate-maintain/src/lib.rs
COPY crates/icegate-maintain/src/main.rs crates/icegate-maintain/src/main.rs
COPY crates/icegate-query/Cargo.toml crates/icegate-query/Cargo.toml
COPY crates/icegate-query/src/lib.rs crates/icegate-query/src/lib.rs
COPY crates/icegate-query/src/main.rs crates/icegate-query/src/main.rs
COPY crates/icegate-query/benches/loki_queries.rs crates/icegate-query/benches/loki_queries.rs
COPY crates/icegate-query/benches/common/mod.rs crates/icegate-query/benches/common/mod.rs
COPY crates/icegate-query/benches/common/harness.rs crates/icegate-query/benches/common/harness.rs
COPY crates/icegate-queue/Cargo.toml crates/icegate-queue/Cargo.toml
COPY crates/icegate-queue/src/lib.rs crates/icegate-queue/src/lib.rs
COPY crates/icegate-queue/benches/queue_s3_bench.rs crates/icegate-queue/benches/queue_s3_bench.rs
COPY crates/icegate-queue/benches/common/mod.rs crates/icegate-queue/benches/common/mod.rs
COPY crates/icegate-queue/benches/common/data.rs crates/icegate-queue/benches/common/data.rs
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG PROFILE=release
ARG BINARY

COPY --from=planner /app/recipe.json recipe.json
ENV RUSTFLAGS="--cfg tokio_unstable"
ENV CARGO_TARGET_DIR=/app/target
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/git/checkouts \
    --mount=type=cache,target=/app/target \
    cargo chef cook \
      $([ "$PROFILE" = "release" ] && echo "--release") \
      --recipe-path recipe.json
COPY ./crates ./crates
COPY Cargo.toml Cargo.lock ./
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/git/checkouts \
    --mount=type=cache,target=/app/target \
    test -n "${BINARY}" && \
    cargo build \
      $([ "$PROFILE" = "release" ] && echo "--release") \
      --bin "${BINARY}" && \
    case "${PROFILE}" in \
      release) CARGO_PROFILE_DIR=release ;; \
      *) CARGO_PROFILE_DIR=debug ;; \
    esac && \
    mkdir -p /app/output && \
    cp /app/target/${CARGO_PROFILE_DIR}/${BINARY} /app/output/${BINARY}

FROM debian:bookworm-slim
ARG BINARY
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
RUN test -n "$BINARY" || { echo "ERROR: BINARY build-arg is required"; exit 1; }
COPY --from=builder /app/output/${BINARY} /usr/local/bin/${BINARY}
USER nobody
ENTRYPOINT ["/bin/sh", "-c", "exec \"$@\"", "--"]
