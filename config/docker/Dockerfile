# syntax=docker/dockerfile:1.7
FROM rust:bookworm AS builder
WORKDIR /app

ARG PROFILE=release
ARG BINARY

COPY ./crates ./crates
COPY Cargo.toml Cargo.lock ./

ENV RUSTFLAGS="--cfg tokio_unstable"
ENV CARGO_TARGET_DIR=/app/target
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
