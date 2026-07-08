FROM rust:bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY ./crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG PROFILE=release
# Optional extra Cargo features for the build, e.g.
# `--build-arg CARGO_FEATURES=icegate-ingest/tokio-console` to produce a
# tokio-console-instrumented ingest image. Empty by default (no behaviour change).
ARG CARGO_FEATURES=""
# `protobuf-compiler` provides `protoc`; `libprotobuf-dev` ships the
# well-known `.proto` files (any.proto, empty.proto, ...) imported by
# the `substrait` build script (transitive of
# `datafusion-flight-sql-server`).
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
COPY --from=planner /app/recipe.json recipe.json
ENV RUSTFLAGS="--cfg tokio_unstable"
RUN cargo chef cook $([ "$PROFILE" = "release" ] && echo "--release") \
    $([ -n "$CARGO_FEATURES" ] && echo "--features $CARGO_FEATURES") --recipe-path recipe.json
COPY ./crates ./crates
COPY Cargo.toml Cargo.lock ./
RUN cargo build $([ "$PROFILE" = "release" ] && echo "--release") \
    $([ -n "$CARGO_FEATURES" ] && echo "--features $CARGO_FEATURES") --workspace --bins

FROM debian:bookworm-slim
ARG PROFILE=release
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/${PROFILE}/query    /usr/local/bin/query
COPY --from=builder /app/target/${PROFILE}/ingest   /usr/local/bin/ingest
COPY --from=builder /app/target/${PROFILE}/maintain  /usr/local/bin/maintain
USER nobody
ENTRYPOINT ["/bin/sh", "-c", "exec \"$@\"", "--"]
