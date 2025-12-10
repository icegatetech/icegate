FROM rust:bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.* .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG PROFILE=release
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook $([ "$PROFILE" = "release" ] && echo "--release") --recipe-path recipe.json
COPY ./src ./src
COPY Cargo.* .
RUN cargo build $([ "$PROFILE" = "release" ] && echo "--release") --lib --bins

FROM debian:bookworm-slim
ARG PROFILE=release
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/${PROFILE}/query /app/target/${PROFILE}/maintain /usr/local/bin/
USER nobody
CMD ["icegate"]
