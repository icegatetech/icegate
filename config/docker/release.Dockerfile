# syntax=docker/dockerfile:1

# ── Builder stages (shared across all binary images) ────────────────────────
# Pin build stages to the host platform so Rust compiles natively,
# then cross-compile for the target architecture.
FROM --platform=$BUILDPLATFORM rust:bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM --platform=$BUILDPLATFORM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY ./crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

FROM --platform=$BUILDPLATFORM chef AS builder
ARG TARGETARCH

# Install both cross-compilation toolchains unconditionally.
# The unused one adds ~60MB but keeps the logic simple and allows
# building for any target from any host (amd64 CI or arm64 Mac).
# `protobuf-compiler` provides `protoc`; `libprotobuf-dev` ships the
# well-known `.proto` files (any.proto, empty.proto, ...) under
# `/usr/include/google/protobuf/` that the `substrait` build script
# imports from. Both are required by `datafusion-substrait`
# (transitive of `datafusion-flight-sql-server`).
RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && apt-get install -y --no-install-recommends \
      gcc-x86-64-linux-gnu libc6-dev-amd64-cross \
      gcc-aarch64-linux-gnu libc6-dev-arm64-cross \
      protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

RUN <<EOF
set -eu
case "${TARGETARCH}" in
  amd64) RUST_TARGET="x86_64-unknown-linux-gnu" ;;
  arm64) RUST_TARGET="aarch64-unknown-linux-gnu" ;;
  *) echo "ERROR: Unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;;
esac
rustup target add "${RUST_TARGET}"
echo "${RUST_TARGET}" > /tmp/rust-target
EOF

COPY --from=planner /app/recipe.json recipe.json
ENV RUSTFLAGS="--cfg tokio_unstable"
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc

# Cook dependencies with cross-compilation target
RUN RUST_TARGET=$(cat /tmp/rust-target) && \
    cargo chef cook --release --target "${RUST_TARGET}" --recipe-path recipe.json --features icegate-catalog-s3/rest

COPY ./crates ./crates
COPY Cargo.toml Cargo.lock ./

# Build workspace binaries and copy to a fixed output path so the runtime
# stage doesn't need to know the target triple.
RUN <<EOF
set -eu
RUST_TARGET=$(cat /tmp/rust-target)
cargo build --release --target "${RUST_TARGET}" --workspace --bins --features icegate-catalog-s3/rest
mkdir -p /app/output
cp /app/target/${RUST_TARGET}/release/query    /app/output/
cp /app/target/${RUST_TARGET}/release/ingest   /app/output/
cp /app/target/${RUST_TARGET}/release/maintain /app/output/
cp /app/target/${RUST_TARGET}/release/catalog  /app/output/
EOF

# ── Binary staging ──────────────────────────────────────────────────────────
# The runtime base has no shell, so everything that needs one happens here:
# validating BINARY, and building the `entrypoint` symlink the Helm chart
# invokes (`config/helm/icegate/templates/deployment-*.yaml`) alongside the
# per-binary name Docker Compose invokes (`config/docker/docker-compose.yml`).
# COPY of a directory preserves the symlink, so the binary is stored once.
FROM --platform=$BUILDPLATFORM builder AS stager
ARG BINARY
RUN <<EOF
set -eu
test -n "${BINARY}" || { echo "ERROR: BINARY build-arg is required" >&2; exit 1; }
mkdir -p /stage
cp "/app/output/${BINARY}" "/stage/${BINARY}"
ln -s "/usr/local/bin/${BINARY}" /stage/entrypoint
EOF

# ── Runtime (one binary per image) ──────────────────────────────────────────
# No platform pin — inherits the target platform from Buildx.
#
# Distroless rather than debian:bookworm-slim: the binaries link only
# libgcc_s/libm/libc, so every other package a Debian base ships is unused
# attack surface that Trivy reports against this image. Bookworm carried 4
# CRITICAL and 26 HIGH findings, none of them with a fixed version available
# (perl-base, util-linux, ncurses, gzip, zlib1g — all `affected`,
# `fix_deferred` or `will_not_fix`, and all still unfixed in trixie), so no
# base-version bump clears them; dropping the packages does. `cc` is the
# smallest variant that carries libgcc-s1, which the binaries need; the
# `-debian12` suffix keeps glibc in step with the `rust:bookworm` builder.
# ca-certificates ships in the base, so no apt layer is needed for TLS.
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

ARG BINARY
ARG VERSION
ARG REVISION
ARG BUILD_DATE
ARG DESCRIPTION

LABEL org.opencontainers.image.title="icegate-${BINARY}" \
      org.opencontainers.image.description="${DESCRIPTION}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.source="https://github.com/icegatetech/icegate" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=stager /stage/ /usr/local/bin/

# Numeric, because the image has no user database to resolve a name against.
# 65532 is the distroless `nonroot` uid; the chart pins its own uid through
# `securityContext` (`config/helm/icegate/templates/_helpers.tpl`).
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/entrypoint"]
