# syntax=docker/dockerfile:1.7
# Linux runner for `make sanitize-*` on non-Linux developer machines.
# CI already runs on Linux and never builds this image.
FROM rust:bookworm

# protobuf-compiler provides protoc; libprotobuf-dev ships the well-known .proto
# files imported by the substrait build script — the same pair installed by
# config/docker/Dockerfile and by the CI workflows.
# llvm supplies llvm-symbolizer, without which sanitizer stack traces are raw
# addresses instead of function names.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        protobuf-compiler libprotobuf-dev llvm curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# testcontainers starts sibling containers on the daemon socket mounted at run
# time, so only the client binary is needed — not a second daemon.
ARG DOCKER_CLI_VERSION=27.3.1
RUN curl -fsSL "https://download.docker.com/linux/static/stable/$(uname -m)/docker-${DOCKER_CLI_VERSION}.tgz" \
    | tar -xz -C /usr/local/bin --strip-components=1 docker/docker

# rust-src is required by -Zbuild-std, which compiles std from source so that
# std allocations are instrumented too.
#
# The nightly is pinned, not floating: an unpinned nightly picks up new lints on every
# rebuild and breaks the sanitizer run for reasons unrelated to sanitizers — the same
# reproducibility rule Cargo.toml applies to the jobmanager and iceberg git revs.
#
# The version is read from config/sanitizers/toolchain so this image, scripts/sanitize.sh,
# and the CI workflow cannot drift apart. To bump: edit that file, rebuild, and re-run
# `make sanitize-leak` before merging.
#
# Installed under its dated name because scripts/sanitize.sh invokes `cargo +<name>`, an
# explicit-name override that ignores `rustup default` — installing it as the default
# would leave `+nightly` auto-installing a floating toolchain instead.
COPY config/sanitizers/toolchain /etc/icegate-sanitize-toolchain
RUN rustup toolchain install "$(cat /etc/icegate-sanitize-toolchain)" \
        --component rust-src --profile minimal

WORKDIR /work
