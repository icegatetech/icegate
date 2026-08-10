#!/usr/bin/env bash
# Run the workspace's unit and integration tests under an LLVM sanitizer.
# Doctests are excluded — see the --lib --bins --tests note below.
#
# Usage: scripts/sanitize.sh <address|leak|memory>
#
# Leak and memory detection do not exist on Darwin (aarch64-apple-darwin supports
# only address and thread), so on a non-Linux host this script builds a Linux
# image and re-execs itself inside it. CI already runs on Linux and takes the
# direct path, so both share one code path and cannot drift.
set -euo pipefail

SANITIZER="${1:-}"
case "$SANITIZER" in
    address | leak | memory) ;;
    *)
        echo "sanitize.sh: usage: sanitize.sh <address|leak|memory>" >&2
        exit 2
        ;;
esac

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_TAG="icegate-sanitize:local"
CARGO_VOLUME="icegate-sanitize-cargo"

# The nightly is pinned, not floating: an unpinned nightly picks up new lints on every
# rebuild and breaks the run for reasons unrelated to sanitizers. Read from one file so
# the image, this script, and CI cannot drift apart. SANITIZE_TOOLCHAIN overrides it for
# a one-off bump check.
TOOLCHAIN="${SANITIZE_TOOLCHAIN:-$(tr -d '[:space:]' < "$REPO_ROOT/config/sanitizers/toolchain")}"

if [ "$(uname -s)" != "Linux" ]; then
    docker build \
        -f "$REPO_ROOT/config/docker/sanitize.Dockerfile" \
        -t "$IMAGE_TAG" \
        "$REPO_ROOT"

    # --network host: crates/icegate-common/src/testing/container.rs dials the
    #   testcontainer at 127.0.0.1:<mapped port>, which resolves only when this
    #   container shares the namespace that published that port.
    # docker.sock: testcontainers starts sibling containers on the host daemon.
    # The image is built for the host architecture, so the uname -m dispatch
    # below picks aarch64 on Apple Silicon rather than emulating x86_64.
    exec docker run --rm -t \
        --network host \
        -e "SANITIZE_TOOLCHAIN=$TOOLCHAIN" \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -v "$REPO_ROOT:/work" \
        -v "$CARGO_VOLUME:/usr/local/cargo/registry" \
        -w /work \
        "$IMAGE_TAG" \
        scripts/sanitize.sh "$SANITIZER"
fi

case "$(uname -m)" in
    x86_64) TARGET="x86_64-unknown-linux-gnu" ;;
    aarch64 | arm64) TARGET="aarch64-unknown-linux-gnu" ;;
    *)
        echo "sanitize.sh: unsupported architecture '$(uname -m)'" >&2
        exit 2
        ;;
esac

# Sanitizer-instrumented links are memory-hungry: linking this dependency graph at
# full parallelism exhausted a 19.5 GiB VM with `ld: Cannot allocate memory`.
# Budget memory per link job and never exceed the CPU count.
#
# The budget is per sanitizer because they are not equally expensive. Standalone
# LeakSanitizer only intercepts malloc at runtime and links near-normally, while
# AddressSanitizer and MemorySanitizer instrument every memory access AND run
# under -Zbuild-std, so std is rebuilt instrumented too.
#
# What was actually measured: 4 GiB/job succeeded for leak, then failed for
# address with eight `ld: Cannot allocate memory` errors. That establishes only
# that 4 is too low for address; 8 is conservative, not a measured optimum. It is
# also the value the green address run was verified at (-j2 on a 19.5 GiB VM), so
# it is not lowered without re-verifying — a smaller divisor would land locally
# between the known-good 2 and the known-OOM 4.
#
# Consequence to know: the division truncates, so a 16 GB CI runner (~16373036 kB)
# gets -j1 for address, not -j2. That is slow but correct; the remedy if the CI
# job times out is a larger runner, not a smaller divisor chosen blind.
# The memory branch inherits this untested.
case "$SANITIZER" in
    leak) gib_per_job=4 ;;
    address | memory) gib_per_job=8 ;;
    *)
        echo "sanitize.sh: no memory budget for '$SANITIZER'" >&2
        exit 2
        ;;
esac

mem_kib=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
mem_jobs=$(( mem_kib / (gib_per_job * 1048576) ))
if [ "$mem_jobs" -lt 1 ]; then
    mem_jobs=1
fi
cpu_jobs=$(nproc)
if [ "$mem_jobs" -lt "$cpu_jobs" ]; then
    JOBS=$mem_jobs
else
    JOBS=$cpu_jobs
fi

# An explicit --target is required even when it equals the host triple: without
# it, cargo applies RUSTFLAGS to build scripts and proc-macro crates too, and
# those are host binaries that then run under instrumentation.
#
# --cfg icegate_sanitize disables the jemalloc global allocator, which would
# otherwise replace the malloc/free the sanitizer runtime must intercept.
RUSTFLAGS="-Zsanitizer=$SANITIZER --cfg icegate_sanitize"
# -Z flags are appended directly after the subcommand, before the rest, so cargo
# parses them unambiguously. -Zbuild-std also requires an explicit --target,
# which is always passed below.
CARGO_ARGS=(+"$TOOLCHAIN" test)

case "$SANITIZER" in
    address)
        CARGO_ARGS+=(-Zbuild-std)
        # rust-lld instead of GNU ld. ASan instrumentation plus -Zbuild-std
        # inflates these binaries past aarch64's +/-128 MB direct-call range, and
        # GNU ld fails with `relocation truncated to fit: R_AARCH64_CALL26`
        # rather than generating range-extension thunks for the .text.startup
        # constructor sections the `inventory` crate emits. lld generates them,
        # and links large binaries in less memory besides.
        #
        # rust-lld ships with the toolchain (lib/rustlib/<triple>/bin/gcc-ld/),
        # so this needs nothing installed in the image.
        #
        # Applied on every architecture, deliberately, even though the symptom is
        # aarch64-only: this script's whole point is that local and CI share one
        # code path (see the header), and an arch-conditional linker would mean
        # the aarch64 wrapper and the x86_64 CI job link differently. The
        # consequence to know: this combination has only ever run on aarch64, so
        # the first x86_64 exercise will be the nightly CI job.
        #
        # Not applied to the leak target: it does not use -Zbuild-std, its
        # binaries stay under the range limit, and it is already verified green
        # with the default linker.
        RUSTFLAGS="$RUSTFLAGS -Zunstable-options -Clinker-flavor=gnu-lld-cc -Clink-self-contained=+linker"
        # -Zsanitizer=address instruments only rustc-compiled units. The C in
        # zstd-sys, lz4-sys, aws-lc-sys, and ring is compiled by build scripts via
        # the cc crate, so without these an out-of-bounds inside those libraries is
        # invisible: ASan detects through compiler-inserted shadow checks, and
        # uninstrumented code carries none.
        #
        # Note what this actually couples: the image has no clang, so `cc` is GCC
        # and the C is instrumented by GCC's ASan (libzstd.a carries GCC's
        # __asan_version_mismatch_check_v8 guard), then linked against LLVM
        # compiler-rt's runtime supplied by rustc. That works, and fails loudly
        # rather than silently if it ever stops working — but note that
        # config/sanitizers/toolchain pins only the LLVM half; the GCC half floats
        # with the unpinned `FROM rust:bookworm` base image.
        #
        # Instrumenting all four is not the same as exercising them: what a clean
        # run actually covers is in config/sanitizers/README.md.
        export CFLAGS="${CFLAGS:-} -fsanitize=address -fno-omit-frame-pointer"
        export CXXFLAGS="${CXXFLAGS:-} -fsanitize=address -fno-omit-frame-pointer"
        # detect_leaks=0: ASan enables LeakSanitizer by default on Linux, which
        # would make this target duplicate sanitize-leak and fail on leaks too.
        # Keeping them apart yields two independent signals.
        export ASAN_OPTIONS="detect_leaks=0:detect_stack_use_after_return=1:suppressions=$REPO_ROOT/config/sanitizers/asan.supp"
        ;;
    leak)
        # No -Zbuild-std: standalone LSan intercepts malloc at runtime rather
        # than instrumenting code, so an uninstrumented std costs it nothing.
        # That keeps the leak pass the cheap one.
        export LSAN_OPTIONS="suppressions=$REPO_ROOT/config/sanitizers/lsan.supp"
        ;;
    memory)
        # This target does not currently work, and is retained only so the
        # finding is not rediscovered. It dies at the first C file below: MSan
        # is clang-only and `cc` here is GCC. Switching to clang does not fix
        # it — see config/sanitizers/README.md for the second, independent
        # blocker and the full remediation.
        RUSTFLAGS="$RUSTFLAGS -Zsanitizer-memory-track-origins"
        # Same linker reasoning as the address branch: -Zbuild-std plus
        # instrumentation exceeds aarch64's direct-call range under GNU ld.
        RUSTFLAGS="$RUSTFLAGS -Zunstable-options -Clinker-flavor=gnu-lld-cc -Clink-self-contained=+linker"
        CARGO_ARGS+=(-Zbuild-std)
        # MSan reports any read of memory written by uninstrumented code, so the
        # C dependencies must carry the same instrumentation.
        export CFLAGS="${CFLAGS:-} -fsanitize=memory -fPIE"
        export CXXFLAGS="${CXXFLAGS:-} -fsanitize=memory -fPIE"
        ;;
esac

# --lib --bins --tests, not a bare --workspace: a plain `cargo test` also builds
# doctests, and doctests are compiled by rustdoc, which never receives RUSTFLAGS.
# They would run uninstrumented and without --cfg icegate_sanitize — passing while
# detecting nothing. `--all-targets` is not the answer either: it would pull in the
# two `harness = false` criterion benches, one of which starts an S3 container.
#
# --no-fail-fast because a sanitizer report is not a failing test: the binary
# passes every test, then the runtime finds leaks and exits non-zero at process
# exit. Cargo stops after the first test executable that fails, so without this
# one crate's leak means every later crate never runs — and the report reads as
# the whole workspace's leak total when it is only the first binary's. That is
# not hypothetical: the 2026-08-09 nightly aborted inside `icegate-common --lib`
# having executed 2 of the workspace's test binaries, and the two suppression
# patterns covering icegate-query were never exercised.
CARGO_ARGS+=(--workspace --lib --bins --tests --no-fail-fast --target "$TARGET" -j "$JOBS")

export RUSTFLAGS
# Layout note for anyone inspecting artifacts: this toolchain places compiled
# test executables under debug/build/<pkg>/<hash>/out/, NOT the classic
# debug/deps/. A glob over deps/ matches nothing and silently reports zero, which
# looks identical to missing instrumentation.
export CARGO_TARGET_DIR="$REPO_ROOT/target/sanitize-$SANITIZER"
export RUST_BACKTRACE=1

echo "sanitize.sh: $SANITIZER on $TARGET, -j$JOBS (target dir: $CARGO_TARGET_DIR)"
exec cargo "${CARGO_ARGS[@]}"
