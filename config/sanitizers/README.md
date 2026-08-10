# Sanitizers

The workspace forbids `unsafe`, so these targets are aimed at the C dependencies
(`zstd-sys`, `lz4-sys`, `aws-lc-sys`, `ring`), at `unsafe` inside third-party Rust, and at
leaked allocations. When a change is expected to run them is a testing-policy question and
lives in [docs/tests.md](../../docs/tests.md); this file covers what the runs mean and how
they are maintained.

| Where | What it holds |
|-------|---------------|
| [`Makefile`](../../Makefile) | the `sanitize-address` / `sanitize-leak` / `sanitize-memory` / `sanitize` targets |
| [`scripts/sanitize.sh`](../../scripts/sanitize.sh) | the runner, and the reasoning behind every flag it sets |
| [`config/docker/sanitize.Dockerfile`](../docker/sanitize.Dockerfile) | the Linux wrapper image used on macOS |
| [`.github/workflows/sanitizers.yml`](../../.github/workflows/sanitizers.yml) | the nightly job and the `sanitize` PR label |
| `toolchain` | the pinned nightly, read by the image, the script, and CI |
| `asan.supp`, `lsan.supp` | suppressions, one file per sanitizer |

All of them run on Linux; on macOS the runner re-execs itself inside a container. They are
not part of `make ci` — CI runs them nightly and on any PR labelled `sanitize`.

## Reading a green run honestly

A green `make sanitize-leak` means **no new leaks**, not no leaks. `lsan.supp` suppresses a
documented baseline: upstream retention we do not control, plus one first-party entry that
covers test scaffolding. Each entry records what leaks, how much, and the condition that
retires it. Read that file before concluding the codebase is leak-free.

No first-party *production* path is suppressed today, and that is a property worth keeping —
adding one back means the nightly stops reporting a real defect. Two such entries were
deleted in August 2026 once the code they named had been refactored away; `lsan.supp`'s
header records both, because the failure mode they share is that a suppression outlives its
subject in silence.

**The sanitizer suite covers less than `make ci` does.** It runs no `--features`, so
`crates/icegate-catalog-s3`'s `catalog` binary — which declares
`required-features = ["rest"]` — is skipped entirely and without a warning, along with the
Axum server, config parsers, and TLS stack that feature gates. That binary ships in every
production image, and `ci.yml` tests it separately. Adding
`--features icegate-catalog-s3/rest` to `scripts/sanitize.sh` would close the gap, at the
cost of a full instrumented rebuild to re-verify. Until then, treat a green sanitizer run
as saying nothing about the REST catalog server.

A green `make sanitize-address` currently means **zstd is instrumented and clean under this
suite**. All four C libraries are compiled with `-fsanitize=address`, but only zstd is
meaningfully exercised: it is the Parquet writer's default codec. LZ4 is a non-default
queue codec no test selects, and `aws-lc-sys`/`ring` are largely unentered because the
container tests speak plain HTTP. Instrumented-but-unexecuted code finds nothing.

Two limits worth knowing before reaching for these:

- **LeakSanitizer does not detect memory growth.** It reports allocations unreachable at
  exit. Retained-but-reachable memory — unbounded channels, retained buffers, oversized
  caches — is invisible to it by construction. Use heap profiling (jemalloc `prof`, `dhat`)
  for those.
- **AddressSanitizer does not instrument assembly.** `aws-lc-sys` and `ring` ship
  hand-written assembly, which ASan leaves alone. That is missed coverage, not a false
  positive.

## Suppressions

Two rules, stated in full at the top of `lsan.supp`: every entry names the library and why
the allocation is not ours, and no entry may match a frame inside `icegate_*`. A leak in
first-party code is a bug to fix, not to suppress.

`lsan.supp` carries documented exceptions to the second rule, each with measured figures
and a retirement condition. Adding another needs the same accounting, or the target quietly
becomes a rubber stamp. `asan.supp` gets no such exemption: an ASan report is a memory error
rather than a retained allocation, so suppressing one hides a bug.

## Maintaining the targets

- **To bump the pinned nightly**, edit `toolchain` — the image, the runner, and CI all read
  that one file — then rebuild and confirm `make sanitize-leak` passes before merging.
  `SANITIZE_TOOLCHAIN=<name>` overrides it for a one-off check without touching the file.
  Leaving the nightly unpinned breaks these runs on unrelated lint churn; that is not
  hypothetical, it happened.
- **Parallelism is capped by memory**, per sanitizer, because instrumented links at full
  parallelism exhaust a typical dev VM. The measured budgets and their consequences are in
  `scripts/sanitize.sh`; the chosen value appears in the runner's progress line.
- **`address` and `memory` link with `rust-lld`** rather than GNU ld, on every
  architecture — see `scripts/sanitize.sh` for the range-extension failure that forces it
  and why it is not made conditional.

## `make sanitize-memory` does not work

The target is retained but broken. Verified 2026-08-05; it is excluded from `make sanitize`
and is not run by CI at all — a nightly job would spend a runner to rediscover the failure
below. It is kept for a manual re-check after either blocker is addressed. Two independent
blockers, either of which is sufficient:

1. **GCC does not implement MemorySanitizer.** The wrapper image has no clang, so `cc` is
   GCC 12.2.0, and `cc -fsanitize=memory` fails with `unrecognized argument to
   '-fsanitize=' option: 'memory'`. MSan is a clang-only sanitizer. The run dies at the
   first C file — `ring`'s `sha256-armv8-linux64.S`.
2. **Even with clang, the assembly is uninstrumentable.** MSan requires every linked
   instruction to be instrumented, and `ring` and `aws-lc-sys` ship hand-written `.S`
   files. Values flowing out of them would read as uninitialised, producing false positives
   rather than findings.

Making this target work would mean installing clang, switching the C dependencies to it,
and then eliminating the assembly-bearing crates from the test binaries. That is not a
suppressions exercise. The target is kept so this finding is not rediscovered.
