# Testing

IceGate is a data engine whose correctness depends on contracts across OTLP, Arrow,
Parquet, Iceberg, object storage, DataFusion, and public query protocols. Tests MUST
protect observable behavior, public contracts, and data invariants. They MUST NOT be
added merely to mirror every production function or type.

## Change coverage

For every behavior change, build a coverage map before considering the work complete:

```text
changed behavior -> possible failure -> test layer -> concrete test
```

- Every reachable branch and error path introduced or changed MUST be covered, or its
  omission MUST be explicitly justified as unreachable or irrelevant.
- Every bug fix MUST include a regression test that reproduces the defect and would fail
  without the fix.
- A behavior-preserving refactor does not require tautological new tests when existing
  tests already protect the affected contract. The existing coverage MUST be identified.
- Line coverage is a gap detector, not proof of correctness. A coverage percentage does
  not replace independent assertions or the risk analysis below.

For each change, consider every applicable risk class:

- normal behavior;
- empty, missing, null, zero, one, and many values;
- immediately below, exactly at, and immediately above a boundary;
- malformed, unsupported, truncated, and overflowing input;
- duplicates, ties, ordering, and out-of-order input;
- partial failure, retry, cancellation, restart, and idempotency;
- concurrent reads, writes, and state transitions;
- mixed tenants, partitions, snapshots, files, and row groups;
- a cross-tenant negative case when tenant-scoped data is affected.

This is a risk checklist, not a requirement to add meaningless cases. Tests SHOULD use
table-driven cases or property tests when multiple inputs represent the same rule.

## Choose the relevant boundary

Use the lowest stable boundary that still includes the behavior and risk under test.

### Unit tests

Use unit tests for pure transformations, parsers, validation, state transitions,
planning rules, and deterministic algorithms. Unit tests MUST NOT perform network,
filesystem, object-store, or container I/O.

Prefer testing a stable module or crate contract. A private pure algorithm MAY be tested
directly when its behavior is independently specified and testing only through a higher
boundary would hide the failing rule. Do not expose production APIs or add production
methods solely for tests.

### Component tests

Use component tests for orchestration across several production types when the external
boundary is not relevant. Fakes and mocks are appropriate for deterministic fault
injection, call ordering that is itself a contract, and otherwise difficult failure paths.
Test doubles SHOULD implement the same production traits as real dependencies.

### Integration tests

Use real implementations when the behavior belongs to an integration boundary. In
particular, mocks are not sufficient as the only coverage for:

- S3 conditional requests, ETags, and compare-and-swap behavior;
- Iceberg transactions, snapshots, manifests, partitioning, and schema evolution;
- Parquet encoding, statistics, row groups, and metadata;
- DataFusion planning and execution;
- HTTP, gRPC, Flight SQL, Loki, Prometheus, Tempo, and OTLP wire contracts.

New integration-style tests SHOULD live under `crates/<crate>/tests/` when the scenario
can be expressed through the public crate API. A crate-internal component test MAY live
under `src/tests/` only when access to a meaningful `pub(crate)` contract is necessary.

### Contract and end-to-end tests

A change to a public protocol MUST have a test at the real transport boundary. Calling a
handler directly is useful component coverage, but is not sufficient as the only test of
an HTTP, gRPC, or Flight contract.

A change to a schema, serialized format, column encoding, or boundary between pipeline
components MUST include a cross-component test. At least one test MUST use the production
writer configuration and a real downstream reader.

Changes spanning the ingestion and query path MUST cover the applicable part of:

```text
OTLP -> WAL -> Shift -> Iceberg commit -> Loki/Prometheus/Tempo/Flight query
```

## Independent test oracles

- Expected results MUST come from a specification, a small independent reference model,
  or explicitly stated values.
- Expected results MUST NOT be computed with the same helper, constant, parser, planner,
  sorting routine, or conversion algorithm as the code under test.
- Arrange code MAY use canonical production schemas and builders to create valid input.
  Assertions about semantic output MUST remain independent of the implementation.
- A test MUST prove that its fixture reaches the condition under test when that condition
  is not obvious, for example a payload crossing an offload threshold or a partition
  qualifying for compaction.
- Do not guard important assertions with `if !result.is_empty()` or equivalent logic. If
  results are expected, assert the exact count or non-emptiness before inspecting them.
- Compare unordered results after canonicalization unless ordering is part of the public
  contract. When ordering is a contract, assert the complete relevant order.
- Do not assert against `Debug` output or use substring checks on a formatted DataFusion
  plan as the primary semantic oracle. Execute the plan or inspect stable structured
  nodes. Plan-shape assertions are appropriate only when a specific optimization such as
  pruning or projection is itself the contract.
- Snapshot or golden tests MAY be used for stable wire formats. Volatile values such as
  UUIDs, timestamps, paths, ports, execution times, and generated metadata locations MUST
  be normalized or asserted structurally.

For errors, assert the stable machine-readable contract: the Rust variant, error kind,
protocol code, HTTP/gRPC status, retryability, and meaningful structured fields. Do not
assert human-readable error text unless the text is explicitly part of a public protocol
contract.

## IceGate invariants

Apply the following requirements whenever the corresponding subsystem is affected.

### Schemas, Arrow, Parquet, and Iceberg

- Test fixtures MUST derive Iceberg schemas, partition specs, and sort orders from
  `crates/icegate-common/src/schema.rs`. Production encodings and writer properties MUST
  come from their corresponding public helpers.
- Tests MUST address fields by name. Hardcoded field positions are allowed only when field
  order is itself the contract and the test asserts that order explicitly.
- Tests of the canonical schema itself MUST use an independent expected contract. Other
  tests MUST NOT manually duplicate a production schema without an explicit
  schema-equivalence assertion.
- A rewrite, compaction, migration, or recovery test MUST verify the full relevant row
  multiset, not only row count or schema.
- Where applicable, also verify tenant and partition isolation, physical ordering,
  statistics and bounds, snapshot lineage, and the committed WAL offset.
- Changes to maintenance operations that support repeated execution MUST include an
  idempotency case. Destructive operations such as garbage collection MUST fail closed
  when reachability cannot be established.

### Ingestion and WAL

- OTLP changes MUST cover the relevant protobuf and JSON paths and the HTTP and/or gRPC
  boundary affected by the change.
- Consider compressed, large, malformed, partially valid, and empty payloads.
- Backpressure, closed/full channels, acknowledgements, partial success, timeouts, and
  cancellation MUST be covered when their paths are changed.
- WAL changes MUST cover segment and row-group boundaries, offset monotonicity, restart
  recovery, corrupted or incomplete metadata, and independent topics when applicable.

### Query

- Parser changes MUST cover valid syntax, invalid syntax, precedence, and boundary values.
- Planner and executor changes MUST be tested by executing representative data whenever
  practical; AST or plan-shape assertions alone are insufficient for semantic behavior.
- Query results MUST include exact series, streams, rows, labels, values, and ordering
  relevant to the contract, rather than only checking that a response succeeds.
- Changes affecting tenant filtering MUST include data from multiple tenants in the same
  physical file or batch when possible, plus a cross-tenant negative query.
- Changes to parsers, sort/merge logic, serialization, or state machines MUST use property
  or fuzz tests when a few examples cannot cover the input space, or the coverage map MUST
  explain why a bounded case matrix is sufficient. Any discovered failing input MUST be
  preserved as a deterministic regression test.

### Catalog, jobs, and concurrency

- Catalog changes affecting persistence MUST include real S3-compatible storage coverage.
- Compare-and-swap and concurrent state transitions MUST assert all allowed outcomes and
  the final shared invariant. Do not assert which task wins the scheduler.
- Retry tests MUST cover the first delay, exhaustion, retryable versus terminal errors,
  and cancellation when applicable.
- Job state changes MUST cover invalid transitions, dependency failures, repeated events,
  restart or cache invalidation, and size/time limits at the boundary and just beyond it.

## Determinism and isolation

- Use fixed timestamps and dates. Use the real clock only when current-time behavior is
  the contract under test.
- Use Tokio's paused clock for timers and backoff where possible.
- Real sleeps MUST NOT determine event ordering or correctness. Coordinate concurrency
  with barriers, notifications, channels, or explicit test gates.
- Every wait MUST have a bounded timeout. A timeout error SHOULD retain the underlying
  failure or diagnostic state.
- Randomized tests MUST use a reproducible seed. Random UUIDs MAY be used only for resource
  isolation when their value is not part of the assertion.
- Tests using shared infrastructure MUST use unique buckets, prefixes, namespaces, tables,
  and ports so normal parallel execution is safe.
- Servers SHOULD bind to port `0` or use an already-bound listener. Harnesses MUST confirm
  readiness before sending requests.
- Harnesses MUST own temporary directories, containers, listeners, and background tasks
  through RAII-style guards. They MUST clean up on success, error, timeout, and panic.
- A background panic, failed join, or failed shutdown MUST fail the test instead of being
  silently ignored.
- Tests MUST NOT depend on external networks, cloud credentials, or shared remote state.
  Infrastructure tests use local containers or explicitly provisioned CI services.

## Test readability

- Follow Arrange-Act-Assert as a logical structure. Do not add heading comments when the
  phases are already obvious.
- Test names MUST state the condition or trigger and the observable result. Do not list a
  case in the name that the body does not exercise.
- If behavior is triggered implicitly by startup, a callback, a timer, cancellation, or a
  background task, make the trigger explicit in the name or a short comment.
- Comments explain why a case matters, an external specification, a non-obvious trigger,
  or a previous regression. Do not narrate the test body.
- Keep inline `#[cfg(test)]` modules at the end of the source file.
- Do not commit commented-out tests.
- Shared test helpers MUST reduce setup duplication without hiding the inputs and outputs
  that make the case meaningful.

## Disabled and flaky tests

- A required test MUST NOT be made green by retrying it in CI.
- Treat a flaky test as a defect. Replace timing assumptions with deterministic
  synchronization or fix resource isolation.
- `#[ignore]` requires a linked issue, an explanation of the lost coverage, and a clear
  condition for re-enabling the test.
- An ignored, skipped, or environment-gated test does not count as coverage for a change.
- If required integration infrastructure is unavailable locally, report which test command
  was not run. Do not claim the test suite passes and do not silently skip required tests.

## Test review

Review tests by mapping affected behavior to coverage by layer before reviewing individual
assertions. Two tests are not duplicates merely because their final assertions look alike:
a unit test can protect a pure rule while an integration test protects serialization or
orchestration of the same result.

Before completing a code change, verify:

- the changed behavior and failure modes are represented in the coverage map;
- the chosen test layers include every changed boundary;
- regression tests fail for the defect they claim to protect;
- expected values are independent of the implementation;
- relevant boundary, failure, tenant, ordering, and concurrency cases are covered;
- fixtures use canonical schemas and production formats where required;
- tests are deterministic, isolated, and safe under normal parallel execution;
- all applicable feature combinations were tested.

Run the narrowest relevant test while developing, then the affected crate before
completion:

```bash
cargo test -p <crate> <test_name>
cargo test -p <crate>
cargo test -p icegate-catalog-s3 --all-targets --features rest
```

The REST command is required when `icegate-catalog-s3` REST code or shared catalog behavior
changes. Other optional features MUST be tested whenever the change can affect them.

## Sanitizers

The sanitizer targets run the test suite under LeakSanitizer or AddressSanitizer to protect
the C dependencies, `unsafe` inside third-party Rust, and allocations unreachable at exit.
They are not part of `make ci`. How to run them, what a green run does and does not prove,
and how the suppression files are maintained: [config/sanitizers/README.md](../config/sanitizers/README.md).

- A change to a C dependency, to a compression or TLS code path, or to the global allocator
  SHOULD be verified under `make sanitize-address` and `make sanitize-leak` before merging.
  Where a local run is impractical, the PR MUST carry the `sanitize` label so CI runs them
  on it, and the result MUST be reported like any other test command.
- A sanitizer run MUST NOT be counted as coverage for feature-gated code. It builds with no
  `--features`, so anything behind a feature gate is skipped silently — that code still
  needs the tests required above.
- A leak or memory error in first-party code is a defect to fix, not to suppress. A
  suppression matching a frame inside `icegate_*` MUST carry measured figures from a full
  run on the current tree and a retirement condition.
