# icegate-maintain

Background maintenance for the IceGate tables: schema migration, Parquet and
manifest compaction, orphan-file garbage collection, WAL segment cleanup, and the
LLM pricing crawler.

The crate has two entry points and they behave very differently:

- the one-shot `migrate` commands, which create the tables and stamp their
  properties, then exit;
- the long-running `run` service, which drives compaction, GC, WAL cleanup, and
  pricing as jobmanager tasks.

## Structure

| Path                              | Owns                                                                            | Deep dive                            |
|-----------------------------------|---------------------------------------------------------------------------------|--------------------------------------|
| [`migrate/`](src/migrate)         | Table creation, schema-drift report, the retention policy stamped onto new tables. | [README](src/migrate/README.md)      |
| [`compact/`](src/compact)         | Data-file and manifest compaction jobs.                                          | [README](src/compact/README.md)      |
| [`gc/`](src/gc)                   | Orphan-file sweep: reachable set, decision, delete.                              | module doc comments                  |
| [`wal_cleanup/`](src/wal_cleanup) | WAL segment cleanup: delete bound, per-topic delete cycle.                       | [README](src/wal_cleanup/README.md)  |
| [`pricing/`](src/pricing)         | LLM rate-card crawler writing the `prices` table.                                | module doc comments                  |
| [`jobs.rs`](src/jobs.rs)          | Worker pool and job-state storage config, shared by all four services.           | module doc comments                  |
| [`cli/`](src/cli)                 | Argument parsing and the wiring of each entry point.                             | —                                    |

Each of the four services runs on a worker pool of its own, with its own
job-state prefix: a pool shared between a daily sweep and a ten-minute cleanup
turns the shorter interval into a lower bound nobody can predict. What one pool
costs while it is idle is a poll of a local cache, not a request to the store.

## Configuration

Both entry points load a single `MaintainConfig` ([`config.rs`](src/config.rs)),
so one file can serve both, and each side reads only the blocks it owns. What a
field means is documented on the field.

Helm keys live under `migrate.*` and `maintain.*` in
[`values.yaml`](../../config/helm/icegate/values.yaml); the Compose file is
[`config/docker/maintain.yaml`](../../config/docker/maintain.yaml). The two
deployments change together.

## Deployment contract

Values owned by three components have to stay ordered, and no single config file
holds them all. `max_snapshot_age_ms` is in milliseconds and the others in
seconds, so the first ordering carries the conversion:

```text
query.engine.max_age_secs * 1000 < migrate.snapshot_expiration.max_snapshot_age_ms
query.engine.max_age_secs        < maintain.gc.orphans.min_age_secs
```

The query engine caches a catalog provider — a fixed table state and the file
list behind it — for `max_age_secs`. A snapshot must therefore outlive every
cached reference to it, and a file must stay unreferenced for longer than a
provider can be cached before the sweep may take it. Violate either and a query
plans against a file the sweep has already deleted: a scan error, not wrong
results.

WAL cleanup is deliberately not part of this ordering. Its bound is
`committed - maintain.wal_cleanup.keep_segments_count`, in segments, so it depends on no
other component's window — see [`wal_cleanup/README.md`](src/wal_cleanup/README.md) for the
reader it has to stay behind and why the count, not a clock, is what holds it
there.

Who enforces what:

- The **Helm chart** is the only place all three values exist at once, so it
  checks the full ordering at render time (`icegate.validateRetentionWindow` in
  [`_helpers.tpl`](../../config/helm/icegate/templates/_helpers.tpl)). It also
  mirrors the per-value bounds of the component validators, because
  `configmap-migrate.yaml` is a `pre-install,pre-upgrade` hook: a block only the
  pod rejects fails the release after the render has already passed. That mirror
  covers every rule whose operands are in values and no more — a bound stated
  against a default resolved in code stays with the pod that owns it.
- **`MaintainConfig::validate`** sees one file, so it checks what that file can
  answer for: the whole `snapshot_expiration` block (window bounds, and the
  metadata-log bound against the snapshot window), the WAL-cleanup tunables and
  the queue location they address, plus the single cross-component case that does
  not need the query engine's value — `gc.orphans.min_age_secs` of zero breaks
  the ordering whatever `max_age_secs` is set to, since that value is positive by
  definition. Compose and hand-written configs get exactly these. A service's
  `jobsmanager` block is not among them: the `migrate` commands share this config
  and carry none, so each runner validates its own when it builds its pool.
- **Nothing enforces it at runtime.** The read path does not yet retry a query
  whose file went missing (see the module docs of `icegate-query`
  `engine/core.rs`).

The retention window itself — where expiration runs, what it costs the operator,
and what stops being recoverable once it is on — is in
[`migrate/README.md`](src/migrate/README.md).
