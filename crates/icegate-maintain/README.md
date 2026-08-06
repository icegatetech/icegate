# icegate-maintain

Background maintenance for the IceGate tables: schema migration, Parquet and
manifest compaction, orphan-file garbage collection, and the LLM pricing crawler.

The crate has two entry points and they behave very differently:

- the one-shot `migrate` commands, which create the tables and stamp their
  properties, then exit;
- the long-running `run` service, which drives compaction, GC, and pricing as
  jobmanager tasks.

## Structure

| Path                              | Owns                                                                            | Deep dive                            |
|-----------------------------------|---------------------------------------------------------------------------------|--------------------------------------|
| [`migrate/`](src/migrate)         | Table creation, schema-drift report, the retention policy stamped onto new tables. | [README](src/migrate/README.md)      |
| [`compact/`](src/compact)         | Data-file and manifest compaction jobs.                                          | [README](src/compact/README.md)      |
| [`gc/`](src/gc)                   | Orphan-file sweep: reachable set, decision, delete.                              | module doc comments                  |
| [`pricing/`](src/pricing)         | LLM rate-card crawler writing the `prices` table.                                | module doc comments                  |
| [`cli/`](src/cli)                 | Argument parsing and the wiring of each entry point.                             | —                                    |

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
holds them all. `max_snapshot_age_ms` is in milliseconds and the other two in
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
  metadata-log bound against the snapshot window), plus the single cross-component
  case that does not need the query engine's value — `gc.orphans.min_age_secs` of
  zero breaks the ordering whatever `max_age_secs` is set to, since that value is
  positive by definition. Compose and hand-written configs get exactly these.
- **Nothing enforces it at runtime.** The read path does not yet retry a query
  whose file went missing (see the module docs of `icegate-query`
  `engine/core.rs`).

The retention window itself — where expiration runs, what it costs the operator,
and what stops being recoverable once it is on — is in
[`migrate/README.md`](src/migrate/README.md).
