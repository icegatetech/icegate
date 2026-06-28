# icegate-catalog-s3

S3-backed Iceberg catalog: a single `root.json` is the source of truth, atomicity comes from
compare-and-swap on its ETag. No external database.

Architecture, S3 layout, and layer responsibilities are documented in [AGENTS.md](AGENTS.md).

## Known limitations

### Lost-ack re-apply on a truncated metadata-log

When a writer's root CAS physically lands but its acknowledgement is lost (a timeout surfaced as a
conflict), the retry reloads and must recognise its own already-applied commit instead of applying
it again. The classifier decides "already landed" by checking whether the writer's metadata file is
present in the head's `metadata_log`.

Iceberg bounds that log by `write.metadata.previous-versions-max` (default 100) and drains it
oldest-first. So the membership check is only sound while the log is **not** truncated. If all of the
following happen together, the landed commit is misclassified as `Rebuild` and re-applied:

1. the writer's CAS landed but its ack was lost, and
2. the head advanced by **more than** `previous-versions-max` versions on the same table before the
   writer's retry reloaded the root.

Consequence depends on the commit:

- **Commit carrying `RefSnapshotIdMatch`** (every current icegate path — ingest `fast_append`,
  compaction `rewrite`): the rebuild re-checks requirements against the advanced head *before*
  writing anything, the snapshot ids no longer match, and it fails as a surfaced
  `CatalogCommitConflicts`. No silent duplication, no storage mutation.
- **Commit without a snapshot requirement** (possible only through the public `commit_transaction`
  API; no such path exists in icegate today): the rebuild succeeds and silently duplicates the
  snapshot.

### Why Icegate does not hit this

- Per-table write concurrency is low: a single shifter commits to a given table, so the head cannot
  advance by 100+ versions inside one writer's retry window (sub-second; the CAS backoff curve is
  measured in milliseconds).
- Every commit path carries `RefSnapshotIdMatch`, which downgrades the worst case from silent data
  duplication to a surfaced conflict.

The fix (an identity check that survives log truncation — e.g. matching the prepared `snapshot_id`
against the head's snapshots, or returning a hard conflict instead of a silent rebuild) is deferred
as over-engineering for the current usage. Tracked by the `TODO(low)` in
`CatalogRoot::merge_transaction` (`src/root.rs`). Revisit if per-table write concurrency rises or a
commit path without a snapshot requirement is introduced.
