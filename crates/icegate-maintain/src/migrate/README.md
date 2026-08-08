# Table migration and snapshot retention

`migrate create` creates the six IceGate tables from the schema module and stamps
their properties; `migrate upgrade` reports schema drift and never rewrites a
table in place. Both exit when done. What each command does with an existing
table is documented on [`operations.rs`](operations.rs); what a policy field
means, on the field in [`config.rs`](config.rs).

This file covers what no single field can state: where snapshot expiration
actually runs, and what an operator inherits by turning it on.

## Why history is bounded

IceGate does not use time travel, but every live snapshot in `metadata.json`
keeps its manifests and data files referenced. GC's reachable set is built from
those snapshots, so an unbounded history leaves the sweep nothing to reclaim and
makes compaction free space on paper only.

## Where expiration runs

Not here, and not as a job anywhere in this crate. `migrate create` writes a
retention policy into the properties of each table it creates; from then on every
commit against that table — an ingest shift, a compaction rewrite, a pricing
append — resolves the policy from those properties and carries a
`RemoveSnapshots` update along with whatever it was already writing. The
mechanism, and which snapshots it will never expire, belong to `iceberg-rust`
(`transaction/snapshot_expiration.rs` in the `icegatetech` fork).

Which properties carry the policy is
[`SnapshotExpirationConfig::build_table_properties`](config.rs) — the spec keys
are named there, against the constants of the spec crate. One of them is not a
config field: the summary key whose carrier expiration must keep reachable is
derived per table, because it is a promise about that table's own summaries.
IceGate uses it for the WAL offset, so it is stamped onto the five tables the
Shifter writes and left off `prices`
([`operations.rs`](operations.rs)).

## What the operator inherits

- **The policy is written once, at creation.** `migrate create` skips a table
  that already exists, properties included, and `migrate upgrade` does not touch
  properties either. Changing the config re-stamps nothing; there is no backfill
  command. A table created before this policy existed keeps its history forever.
- **A live table is turned off on the table.** Setting
  `history.expire.enabled=false` on the table itself is what stops expiration.
  The `snapshot_expiration` block describes tables not yet created, which is why
  nothing else in this crate reads it as a statement about the deployment.
- **Expiration rides on commits and only on commits.** A table nothing writes to
  never converges to its window: GC opens no transaction, and neither does an
  empty one. TODO in the fork: an operator-driven one-off catch-up.
- **A long history converges gradually.** One commit expires a bounded number of
  snapshots, so a first run against a neglected table does not produce a
  multi-megabyte commit body — the window is reached over several commits, at the
  rate commits arrive.
- **A bad property value fails the write, not a job.** An unparsable or
  out-of-range `history.expire.*` value fails the commit that resolved it with
  `DataInvalid`. Since every writer resolves it, that stops ingest, not just
  maintenance. `MaintainConfig::validate` catches such a window while it is still
  a config file; edits made directly to a live table's properties have no such
  gate.
- **Side branches keep their head, not their history.** Every reference target
  survives, but the retained window is walked from the current snapshot alone, so
  a branch's own ancestors expire out from under it. Per-branch retention
  overrides (`SnapshotRetention`) are not resolved yet — TODO in the fork.

## Rollback stops at the window

A superseded `metadata.json` older than the retention window is not a restore
point. The manifests and data files of the snapshots it names became orphans the
moment those snapshots expired, and the sweep takes them once
`gc.orphans.min_age_secs` has passed. Rolling a table back to such a version
yields metadata pointing at deleted files — silently, since nothing rejects the
rollback itself.

This is why `metadata_previous_versions_max` is validated against
`min_snapshots_to_keep` rather than set independently: a metadata log shorter
than the snapshot window drops versions covering snapshots the table is still
required to keep, and `icegate-catalog-s3` resolves a lost commit ack by finding
its own metadata file in the head's metadata log — a log truncated past that
point turns a landed commit into an unrecognised one.
