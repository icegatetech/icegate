# WAL segment cleanup

Ingest writes every batch to the WAL queue before it acknowledges the client, and the Shifter
later moves those segments into Iceberg. Nothing removed them: without this service the queue
grows for the lifetime of the deployment, and the bucket holds a second, permanent copy of every
row already committed to a table.

Cleanup decides ONE thing — the offset below which a topic's segments may go:

```text
delete_bound = committed - keep_segments_count      (inclusive)
```

Turning that offset into deletes belongs to `icegate-queue`, whose
[`QueueCleaner`](../../../icegate-queue/src/cleaner.rs) owns the segment key format. Everything
else — the cases where no bound exists, the per-cycle budgets, the retries — is in the doc
comments of [`mod.rs`](mod.rs), [`config.rs`](config.rs), [`runner.rs`](runner.rs) and
[`metrics.rs`](metrics.rs). This file carries what no single one of them can show.

## Why offsets and not a clock

Both terms of the bound are offsets, and cleanup reads no clock at all. Age is not what makes a
segment safe to delete — being in Iceberg is, and only the table can say that. A time-based
retention would delete segments the Shifter had not reached whenever it fell behind, which it
does routinely: on a restart, on a large backlog, on a slow catalog.

`keep_segments_count` is the whole safety margin, and it is a margin in SEGMENTS against a
window in TIME. That mismatch is deliberate — the count costs storage, the window costs
correctness — but it means the value has to be sized, not chosen.

Only readers are at risk, never the Shifter: it resumes from the offset it recorded plus one
([`plan_runner.rs`](../../../icegate-ingest/src/shift/plan_runner.rs)), which the count keeps
above the bound by construction.

## Sizing `keep_segments_count`

A query does not read the WAL from wherever the queue happens to be. It reads from the boundary
recorded by the catalog provider it was PLANNED against, and that provider is cached, then
outlives the plan for as long as the query may run — `max_age_secs` and
`max_query_duration_secs` in
[`icegate-query`'s engine config](../../../icegate-query/src/engine/config.rs). A live reader can
therefore sit that whole window behind the current committed offset.

To size the count for a deployment, take that window and multiply it by the rate ONE topic rolls
segments at. The ceiling is one segment per flush tick of the queue writer
([`writer.rs`](../../../icegate-queue/src/writer.rs)); the actual rate is lower when a topic does
not fill a segment every tick, and `wal_cleanup.segments.found` per cycle over a known interval
measures it directly. Size against the busiest topic: the count is one value for all of them.

The floor [`config.rs`](config.rs) enforces is that product at the values every component ships
today, applied only while cleanup is enabled. Widen either query-side setting, or raise the
ingest rate, and the count has to follow — nothing checks the two against each other, because
they live in different components' configs and the rate lives in neither.

Below the window a stale query does not answer from what is left. It FAILS, and the error names
this setting. Retrying replans it on a fresh provider whose boundary is above the bound again,
and the rows it could not reach are in Iceberg either way — which is why failing is the right
outcome and a shorter answer is not.

## The hole a cycle can leave

A segment the store refuses to delete is skipped rather than made fatal, so a cycle can reclaim
its neighbours and leave it behind. That is the one way this deployment produces a WAL with a
hole in it, and three components are built around that fact:

- cleanup skips the key and reports it, because stopping there would freeze the topic for good —
  every later cycle meets the same key first;
- the writer's recovery searches with a predicate that stays monotone across holes, so it still
  resumes above the true maximum ([`writer.rs`](../../../icegate-queue/src/writer.rs));
- a reader whose range spans the hole is told (`SegmentMissing`), never handed the shorter list
  ([`reader.rs`](../../../icegate-queue/src/reader.rs)).

The hole does not heal: the refused key is met and refused again every cycle. It sits below the
committed offset, so only a stale query can ever read across it.

## What cleanup is not

- **Not part of the snapshot/orphan retention ordering.** That ordering ties three windows in
  time together and the chart checks it at render time (see the crate
  [README](../../README.md)); this bound is a count of segments, so there is nothing there to
  order it against. It is NOT independent of the query window all the same — sizing the count
  against that window is [above](#sizing-keep_segments_count).
- **Not a defence for an arbitrarily old reader.** The count buys a bounded window, not history.
  A reader older than it gets an error, not partial rows.
- **Not a job for every table.** `events` is an Iceberg table with no WAL topic of its own, so it
  is swept for orphans by [`gc`](../gc) and has nothing to clean up here.
- **Not a fast drain.** A cycle is capped, reports when the cap cut it short, and leaves the rest
  for the next one: a large backlog is reclaimed over hours rather than in one burst.

## Operating it

One listing pass per topic per cycle, one delete request per segment, no HEAD at all. Two known
costs — batched deletes and the repeated walk over an already-reclaimed prefix — are marked
`TODO` at their call sites in [`cleaner.rs`](../../../icegate-queue/src/cleaner.rs), each with
what blocks it.

Instruments are in [`metrics.rs`](metrics.rs), labelled by topic. Two of them are verdicts rather
than counts, and reading them apart matters: a cycle that derived no bound is the normal state of
a young table, while a stalled cycle means a bound WAS derived, candidates WERE found, and the
tail is still there — the queue is growing while the service reports itself as running.

Configuration defaults live in [`config.rs`](config.rs); the queue location cleanup addresses is
the `queue.common.base_path` of [`MaintainConfig`](../config.rs) and must be the one ingest
writes to. Deployment keys change in pairs — Helm under `maintain.walCleanup` in
[`values.yaml`](../../../../config/helm/icegate/values.yaml), Compose in
[`maintain.yaml`](../../../../config/docker/maintain.yaml).
