# Parquet compaction

Compaction rewrites the small files an Iceberg table accumulates — both the many
small Parquet data files that ingest leaves and the metadata manifests that track
them — into fewer, larger, target-sized ones, so queries scan and plan over fewer
objects. It runs as a long-running background service (`maintain run`), never
blocks ingest, and commits its work as atomic Iceberg `replace` snapshots.

The algorithm (which files to merge, size tiers, convergence) lives in each
module's own doc comments — this file stays at the level that no single source
file can show: how the pieces fit and which guarantees hold across them.

## Structure

Two axes: the compaction **domain** (data files vs manifests) and the **layer**
(jobmanager task → domain logic → assembly). Each domain is planner + rewrite.

| Path                           | Layer    | Where to look                                                                                       |
|--------------------------------|----------|-----------------------------------------------------------------------------------------------------|
| [`compactor.rs`](compactor.rs) | assembly | Per-table job specs and the `Compactor` service — the only module describing jobs to the jobmanager. |
| [`data/`](data/)               | domain   | Data-file compaction.                                                                               |
| [`manifest/`](manifest/)       | domain   | Manifest compaction.                                                                                |

## Service

One jobmanager job per enabled table. Each job is a `compact_plan →
compact_files` pipeline: `compact_plan` decides *what* to rewrite and fans out
one `compact_files` per group; each `compact_files` does its own merge and
commits its own replace, with optimistic-concurrency retry from the generic
catalog (so concurrent ingest commits are tolerated). `compact_plan` also fans
out one `compact_manifest` task, gated on those rewrites, that repacks the
manifests they leave behind.

Every fanned-out task opens its OWN trace and joins the planner's by a **span
link**, not by parent-child: a link is the only relation that spans two traces,
and the tasks genuinely are separate traces (see
[`tasks::link_planning_span`](tasks.rs)). The link graph is a star centred on
PLAN — both REWRITE and MANIFEST link to PLAN, and no `rewrite → manifest` edge
is built even though the dependency exists: a REWRITE task's output is empty, so
the repack would have to read every dependency's job state purely for telemetry.
Tasks queued before the payload field existed (and any run with tracing disabled)
simply carry no link.

#### Job scheme
```mermaid
flowchart TD
    START(["iteraion: every scan_interval_secs"]) --> PLAN

    subgraph P["compact_plan task"]
        PLAN["load_table fresh"] --> HS{"current_snapshot?"}
        HS -->|None| DONE0["return Completed"]
        HS -->|Some| ENUM["enumerate + plan_rewrite_groups"]
        ENUM --> FAN["add_task ×N + add_task manifest"]
        FAN --> DONEP["return Completed"]
    end

    FAN --> F1
    FAN --> F2
    FAN --> FN
    FAN -.->|"Blocked, deps = [ref₁..refₙ]"| MAN

    subgraph W["N × compact_files tasks"]
        F1["#1: merge → write"] --> K1{{"commit"}} --> D1["return Completed"]
        F2["#2: merge → write"] --> K2{{"commit"}} --> D2["return Completed"]
        FN["#N: merge → write"] --> KN{{"commit"}} --> DN["return Completed"]
        F1 -.->|"maybe failed"| D1
    end

    D1 ==>|Completed| MAN
    D2 ==>|Completed| MAN
    DN ==>|Completed| MAN

    subgraph M["compact_manifest task (gate for complete compact_files tasks)"]
        MAN["load fresh + plan repack"] --> KM{{"commit"}} --> DM["return Completed"]
        MAN -.->|"Skip / NoReduction"| DM
    end

    DM --> NEXT(["continue"])
    DONE0 --> NEXT
```

## Guarantees and limitations

- **Convergence.** A group is kept only when it actually reduces the partition's
  file count (`ceil(sum_bytes / target) < len`). Single-file groups, and groups
  whose inputs are each already at/above target (`N` in → `N` out), are dropped,
  so the planner never spins re-rewriting files that cannot beneficially merge.
- **No cross-partition merges.** All files in a group share one `(tenant, day)`.
- **No sort-key clustering.** Files with disjoint sort-key ranges in one
  partition may merge into a single output whose range then spans them, weakening
  per-file pruning for that partition (the k-way merge still yields a sorted
  output). Over-target disjoint files are *not* merged: re-rolling them at target
  size reduces no file count, so the convergence guard drops the group — this is
  what prevents an infinite re-rewrite loop.
- **Known gap** ([`TODO`](data/planner.rs)). A cold partition
  whose largest file is over target and whose sub-target tail sums to less than
  half of it never converges to one file: the large file escapes the size gate
  every scan. A future change would detect "closed" partitions (day old enough
  that no further writes land) and bypass the gate.

## Configuration

Defaults live in [`config.rs`](config.rs); Helm keys in
[`config/helm/icegate/values.yaml`](../../../../config/helm/icegate/values.yaml)
under `maintain.compaction`.
