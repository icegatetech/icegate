# Parquet compaction

Compaction rewrites the many small Parquet files that ingest leaves in an
Iceberg table into fewer, larger, target-sized files, so queries scan fewer
objects and sort-key pruning stays effective. It runs as a long-running
background service (`maintain run`), never blocks ingest, and commits its work
as atomic Iceberg `replace` snapshots.

- Code: [`mod.rs`](mod.rs) (service), [`planner.rs`](planner.rs) (which files to
  rewrite), [`rewrite.rs`](rewrite.rs) (how they are merged), [`config.rs`](config.rs).
- Design: [`docs/superpowers/specs/2026-06-11-parquet-compaction-design.md`](../../../../docs/superpowers/specs/2026-06-11-parquet-compaction-design.md)
  and [`...-2026-06-14-compaction-size-aware-grouping-design.md`](../../../../docs/superpowers/specs/2026-06-14-compaction-size-aware-grouping-design.md).

## Service architecture

One jobmanager job per enabled table (`logs`, `spans`, `events`, `metrics`).
Each job is a `PLAN → REWRITE` pipeline. The PLAN task decides *what* to rewrite;
each REWRITE task it fans out does the merge and commits its own replace.

```mermaid
flowchart LR
    CFG[CompactionConfig] --> CMP[Compactor service]
    CMP --> J1[job: logs]
    CMP --> J2[job: spans]
    CMP --> J3[job: events / metrics]
    J1 --> P[PLAN task<br/>enumerate + plan groups]
    P -->|fan out one per group| R1[REWRITE task]
    P --> R2[REWRITE task]
    R1 -->|k-way merge + replace| IC[(Iceberg snapshot)]
    R2 --> IC
```

- **PLAN** loads the table fresh, enumerates its data files with per-column
  sort-key bounds, calls [`plan_rewrite_groups`](planner.rs), and dynamically
  fans out one REWRITE task per group. It schedules no commit task.
- **REWRITE** k-way-merges its group's already-sorted inputs into target-sized
  Parquet and atomically swaps inputs for outputs via Iceberg
  `Transaction::rewrite_files`, with optimistic-concurrency retry handled by the
  generic catalog (so concurrent ingest commits are tolerated).

## The planning pipeline

[`plan_rewrite_groups`](planner.rs) runs six stages per partition. Stages 1, 3,
5 are structural; stages 2, 4, 6 decide what is worth rewriting.

```mermaid
flowchart TD
    A[data files in current snapshot] --> B[1 · group by partition key]
    B --> C{2 · healthy partition?}
    C -- yes --> S1[skip]
    C -- no --> D[3 · cluster by sort-key overlap]
    D --> E[4 · split_cluster_by_size]
    E --> F[5 · bin-pack by input budget]
    F --> G{"6 · group has >= 2 files?"}
    G -- no --> S2["drop / leave file alone"]
    G -- yes --> H["emit rewrite group → REWRITE"]
```

| Stage | Function | Purpose |
|-------|----------|---------|
| 1 | `group_by_partition` | Never merge across `(tenant, day)` partitions. |
| 2 | `is_healthy` | Skip partitions with few files and no sub-target tail. |
| 3 | `swept_line_cluster` | Group transitively sort-key-overlapping files. |
| 4 | `split_cluster_by_size` | Keep size-similar files together (see below). |
| 5 | `bin_pack_into` | Cap each group at `max_group_input_bytes`. |
| 6 | `retain(len >= 2)` | Drop single-file groups (the convergence guard). |

**Why stage 6 matters:** rewriting one file 1-to-1 reduces nothing, so emitting
a single-file group would make every scan re-rewrite the same file forever. A
partition left with only single-file groups is reported as *skipped*, not
compacted.

## Inside `split_cluster_by_size`

This is the only stage with real size policy. Its goal: **merge files of similar
size, and never re-read a large, near-target file just to absorb a few small
ones.** It loops, peeling one size tier at a time.

```mermaid
flowchart TD
    A[cluster of overlapping files] --> B{"largest >= target_file_size_bytes?"}
    B -- no --> M1[merge all into one tier]
    B -- yes --> C{"any file smaller than largest / ratio?"}
    C -- no --> M2[all similar: one tier]
    C -- yes --> D{"smalls sum >= largest/2<br/>AND cluster fits the budget?"}
    D -- yes --> M3[absorb: keep the whole cluster]
    D -- no --> E["peel the comparable tier<br/>loop on the smaller files"]
    E --> B
```

Three rules, in order:

1. **Target shortcut** — if even the largest file is *below* target, re-reading
   it is cheap and merging shrinks the file count, so merge everything. This is
   the common case (a partition of small files from WAL shift).
2. **Ratio gate** (`max_merge_size_ratio`) — above target, a file joins the
   largest's tier only when `size * ratio >= largest` (i.e. it is at least
   `1/ratio` of the largest). Smaller files drop to lower tiers.
3. **Absorb override** — pull the small files into the large one anyway when they
   collectively reach half the largest **and** the whole cluster fits one rewrite
   group. The fit check prevents the bin-packer from later stranding a file.

### Ratio gate vs. absorb gate

These are two different questions that happen to share the same `largest / 2`
line under the defaults (`ratio = 2`). The difference is **what** is measured
against the line:

```
largest = 200K

  ratio gate  -> asks EACH file:            absorb gate -> asks the SUM of smalls:
  "is this one file >= largest/ratio?"      "do all smalls together >= largest/2?"

  200K ############  comparable
  ---- ---------------------------- largest/2 = 100K (the line) -------------------
   60K ###   small  ┐
   60K ###   small  ├─ sum = 140K  >= 100K  -> absorb*  (*only if cluster fits budget)
   20K #     small  ┘
```

- `max_merge_size_ratio` is **configurable** and looks at **one file at a time**
  (tier membership).
- `LARGE_FILE_ABSORB_DENOMINATOR` is a **hard-coded** `2` (one half) and looks at
  the **sum of the small files** (is the merge worth the re-read).

Change `ratio` to 3 and they diverge: a file is "small" below `largest/3`, but
absorb still triggers at `sum >= largest/2`.

## Worked example

One partition, `target = 100K`, `ratio = 2`, `budget = 250K`. Six files, in two
sort-key clusters:

```
input                 cluster (stage 3)        split (stage 4)        output (stages 5-6)
200K [10-40]   ┐                               tier {200K}        ->  dropped (lone, left alone)
 60K [12-38]   ├ cluster 1 (keys 10-40)  ->    tier {60,60,20}    ->  group (1): 140K
 60K [13-37]   │
 20K [14-36]   ┘
 20K [50-70]   ┐ cluster 2 (keys 50-70)  ->    tier {20,20}       ->  group (2): 40K
 20K [52-68]   ┘
```

Cluster 1 walks the `split_cluster_by_size` loop:

- **iteration 1** — largest `200K >= target`, so the gate runs. The smalls
  (60, 60, 20) sit below the `100K` line; their sum is `140K (>= 100K)`, but the
  whole cluster is `340K > 250K` budget, so absorb does **not** fire. The `200K`
  tier is peeled off; the loop continues on `{60, 60, 20}`.
- **iteration 2** — largest is now `60K < target`, so the shortcut merges all
  three.

Result: two rewrite groups (`140K`, `40K`); the `200K` file is never re-read.

## Configuration

Defaults live in [`config.rs`](config.rs); Helm keys in
[`config/helm/icegate/values.yaml`](../../../../config/helm/icegate/values.yaml)
under `compact.compaction`.

| Field (`snake_case`) | Helm (`camelCase`) | Default | Meaning |
|----------------------|--------------------|---------|---------|
| `target_file_size_bytes` | `targetFileSizeBytes` | 128 MiB | Desired output file size; below this a file is "sub-target". |
| `max_group_input_bytes` | `maxGroupInputBytes` | 256 MiB | Max summed input a single rewrite may read. |
| `min_input_files` | `minInputFiles` | 4 | A partition at or below this is a skip candidate. |
| `max_skippable_tail_files` | `maxSkippableTailFiles` | 0 | Tolerated sub-target files in a skip candidate. |
| `max_merge_size_ratio` | `maxMergeSizeRatio` | 2 | Largest-to-smallest size ratio within one group. Must be `>= 1` (rejected at startup otherwise). |
| `scan_interval_secs` | `scanIntervalSecs` | 300 | Discovery loop period. |
| `rewrite_timeout_secs` | `rewriteTimeoutSecs` | 3600 | Deadline for one REWRITE task. |
| `worker_count` | `workerCount` | half of CPUs | Concurrent REWRITE workers. |
| `{logs,spans,events,metrics}_enabled` | `…Enabled` | true | Per-table toggles. |

`LARGE_FILE_ABSORB_DENOMINATOR` (the absorb half-rule) is a module constant, not
configurable in this iteration.

## Guarantees and limitations

- **Convergence.** The single-file-group drop (stage 6) guarantees the planner
  never spins on a file that cannot beneficially merge.
- **No cross-partition or cross-cluster merges.** Outputs of one cluster stay in
  one `(tenant, day)` partition.
- **Accepted trade-off.** Excluding an overlapping over-target file means the
  small files' merged output overlaps it in sort-key range, weakening pruning for
  that partition in exchange for not re-reading the large file.
- **Known gap (`TODO(closed-partition)`).** A cold partition whose largest file
  is over target and whose small tail sums to less than half will not converge to
  a single file — the large file escapes the gate each scan and is left alone. A
  future change would detect "closed" partitions (day old enough that no further
  writes land) and bypass the size gate.
