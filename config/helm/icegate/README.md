# icegate

![Version: 0.2.0](https://img.shields.io/badge/Version-0.2.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.2.0](https://img.shields.io/badge/AppVersion-0.2.0-informational?style=flat-square)

IceGate is an observability data lake engine. This chart deploys it on Kubernetes:
OpenTelemetry data arrives over OTLP, is written to a write-ahead log on object
storage, and lands in Apache Iceberg tables stored as Parquet. Queries run on Apache
DataFusion and are served through Loki- and Tempo-compatible HTTP APIs and Arrow
Flight SQL. There is no OLTP database in the deployment: the Iceberg catalog is backed
by object storage, and compute scales separately from the data it reads.

## Status and stability

IceGate is a prototype. The released chart versions are `0.1.1` and a series of
`0.2.0` release candidates. Interfaces, values and the Iceberg schema may change
between releases, and there is no upgrade compatibility guarantee yet. Read the
release notes before upgrading.

## Install

```bash
helm install icegate oci://ghcr.io/icegatetech/charts/icegate
```

To pin a version:

```bash
helm install icegate oci://ghcr.io/icegatetech/charts/icegate --version 0.2.0
```

## What this chart deploys

Four components, each independently enabled and scaled:

| Component | Responsibility |
|---|---|
| `ingest` | Receives OTLP over gRPC and HTTP, writes to the WAL on object storage |
| `query` | Serves the Loki-, Tempo- and Prometheus-shaped HTTP APIs and Arrow Flight SQL, reading both Iceberg tables and the live WAL |
| `maintain` | Background compaction, garbage collection and WAL reclamation |
| `catalog` | Read-only Iceberg REST catalog API over the object-storage catalog. Off by default |

A `migrate` Job runs as a Helm hook to create or update the Iceberg tables.

## Requirements

- Kubernetes with a working `StorageClass` if you run object storage in-cluster.
- S3 or an S3-compatible object store, reachable from every enabled component.
- Credentials supplied through `aws.existingSecret`, or an IRSA role on the
  ServiceAccount via `serviceAccount.annotations`.

## Running against S3-compatible storage

The default catalog backend is `s3`: IceGate keeps its own Iceberg catalog state in
object storage, so no external catalog service is needed. Point `storage.s3` at your
endpoint and set credentials. S3 addressing is derived from the endpoint by default —
path-style for S3-compatible endpoints, virtual-hosted on AWS S3 — so leave
`catalog.s3.pathStyleAccess` unset unless you need to overrule that.

## Using an external Iceberg REST catalog

Set `catalog.backend` to `rest` and give `catalog.rest.uri` the catalog's address. The
chart does not deploy a REST catalog, so the URI has no default and the render fails
without one. `s3tables` and `glue` backends are also available; see the values table.

## Querying with Loki- and Tempo-compatible APIs

The `query` component exposes HTTP APIs that speak the Loki and Tempo query protocols,
so existing clients for those protocols can read IceGate directly. Arrow Flight SQL is
available for SQL access. Each listener is enabled and given a port independently under
`query`; the values table below lists the keys.

## Verifying the chart signature

Chart releases are signed with cosign using the release workflow's identity, so there
is no public key to distribute:

```bash
cosign verify ghcr.io/icegatetech/charts/icegate:0.2.0 \
  --certificate-identity-regexp 'https://github.com/icegatetech/icegate/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| aws.existingSecret | string | `""` |  |
| aws.region | string | `"us-east-1"` |  |
| catalog.backend | string | `"s3"` |  |
| catalog.glue.catalogId | string | `""` |  |
| catalog.properties | object | `{}` |  |
| catalog.rest.uri | string | `""` |  |
| catalog.s3.bucket | string | `""` |  |
| catalog.s3.warehouse | string | `"catalog"` |  |
| catalog.s3tables.tableBucketArn | string | `""` |  |
| catalog.warehouse | string | `"s3://warehouse/"` |  |
| catalogServer.affinity | object | `{}` |  |
| catalogServer.enabled | bool | `false` |  |
| catalogServer.extraEnv | list | `[]` |  |
| catalogServer.http.port | int | `8181` |  |
| catalogServer.http.prefix | string | `""` |  |
| catalogServer.image.pullPolicy | string | `"IfNotPresent"` |  |
| catalogServer.image.repository | string | `"ghcr.io/icegatetech/icegate-catalog"` |  |
| catalogServer.image.tag | string | `""` |  |
| catalogServer.nodeSelector | object | `{}` |  |
| catalogServer.pagination.defaultPageSize | int | `100` |  |
| catalogServer.pagination.maxPageSize | int | `1000` |  |
| catalogServer.replicaCount | int | `1` |  |
| catalogServer.resources.limits.cpu | string | `"1"` |  |
| catalogServer.resources.limits.memory | string | `"512Mi"` |  |
| catalogServer.resources.requests.cpu | string | `"250m"` |  |
| catalogServer.resources.requests.memory | string | `"256Mi"` |  |
| catalogServer.rustLog | string | `"info"` |  |
| catalogServer.service.annotations | object | `{}` |  |
| catalogServer.service.type | string | `"ClusterIP"` |  |
| catalogServer.tolerations | list | `[]` |  |
| global.fullnameOverride | string | `""` |  |
| global.imagePullSecrets | list | `[]` |  |
| global.nameOverride | string | `""` |  |
| ingest.affinity | object | `{}` |  |
| ingest.cache.diskDir | string | `"/tmp/icegate/cache"` |  |
| ingest.cache.diskSizeMb | int | `4096` |  |
| ingest.cache.enabled | bool | `false` |  |
| ingest.cache.maxWriteCacheSizeMb | int | `2` |  |
| ingest.cache.memorySizeMb | int | `1024` |  |
| ingest.cache.prefetch.enabled | bool | `false` |  |
| ingest.cache.statTtlSecs | int | `300` |  |
| ingest.cache.volumeSizeLimit | string | `"5Gi"` |  |
| ingest.enabled | bool | `true` |  |
| ingest.extraEnv | list | `[]` |  |
| ingest.image.pullPolicy | string | `"IfNotPresent"` |  |
| ingest.image.repository | string | `"ghcr.io/icegatetech/icegate-ingest"` |  |
| ingest.image.tag | string | `""` |  |
| ingest.ingress.annotations | object | `{}` |  |
| ingest.ingress.className | string | `""` |  |
| ingest.ingress.enabled | bool | `false` |  |
| ingest.ingress.hosts | list | `[]` |  |
| ingest.ingress.tls | list | `[]` |  |
| ingest.livenessProbe.failureThreshold | int | `3` |  |
| ingest.livenessProbe.initialDelaySeconds | int | `15` |  |
| ingest.livenessProbe.periodSeconds | int | `20` |  |
| ingest.livenessProbe.tcpSocket.port | int | `4317` |  |
| ingest.livenessProbe.timeoutSeconds | int | `5` |  |
| ingest.metrics.enabled | bool | `true` |  |
| ingest.metrics.path | string | `"/metrics"` |  |
| ingest.metrics.port | int | `9091` |  |
| ingest.nodeSelector | object | `{}` |  |
| ingest.otlpGrpc.enabled | bool | `true` |  |
| ingest.otlpGrpc.port | int | `4317` |  |
| ingest.otlpHttp.enabled | bool | `true` |  |
| ingest.otlpHttp.maxBodyBytes | int | `16777216` |  |
| ingest.otlpHttp.port | int | `4318` |  |
| ingest.pdb.enabled | bool | `false` |  |
| ingest.pdb.minAvailable | int | `1` |  |
| ingest.readinessProbe.failureThreshold | int | `3` |  |
| ingest.readinessProbe.initialDelaySeconds | int | `5` |  |
| ingest.readinessProbe.periodSeconds | int | `10` |  |
| ingest.readinessProbe.tcpSocket.port | int | `4317` |  |
| ingest.readinessProbe.timeoutSeconds | int | `5` |  |
| ingest.replicaCount | int | `1` |  |
| ingest.resources.limits.cpu | string | `"2"` |  |
| ingest.resources.limits.memory | string | `"2Gi"` |  |
| ingest.resources.requests.cpu | string | `"500m"` |  |
| ingest.resources.requests.memory | string | `"1Gi"` |  |
| ingest.rustLog | string | `"info"` |  |
| ingest.service.annotations | object | `{}` |  |
| ingest.service.type | string | `"ClusterIP"` |  |
| ingest.shift.jobsmanager.iterationIntervalMillisecs | int | `30000` |  |
| ingest.shift.jobsmanager.pollIntervalMs | int | `1000` |  |
| ingest.shift.jobsmanager.storage.bucket | string | `"jobs"` |  |
| ingest.shift.jobsmanager.storage.endpoint | string | `""` |  |
| ingest.shift.jobsmanager.storage.jobStateCodec | string | `"json"` |  |
| ingest.shift.jobsmanager.storage.prefix | string | `"shifter"` |  |
| ingest.shift.jobsmanager.storage.region | string | `""` |  |
| ingest.shift.jobsmanager.storage.requestTimeoutSecs | int | `5` |  |
| ingest.shift.jobsmanager.workerCount | int | `2` |  |
| ingest.shift.read.lowerBoundInputBytesPerTask | int | `67108864` |  |
| ingest.shift.read.maxRecordBatchesPerTask | int | `192` |  |
| ingest.shift.read.planSegmentReadParallelism | int | `8` |  |
| ingest.shift.read.shiftSegmentReadParallelism | int | `8` |  |
| ingest.shift.read.upperBoundInputBytesPerTask | int | `134217728` |  |
| ingest.shift.timeouts.commitBaseMs | int | `30000` |  |
| ingest.shift.timeouts.commitPerParquetFileMs | int | `100` |  |
| ingest.shift.timeouts.planBaseMs | int | `10000` |  |
| ingest.shift.timeouts.shiftBaseMs | int | `10000` |  |
| ingest.shift.timeouts.shiftPerRecordBatchMs | int | `50` |  |
| ingest.shift.timeouts.shiftPerSegmentMs | int | `300` |  |
| ingest.shift.write.dataPageSizeLimitBytes | int | `2097152` |  |
| ingest.shift.write.rowGroupSize | int | `20000` |  |
| ingest.shift.write.tableCacheTtlSecs | int | `60` |  |
| ingest.tolerations | list | `[]` |  |
| ingest.topologySpreadConstraints | list | `[]` |  |
| ingest.tracing.enabled | bool | `true` |  |
| ingest.tracing.otlpEndpoint | string | `""` |  |
| ingest.tracing.sampleRatio | float | `1` |  |
| ingest.waitForDependencies.awsCliImage | string | `"amazon/aws-cli:2.36.34"` |  |
| ingest.waitForDependencies.buckets | list | `[]` |  |
| ingest.waitForDependencies.enabled | bool | `false` |  |
| ingest.waitForDependencies.endpoints | list | `[]` |  |
| ingest.waitForDependencies.image | string | `"busybox:1.38.0"` |  |
| maintain.affinity | object | `{}` |  |
| maintain.compaction.data.dataPageSizeLimitBytes | int | `2097152` |  |
| maintain.compaction.data.maxGroupInputBytes | int | `268435456` |  |
| maintain.compaction.data.maxMergeSizeRatio | int | `2` |  |
| maintain.compaction.data.maxSkippableTailFiles | int | `0` |  |
| maintain.compaction.data.minInputFiles | int | `4` |  |
| maintain.compaction.data.rewriteTimeoutSecs | int | `3600` |  |
| maintain.compaction.data.rowGroupSize | int | `20000` |  |
| maintain.compaction.data.targetFileSizeBytes | int | `134217728` |  |
| maintain.compaction.jobsmanager.pollIntervalMs | int | `1000` |  |
| maintain.compaction.jobsmanager.scanIntervalSecs | int | `300` |  |
| maintain.compaction.jobsmanager.storage.bucket | string | `"jobs"` |  |
| maintain.compaction.jobsmanager.storage.endpoint | string | `""` |  |
| maintain.compaction.jobsmanager.storage.jobStateCodec | string | `"json"` |  |
| maintain.compaction.jobsmanager.storage.prefix | string | `"compactor"` |  |
| maintain.compaction.jobsmanager.storage.region | string | `""` |  |
| maintain.compaction.jobsmanager.storage.requestTimeoutSecs | int | `5` |  |
| maintain.compaction.jobsmanager.workerCount | int | `2` |  |
| maintain.compaction.manifest.candidateSizeRatio | float | `0.75` |  |
| maintain.compaction.manifest.maxManifestsPerCommit | int | `64` |  |
| maintain.compaction.manifest.rewriteTimeoutSecs | int | `600` |  |
| maintain.compaction.manifest.targetSizeBytes | int | `8388608` |  |
| maintain.compaction.tables.events | bool | `true` |  |
| maintain.compaction.tables.logs | bool | `true` |  |
| maintain.compaction.tables.metrics | bool | `true` |  |
| maintain.compaction.tables.operations | bool | `true` |  |
| maintain.compaction.tables.spans | bool | `true` |  |
| maintain.enabled | bool | `false` |  |
| maintain.extraEnv | list | `[]` |  |
| maintain.gc.enabled | bool | `false` |  |
| maintain.gc.jobsmanager.pollIntervalMs | int | `1000` |  |
| maintain.gc.jobsmanager.scanIntervalSecs | int | `86400` |  |
| maintain.gc.jobsmanager.storage.bucket | string | `"jobs"` |  |
| maintain.gc.jobsmanager.storage.endpoint | string | `""` |  |
| maintain.gc.jobsmanager.storage.jobStateCodec | string | `"json"` |  |
| maintain.gc.jobsmanager.storage.prefix | string | `"gc"` |  |
| maintain.gc.jobsmanager.storage.region | string | `""` |  |
| maintain.gc.jobsmanager.storage.requestTimeoutSecs | int | `5` |  |
| maintain.gc.jobsmanager.workerCount | int | `1` |  |
| maintain.gc.orphans.deleteConcurrency | int | `16` |  |
| maintain.gc.orphans.dryRun | bool | `false` |  |
| maintain.gc.orphans.enabled | bool | `true` |  |
| maintain.gc.orphans.includeMetadata | bool | `true` |  |
| maintain.gc.orphans.minAgeSecs | int | `604800` |  |
| maintain.gc.orphans.sweepTimeoutSecs | int | `3600` |  |
| maintain.gc.tables.events | bool | `true` |  |
| maintain.gc.tables.logs | bool | `true` |  |
| maintain.gc.tables.metrics | bool | `true` |  |
| maintain.gc.tables.operations | bool | `true` |  |
| maintain.gc.tables.spans | bool | `true` |  |
| maintain.image.pullPolicy | string | `"IfNotPresent"` |  |
| maintain.image.repository | string | `"ghcr.io/icegatetech/icegate-maintain"` |  |
| maintain.image.tag | string | `""` |  |
| maintain.livenessProbe | object | `{}` |  |
| maintain.metrics.enabled | bool | `true` |  |
| maintain.metrics.path | string | `"/metrics"` |  |
| maintain.metrics.port | int | `9091` |  |
| maintain.nodeSelector | object | `{}` |  |
| maintain.pricing.billingRegion | object | `{}` |  |
| maintain.pricing.crawlTimeoutSecs | int | `600` |  |
| maintain.pricing.enabled | bool | `false` |  |
| maintain.pricing.intervalSecs | int | `21600` |  |
| maintain.pricing.jobsmanager.pollIntervalMs | int | `1000` |  |
| maintain.pricing.jobsmanager.scanIntervalSecs | int | `21600` |  |
| maintain.pricing.jobsmanager.storage.bucket | string | `"jobs"` |  |
| maintain.pricing.jobsmanager.storage.endpoint | string | `""` |  |
| maintain.pricing.jobsmanager.storage.jobStateCodec | string | `"json"` |  |
| maintain.pricing.jobsmanager.storage.prefix | string | `"pricing"` |  |
| maintain.pricing.jobsmanager.storage.region | string | `""` |  |
| maintain.pricing.jobsmanager.storage.requestTimeoutSecs | int | `5` |  |
| maintain.pricing.jobsmanager.workerCount | int | `1` |  |
| maintain.pricing.maxChangeRatio | float | `10` |  |
| maintain.pricing.maxResponseBytes | int | `134217728` |  |
| maintain.pricing.minModelCountRatio | float | `0.8` |  |
| maintain.pricing.sources[0].name | string | `"openrouter"` |  |
| maintain.pricing.sources[0].url | string | `"https://openrouter.ai/api/v1/models"` |  |
| maintain.pricing.sources[1].name | string | `"litellm"` |  |
| maintain.pricing.sources[1].url | string | `"https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"` |  |
| maintain.pricing.timeoutSecs | int | `60` |  |
| maintain.replicaCount | int | `1` |  |
| maintain.resources.limits.cpu | string | `"2"` |  |
| maintain.resources.limits.memory | string | `"2Gi"` |  |
| maintain.resources.requests.cpu | string | `"500m"` |  |
| maintain.resources.requests.memory | string | `"1Gi"` |  |
| maintain.rustLog | string | `"info"` |  |
| maintain.service.annotations | object | `{}` |  |
| maintain.service.type | string | `"ClusterIP"` |  |
| maintain.tolerations | list | `[]` |  |
| maintain.tracing.enabled | bool | `true` |  |
| maintain.tracing.otlpEndpoint | string | `""` |  |
| maintain.tracing.sampleRatio | float | `1` |  |
| maintain.waitForDependencies.awsCliImage | string | `"amazon/aws-cli:2.36.34"` |  |
| maintain.waitForDependencies.buckets | list | `[]` |  |
| maintain.waitForDependencies.enabled | bool | `false` |  |
| maintain.waitForDependencies.endpoints | list | `[]` |  |
| maintain.waitForDependencies.image | string | `"busybox:1.38.0"` |  |
| maintain.walCleanup.cleanupTimeoutSecs | int | `600` |  |
| maintain.walCleanup.deleteConcurrency | int | `16` |  |
| maintain.walCleanup.dryRun | bool | `false` |  |
| maintain.walCleanup.enabled | bool | `false` |  |
| maintain.walCleanup.jobsmanager.pollIntervalMs | int | `1000` |  |
| maintain.walCleanup.jobsmanager.scanIntervalSecs | int | `600` |  |
| maintain.walCleanup.jobsmanager.storage.bucket | string | `"jobs"` |  |
| maintain.walCleanup.jobsmanager.storage.endpoint | string | `""` |  |
| maintain.walCleanup.jobsmanager.storage.jobStateCodec | string | `"json"` |  |
| maintain.walCleanup.jobsmanager.storage.prefix | string | `"wal_cleanup"` |  |
| maintain.walCleanup.jobsmanager.storage.region | string | `""` |  |
| maintain.walCleanup.jobsmanager.storage.requestTimeoutSecs | int | `5` |  |
| maintain.walCleanup.jobsmanager.workerCount | int | `2` |  |
| maintain.walCleanup.keepSegmentsCount | int | `1200` |  |
| maintain.walCleanup.maxDeletesPerCycle | int | `50000` |  |
| maintain.walCleanup.topics.logs | bool | `true` |  |
| maintain.walCleanup.topics.metrics | bool | `true` |  |
| maintain.walCleanup.topics.operations | bool | `true` |  |
| maintain.walCleanup.topics.spans | bool | `true` |  |
| migrate.activeDeadlineSeconds | int | `300` |  |
| migrate.backoffLimit | int | `3` |  |
| migrate.enabled | bool | `true` |  |
| migrate.hookDeletePolicy | string | `"before-hook-creation"` |  |
| migrate.image.pullPolicy | string | `"IfNotPresent"` |  |
| migrate.image.repository | string | `"ghcr.io/icegatetech/icegate-maintain"` |  |
| migrate.image.tag | string | `""` |  |
| migrate.resources.limits.cpu | string | `"500m"` |  |
| migrate.resources.limits.memory | string | `"256Mi"` |  |
| migrate.resources.requests.cpu | string | `"100m"` |  |
| migrate.resources.requests.memory | string | `"128Mi"` |  |
| migrate.snapshotExpiration.enabled | bool | `true` |  |
| migrate.snapshotExpiration.maxSnapshotAgeMs | int | `1800000` |  |
| migrate.snapshotExpiration.metadataPreviousVersionsMax | int | `200` |  |
| migrate.snapshotExpiration.minSnapshotsToKeep | int | `100` |  |
| migrate.waitForDependencies.awsCliImage | string | `"amazon/aws-cli:2.36.34"` |  |
| migrate.waitForDependencies.buckets | list | `[]` |  |
| migrate.waitForDependencies.enabled | bool | `true` |  |
| migrate.waitForDependencies.endpoints | list | `[]` |  |
| migrate.waitForDependencies.image | string | `"busybox:1.38.0"` |  |
| namespace.create | bool | `false` |  |
| query.affinity | object | `{}` |  |
| query.cache.diskDir | string | `"/tmp/icegate/cache"` |  |
| query.cache.diskSizeMb | int | `4096` |  |
| query.cache.enabled | bool | `true` |  |
| query.cache.maxWriteCacheSizeMb | int | `2` |  |
| query.cache.memorySizeMb | int | `1024` |  |
| query.cache.prefetch.enabled | bool | `false` |  |
| query.cache.statTtlSecs | int | `300` |  |
| query.cache.volumeSizeLimit | string | `"5Gi"` |  |
| query.enabled | bool | `true` |  |
| query.engine.batchSize | int | `8192` |  |
| query.engine.catalogName | string | `"iceberg"` |  |
| query.engine.maxAgeSecs | int | `30` |  |
| query.engine.maxQueryDurationSecs | int | `30` |  |
| query.engine.targetPartitions | int | `4` |  |
| query.engine.walMetadataSizeHint | int | `65536` |  |
| query.engine.walQueryEnabled | bool | `false` |  |
| query.extraEnv | list | `[]` |  |
| query.flightSql.enabled | bool | `true` |  |
| query.flightSql.maxMessageSize | int | `16777216` |  |
| query.flightSql.port | int | `8815` |  |
| query.image.pullPolicy | string | `"IfNotPresent"` |  |
| query.image.repository | string | `"ghcr.io/icegatetech/icegate-query"` |  |
| query.image.tag | string | `""` |  |
| query.ingress.annotations | object | `{}` |  |
| query.ingress.className | string | `""` |  |
| query.ingress.enabled | bool | `false` |  |
| query.ingress.hosts | list | `[]` |  |
| query.ingress.tls | list | `[]` |  |
| query.livenessProbe.failureThreshold | int | `3` |  |
| query.livenessProbe.httpGet.path | string | `"/ready"` |  |
| query.livenessProbe.httpGet.port | int | `3100` |  |
| query.livenessProbe.initialDelaySeconds | int | `15` |  |
| query.livenessProbe.periodSeconds | int | `20` |  |
| query.livenessProbe.timeoutSeconds | int | `5` |  |
| query.loki.enabled | bool | `true` |  |
| query.loki.port | int | `3100` |  |
| query.metrics.enabled | bool | `true` |  |
| query.metrics.path | string | `"/metrics"` |  |
| query.metrics.port | int | `9091` |  |
| query.nodeSelector | object | `{}` |  |
| query.pdb.enabled | bool | `false` |  |
| query.pdb.minAvailable | int | `1` |  |
| query.prometheus.enabled | bool | `true` |  |
| query.prometheus.port | int | `9090` |  |
| query.readinessProbe.failureThreshold | int | `3` |  |
| query.readinessProbe.httpGet.path | string | `"/ready"` |  |
| query.readinessProbe.httpGet.port | int | `3100` |  |
| query.readinessProbe.initialDelaySeconds | int | `5` |  |
| query.readinessProbe.periodSeconds | int | `10` |  |
| query.readinessProbe.timeoutSeconds | int | `5` |  |
| query.replicaCount | int | `1` |  |
| query.resources.limits.cpu | string | `"4"` |  |
| query.resources.limits.memory | string | `"4Gi"` |  |
| query.resources.requests.cpu | string | `"1"` |  |
| query.resources.requests.memory | string | `"2Gi"` |  |
| query.rustLog | string | `"info"` |  |
| query.service.annotations | object | `{}` |  |
| query.service.type | string | `"ClusterIP"` |  |
| query.tempo.enabled | bool | `true` |  |
| query.tempo.port | int | `3200` |  |
| query.tolerations | list | `[]` |  |
| query.topologySpreadConstraints | list | `[]` |  |
| query.tracing.enabled | bool | `true` |  |
| query.tracing.otlpEndpoint | string | `""` |  |
| query.tracing.sampleRatio | float | `1` |  |
| query.waitForDependencies.awsCliImage | string | `"amazon/aws-cli:2.36.34"` |  |
| query.waitForDependencies.buckets | list | `[]` |  |
| query.waitForDependencies.enabled | bool | `false` |  |
| query.waitForDependencies.endpoints | list | `[]` |  |
| query.waitForDependencies.image | string | `"busybox:1.38.0"` |  |
| queue.common.basePath | string | `"s3://queue/"` |  |
| queue.common.channelCapacity | int | `64` |  |
| queue.common.maxRowGroupSize | int | `16384` |  |
| queue.read.metadataEntriesCacheCapacity | int | `2048` |  |
| queue.write.compression | string | `"zstd"` |  |
| queue.write.flushIntervalMs | int | `200` |  |
| queue.write.maxBytesPerFlush | int | `67108864` |  |
| queue.write.recordsPerFlushMultiplier | int | `1` |  |
| queue.write.writeRetries | int | `3` |  |
| serviceAccount.annotations | object | `{}` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.name | string | `""` |  |
| serviceMonitor.enabled | bool | `false` |  |
| serviceMonitor.interval | string | `"30s"` |  |
| serviceMonitor.labels | object | `{}` |  |
| serviceMonitor.scrapeTimeout | string | `"10s"` |  |
| storage.s3.bucket | string | `"warehouse"` |  |
| storage.s3.endpoint | string | `""` |  |
| storage.s3.region | string | `"us-east-1"` |  |

## Links

## Source Code

* <https://github.com/icegatetech/icegate>

- [Documentation](https://docs.icegate.tech)
- [Issue tracker](https://github.com/icegatetech/icegate/issues)

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| icegatetech |  | <https://github.com/icegatetech> |
