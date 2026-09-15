{{/*
Namespace name. Always uses .Release.Namespace for consistency with all templates.
*/}}
{{- define "icegate.namespace" -}}
{{- .Release.Namespace }}
{{- end }}

{{/*
Expand the name of the chart.
*/}}
{{- define "icegate.name" -}}
{{- default .Chart.Name .Values.global.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this.
If release name contains chart name it will be used as a full name.
*/}}
{{- define "icegate.fullname" -}}
{{- if .Values.global.fullnameOverride }}
{{- .Values.global.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.global.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "icegate.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Component name: {{ fullname }}-{{ component }}
Usage: include "icegate.componentName" (dict "context" . "component" "query")
*/}}
{{- define "icegate.componentName" -}}
{{- printf "%s-%s" (include "icegate.fullname" .context) .component | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels shared by all resources.
*/}}
{{- define "icegate.labels" -}}
helm.sh/chart: {{ include "icegate.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end }}

{{/*
Component labels: common labels + component-specific labels.
Usage: include "icegate.componentLabels" (dict "context" . "component" "query")
*/}}
{{- define "icegate.componentLabels" -}}
{{ include "icegate.labels" .context }}
{{ include "icegate.selectorLabels" (dict "context" .context "component" .component) }}
{{- end }}

{{/*
Selector labels for a component.
Usage: include "icegate.selectorLabels" (dict "context" . "component" "query")
*/}}
{{- define "icegate.selectorLabels" -}}
app.kubernetes.io/name: {{ include "icegate.name" .context }}
app.kubernetes.io/instance: {{ .context.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "icegate.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "icegate.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Image reference for a component.
Usage: include "icegate.image" (dict "image" .Values.query.image "tag" .Chart.AppVersion)
*/}}
{{- define "icegate.image" -}}
{{- printf "%s:%s" .image.repository (.image.tag | default .tag) }}
{{- end }}

{{/*
Render catalog YAML block (zero-indented).
Produces the YAML tagged union that serde expects: `!rest { uri: ... }` or `!s3tables { ... }`.
Callers must use `nindent N` to place at the correct indentation level.
*/}}
{{- define "icegate.catalogYaml" -}}
{{- if eq .Values.catalog.backend "rest" -}}
backend: !rest
  uri: {{ required "catalog.rest.uri is required when catalog.backend=rest" .Values.catalog.rest.uri }}
warehouse: {{ .Values.catalog.warehouse }}
{{- if .Values.catalog.properties }}
properties:
{{- range $key, $val := .Values.catalog.properties }}
  {{ $key }}: {{ $val | quote }}
{{- end }}
{{- end }}
{{- else if eq .Values.catalog.backend "s3tables" -}}
backend: !s3tables
  table_bucket_arn: {{ .Values.catalog.s3tables.tableBucketArn }}
warehouse: {{ .Values.catalog.warehouse }}
{{- if .Values.catalog.properties }}
properties:
{{- range $key, $val := .Values.catalog.properties }}
  {{ $key }}: {{ $val | quote }}
{{- end }}
{{- end }}
{{- else if eq .Values.catalog.backend "glue" -}}
{{- if .Values.catalog.glue.catalogId }}
backend: !glue
  catalog_id: {{ .Values.catalog.glue.catalogId }}
{{- else }}
backend: !glue {}
{{- end }}
warehouse: {{ .Values.catalog.warehouse }}
{{- if .Values.catalog.properties }}
properties:
{{- range $key, $val := .Values.catalog.properties }}
  {{ $key }}: {{ $val | quote }}
{{- end }}
{{- end }}
{{- else if eq .Values.catalog.backend "s3" -}}
backend: !s3
  warehouse: {{ .Values.catalog.s3.warehouse | quote }}
warehouse: {{ .Values.catalog.warehouse }}
properties:
  bucket: {{ .Values.catalog.s3.bucket | default .Values.storage.s3.bucket | quote }}
  region: {{ .Values.storage.s3.region | quote }}
  {{- if .Values.storage.s3.endpoint }}
  endpoint: {{ .Values.storage.s3.endpoint | quote }}
  {{- end }}
  {{- with .Values.catalog.s3.codec }}
  codec: {{ . | quote }}
  {{- end }}
  {{- if not (kindIs "invalid" .Values.catalog.s3.pathStyleAccess) }}
  s3.path-style-access: {{ .Values.catalog.s3.pathStyleAccess | quote }}
  {{- end }}
  {{- /* Forward user-supplied catalog.properties into FileIO, skipping the
         structural keys already rendered above to avoid duplicate YAML keys. */}}
  {{- range $key, $val := .Values.catalog.properties }}
  {{- if not (has $key (list "bucket" "region" "endpoint" "codec" "s3.path-style-access")) }}
  {{ $key }}: {{ $val | quote }}
  {{- end }}
  {{- end }}
{{- end }}
{{- end }}

{{/*
Render storage YAML block (zero-indented).
Produces the YAML tagged union: `!s3 { bucket, region, endpoint? }`.
Callers must use `nindent N` to place at the correct indentation level.
*/}}
{{- define "icegate.storageYaml" -}}
backend: !s3
  bucket: {{ .Values.storage.s3.bucket }}
  region: {{ .Values.storage.s3.region }}
{{- if .Values.storage.s3.endpoint }}
  endpoint: {{ .Values.storage.s3.endpoint }}
{{- end }}
{{- end }}

{{/*
Render the ingest tenant policy (zero-indented).
Produces the YAML tagged union: `!single { id }` or `!multi`.
Callers must use `nindent N` to place at the correct indentation level.

`fail` rather than a default: a `single` policy with no id would deserialize into
an empty tenant that no request can name, and the pod would reject every batch.
*/}}
{{- define "icegate.tenantYaml" -}}
{{- if eq .Values.ingest.tenant.mode "single" -}}
{{- if not .Values.ingest.tenant.id }}{{ fail "ingest.tenant.id is required when ingest.tenant.mode is single" }}{{ end -}}
tenant: !single
  id: {{ .Values.ingest.tenant.id | quote }}
{{- else if eq .Values.ingest.tenant.mode "multi" -}}
tenant: !multi
{{- else -}}
{{ fail (printf "ingest.tenant.mode must be single or multi, got %q" .Values.ingest.tenant.mode) }}
{{- end -}}
{{- end }}

{{/*
Answer whether a listener host is reachable only from inside the container.

Returns a non-empty string for a loopback address and the empty string otherwise,
which is how Helm spells a boolean a caller can test with `if`.

The whole 127.0.0.0/8 range counts, not just the address `values.yaml` ships, and
the IPv6 loopback counts in both spellings: `run_operational_server` joins host
and port as `{host}:{port}`, so the bracketed `[::1]` is the form that parses as a
socket address there, while the bare `::1` reaches the same bind through the
resolver.

Usage: include "icegate.isLoopbackHost" .Values.ingest.otlpHttp.host
*/}}
{{- define "icegate.isLoopbackHost" -}}
{{- if or (hasPrefix "127." .) (eq . "localhost") (eq . "::1") (eq . "[::1]") -}}
true
{{- end -}}
{{- end }}

{{/*
Fail the render when the ingest operational listener is bound where the kubelet
cannot reach it.

That listener carries `/health`, and both probes in `values.yaml` address it with
an `httpGet` that names no `host`, which the kubelet resolves to the Pod IP. A
loopback bind therefore answers the container itself and nobody else: readiness
never passes, liveness restarts the container, and the deployment never reports a
reason beyond a failing probe. The OTLP listeners have the opposite requirement:
an operator moves them to loopback through `ingest.otlpHttp.host` /
`ingest.otlpGrpc.host` when something else publishes their ports — hence separate
hosts, and hence this check naming only the operational one, which has to stay on
the Pod IP whatever the others do.

Which addresses count as loopback is `icegate.isLoopbackHost` above, so this
check and the port declarations of `deployment-ingest.yaml` cannot disagree on
the answer.

The value is `required` rather than read straight, and through the parenthesised
path, for the reason `icegate.validateRetentionWindow` uses the same pair below:
Helm reads a `null` in an overlay as the removal of the key, so the check would
otherwise abort on `invalid value; expected string`, naming a line of this file
and none of the operator's own values. `required` rejects nil and the empty
string alike, and the empty one is worth rejecting here too — it renders a
listener config the ingest pod refuses on load, which is the same failure one
deploy later.

Usage: include "icegate.validateOperationalHost" .
*/}}
{{- define "icegate.validateOperationalHost" -}}
{{- $host := required "ingest.metrics.host is required: the operational listener binds it whatever ingest.metrics.enabled says, and the probes address that listener on the Pod IP. Bind 0.0.0.0" (.Values.ingest.metrics).host }}
{{- if include "icegate.isLoopbackHost" $host }}
{{- fail (printf "ingest.metrics.host (%s) is a loopback address: the operational listener carries /health, and both probes address it on the Pod IP, so this bind fails every readiness check and restarts the container on liveness. Bind 0.0.0.0" $host) }}
{{- end }}
{{- end }}

{{/*
The OTLP port the Service publishes, which is the port a sender addresses.

`ingest.service.otlpHttpPort` / `ingest.service.otlpGrpcPort` state it; left
unset each follows the receiver port, the same number while icegate is the
published listener itself and a different one as soon as a sidecar owns the
published ports.

Every template answering "where does a sender connect" reads it here rather than
restating the expression: `service-ingest.yaml` publishes the port and `NOTES.txt`
prints the `kubectl port-forward` command that addresses the Service by it. Built
from the receiver port instead, that command names a port that exists only inside
the pod, and it is the first text an operator of the sidecar example sees.

Usage: include "icegate.ingestPublishedPort" (dict "context" . "signal" "otlpHttp")
*/}}
{{- define "icegate.ingestPublishedPort" -}}
{{- $receiver := index .context.Values.ingest .signal -}}
{{- index .context.Values.ingest.service (printf "%sPort" .signal) | default $receiver.port -}}
{{- end }}

{{/*
Fail the render when an enabled OTLP receiver carries no bind address.

Both reasons `icegate.validateOperationalHost` states for reading its host
through `required` and the parenthesised path hold here unchanged: Helm reads a
`null` in an overlay as the removal of the key, so the value reaches
`icegate.isLoopbackHost` in `deployment-ingest.yaml` as nil and aborts the render
on a line of this file, naming none of the operator's own values; and the empty
string renders a listener config the ingest pod refuses on load, because
`{host}:{port}` is what it parses as a socket address.

Which address it is decides a second thing. The operational listener has to stay
on the Pod IP; these two are the ones an operator deliberately moves to the
loopback when a sidecar publishes their ports — and then `deployment-ingest.yaml`
declares no port for that receiver, while `service-ingest.yaml` keeps addressing
`targetPort` by the same name. So a loopback bind is accepted only alongside a
container in `ingest.extraContainers` declaring that port name: without one the
Service names a port no container in the pod declares, its EndpointSlice carries
none, and a sender is refused the connection by a pod that is Ready and logs
nothing. The check reads `ports[].name` and nothing else, so the chart still
learns nothing about what that container runs.

Each receiver is checked only while it is enabled: a disabled one binds nothing,
and its host is then a key the operator has no reason to carry.

Usage: include "icegate.validateOtlpHosts" .
*/}}
{{- define "icegate.validateOtlpHosts" -}}
{{- $portNames := dict "otlpHttp" "otlp-http" "otlpGrpc" "otlp-grpc" }}
{{- range $signal, $portName := $portNames }}
{{/* `index` on a missing key yields nil rather than aborting, and the default
     turns an explicitly null receiver map into one the field access below
     reads as absent — the same reason the parenthesised path serves
     `icegate.validateOperationalHost`. */}}
{{- $receiver := default (dict) (index $.Values.ingest $signal) }}
{{- if $receiver.enabled }}
{{- $host := required (printf "ingest.%s.host is required: the receiver binds it, and neither an absent nor an empty value is an address — the ingest pod refuses the rendered listener on load. Bind 0.0.0.0, or a loopback address when a sidecar publishes the port" $signal) $receiver.host }}
{{- if include "icegate.isLoopbackHost" $host }}
{{- $isPortDeclared := false }}
{{- range $container := default (list) $.Values.ingest.extraContainers }}
{{- range $port := default (list) $container.ports }}
{{- if eq (default "" $port.name) $portName }}
{{- $isPortDeclared = true }}
{{- end }}
{{- end }}
{{- end }}
{{- if not $isPortDeclared }}
{{- fail (printf "ingest.%s.host (%s) is a loopback address, so this container declares no %s port, and the Service addresses that name as its targetPort: no container of ingest.extraContainers declares it either, so the pod publishes nothing and a sender is refused the connection. Add the container that listens on %s with a port named %s, or bind 0.0.0.0" $signal $host $portName $portName $portName) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Fail the render when the retention window, the query provider cache, and the GC
grace period are not ordered so that a query can never plan against files that
are already gone:

  query.engine.maxAgeSecs * 1000 < migrate.snapshotExpiration.maxSnapshotAgeMs
  query.engine.maxAgeSecs        < maintain.gc.orphans.minAgeSecs

A snapshot must outlive every cached reference to it, and a file must be
unreferenced for longer than a provider can be cached before the sweep may take
it. The three values belong to three components with three config files; this
chart is the only place all of them exist at once, so it is the only place the
ordering can be checked ahead of a query failing at runtime. See
`crates/icegate-maintain/README.md`.

The first ordering is checked only while `migrate.snapshotExpiration.enabled`:
with the policy stamped off, no snapshot is ever dropped and the two magnitudes
constrain nothing. The second is checked whatever that policy says, matching
`MaintainConfig::validate` — a grace period of zero also exposes a compaction
output written but not yet referenced by a manifest, and the tables of the
deployment carry the policy they were created with, not the one in these values.

Alongside the ordering, each value is checked against the bounds its own
component's validator enforces (`SnapshotExpirationConfig::validate`,
`QueryEngineConfig::validate`). Those bounds are not the chart's to invent, but
leaving them to the pod is worse than duplicating them: `configmap-migrate.yaml`
is a `pre-install,pre-upgrade` hook, so a block the migrate pod refuses to load
fails the release after the render has already told the operator it is sound.

One rule stays out of reach: `maxAgeSecs >= refreshIntervalSecs` is checked only
when `refreshIntervalSecs` is set in values, since with it omitted the bound is
the engine's own default, resolved in code. So the guarantee is bounded — every
rule whose operands the chart holds is mirrored here, and nothing beyond that.

`query.engine.maxAgeSecs` is `required` rather than defaulted: the engine's own
default lives in `crates/icegate-query/src/engine/config.rs` and must not be
restated here, and a nil value would make `int` yield 0 and pass the orderings
below in silence. `values.yaml` declares it once, and the same value renders into
`configmap-query.yaml`. The whole `query.engine` map is optional in Helm's eyes,
hence `(.Values.query.engine).maxAgeSecs`: the parenthesised form yields nil for
a missing or explicitly null map and reaches the message below, where the plain
path aborts the render with a nil-pointer message that names none of the
operator's own values. `required` passes a zero through — it rejects nil and the
empty string only — so the positivity of the value is a check of its own.

Invoked from every `ConfigMap` that carries one of these values, so the render is
checked whichever components are enabled.

Usage: include "icegate.validateRetentionWindow" .
*/}}
{{- define "icegate.validateRetentionWindow" -}}
{{- if .Values.query.enabled }}
{{- $queryMaxAgeSecs := required "query.engine.maxAgeSecs is required: the chart checks it against migrate.snapshotExpiration.maxSnapshotAgeMs and maintain.gc.orphans.minAgeSecs, and cannot restate the engine's own default" (.Values.query.engine).maxAgeSecs | int }}
{{- if le $queryMaxAgeSecs 0 }}
{{- fail (printf "query.engine.maxAgeSecs (%d) must be greater than zero: the query pod refuses to start on a non-positive provider cache age, and every retention ordering below is stated against it" $queryMaxAgeSecs) }}
{{- end }}
{{- with (.Values.query.engine).refreshIntervalSecs }}
{{- if lt $queryMaxAgeSecs (int .) }}
{{- fail (printf "query.engine.maxAgeSecs (%d) must be at least query.engine.refreshIntervalSecs (%d): a provider would be stale before the background refresh that replaces it has run" $queryMaxAgeSecs (int .)) }}
{{- end }}
{{- end }}
{{- if and .Values.migrate.enabled .Values.migrate.snapshotExpiration.enabled }}
{{/* Both sides in milliseconds: flooring the window to whole seconds would
     reject a 30001ms window against a 30s cache, which the ordering allows. */}}
{{- $snapshotAgeMs := .Values.migrate.snapshotExpiration.maxSnapshotAgeMs | int }}
{{- if ge (mul $queryMaxAgeSecs 1000) $snapshotAgeMs }}
{{- fail (printf "query.engine.maxAgeSecs (%d) must be below migrate.snapshotExpiration.maxSnapshotAgeMs (%dms): a cached catalog provider would outlive the snapshot it planned against, and the sweep may already have collected that snapshot's files" $queryMaxAgeSecs $snapshotAgeMs) }}
{{- end }}
{{- end }}
{{- if and .Values.maintain.enabled .Values.maintain.gc.enabled .Values.maintain.gc.orphans.enabled }}
{{- if le (.Values.maintain.gc.orphans.minAgeSecs | int) $queryMaxAgeSecs }}
{{- fail (printf "maintain.gc.orphans.minAgeSecs (%d) must exceed query.engine.maxAgeSecs (%d): the sweep would be free to delete a file while a cached catalog provider still plans reads against it" (.Values.maintain.gc.orphans.minAgeSecs | int) $queryMaxAgeSecs) }}
{{- end }}
{{- end }}
{{- end }}
{{- if and .Values.maintain.enabled .Values.maintain.gc.enabled .Values.maintain.gc.orphans.enabled }}
{{- if eq (.Values.maintain.gc.orphans.minAgeSecs | int) 0 }}
{{- fail "maintain.gc.orphans.minAgeSecs must be greater than zero: a swept file has to stay unreferenced for longer than query.engine.maxAgeSecs, which is positive by definition, and the grace period is also what keeps a compaction output not yet referenced by any manifest out of the sweep's reach" }}
{{- end }}
{{- end }}
{{/* The bounds of `SnapshotExpirationConfig::validate`, which the migrate pod
     applies to the whole block whatever `enabled` says — a policy stamped off
     still has to be a well-formed one. */}}
{{- if .Values.migrate.enabled }}
{{- $minSnapshotsToKeep := .Values.migrate.snapshotExpiration.minSnapshotsToKeep | int }}
{{- $metadataPreviousVersionsMax := .Values.migrate.snapshotExpiration.metadataPreviousVersionsMax | int }}
{{- if le $minSnapshotsToKeep 0 }}
{{- fail (printf "migrate.snapshotExpiration.minSnapshotsToKeep (%d) must be greater than zero: a table that keeps no ancestor of its current snapshot has no history for an in-flight reader or the WAL offset to sit in" $minSnapshotsToKeep) }}
{{- end }}
{{- if le (.Values.migrate.snapshotExpiration.maxSnapshotAgeMs | int) 0 }}
{{- fail (printf "migrate.snapshotExpiration.maxSnapshotAgeMs (%d) must be greater than zero" (.Values.migrate.snapshotExpiration.maxSnapshotAgeMs | int)) }}
{{- end }}
{{- if lt $metadataPreviousVersionsMax $minSnapshotsToKeep }}
{{- fail (printf "migrate.snapshotExpiration.metadataPreviousVersionsMax (%d) must be at least migrate.snapshotExpiration.minSnapshotsToKeep (%d): the table would drop metadata.json versions covering snapshots it is still required to keep, and the S3 catalog resolves a lost commit ack by finding its own metadata file in the head's metadata-log" $metadataPreviousVersionsMax $minSnapshotsToKeep) }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Render AWS credential env vars from an existing Secret.
Usage: include "icegate.awsEnv" .
*/}}
{{- define "icegate.awsEnv" -}}
{{- if .Values.aws.existingSecret }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ .Values.aws.existingSecret }}
      key: aws-access-key-id
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.aws.existingSecret }}
      key: aws-secret-access-key
{{- end }}
- name: AWS_REGION
  value: {{ .Values.aws.region | quote }}
{{- end }}

{{/*
Init containers that block startup until dependencies are ready.

- `endpoints`: HTTP GET each until 2xx (RustFS/service health).
- `buckets`: authenticated `head-bucket` each until it exists. RustFS enforces
  SigV4, so bucket existence cannot be verified over anonymous HTTP; this uses
  the pod's AWS credentials against `storage.s3.endpoint`. Needed because buckets
  are created out-of-band by the `rustfs-init` Job, which finishes after RustFS
  reports healthy.

Usage: include "icegate.waitForDeps" (dict "context" . "config" .Values.ingest.waitForDependencies)
*/}}
{{- define "icegate.waitForDeps" -}}
{{- if .config.enabled }}
{{- if or .config.endpoints .config.buckets }}
initContainers:
  {{- if .config.endpoints }}
  - name: wait-for-deps
    image: {{ .config.image }}
    securityContext:
      {{- include "icegate.containerSecurityContext" .context | nindent 6 }}
    command:
      - sh
      - -c
      - |
        {{- range .config.endpoints }}
        echo "Waiting for {{ . }} ..."
        until wget -qO- -T 2 "{{ . }}" >/dev/null 2>&1; do
          sleep 3
        done
        echo "{{ . }} is ready"
        {{- end }}
  {{- end }}
  {{- if .config.buckets }}
  - name: wait-for-buckets
    image: {{ .config.awsCliImage | default "amazon/aws-cli:2.36.34" }}
    securityContext:
      # readOnlyRootFilesystem is relaxed (unlike the strict container context)
      # so the AWS CLI can use its writable scratch space; still non-root, no
      # privilege escalation, all capabilities dropped.
      allowPrivilegeEscalation: false
      runAsNonRoot: true
      readOnlyRootFilesystem: false
      capabilities:
        drop:
          - ALL
    env:
      {{- include "icegate.awsEnv" .context | nindent 6 }}
    command:
      - /bin/sh
      - -c
      - |
        E={{ .context.Values.storage.s3.endpoint | quote }}
        {{- range .config.buckets }}
        echo "Waiting for bucket {{ . }} ..."
        until aws --endpoint-url "$E" s3api head-bucket --bucket {{ . }} >/dev/null 2>&1; do
          sleep 3
        done
        echo "bucket {{ . }} exists"
        {{- end }}
  {{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Pod security context (shared across all workloads).
*/}}
{{- define "icegate.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 65534
runAsGroup: 65534
fsGroup: 65534
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{/*
Container security context (shared across all containers).
*/}}
{{- define "icegate.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop:
    - ALL
{{- end }}
