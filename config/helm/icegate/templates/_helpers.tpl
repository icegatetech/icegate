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
Image reference for a component or an init container.

`image` is a map of `repository`, `tag` and `digest`. An empty `tag` falls back to
the `tag` argument, which the helper images leave unset because they have no
chart-wide tag to follow. A set `digest` is appended, giving `repository:tag@digest`;
the tag stays in the reference so the rendered manifest still says which release it is.
`key` is the values path of `image`, used only to name it in a failure.

Usage: include "icegate.image" (dict "image" .Values.query.image "tag" .Chart.AppVersion "key" "query.image")
*/}}
{{- define "icegate.image" -}}
{{- if kindIs "string" .image }}
{{- fail (printf "%s must be a map with repository, tag and digest" .key) }}
{{- end }}
{{- $tag := .image.tag | default .tag }}
{{- if not $tag }}
{{- fail (printf "%s.tag must be set" .key) }}
{{- end }}
{{- $reference := printf "%s:%s" .image.repository $tag }}
{{- with .image.digest }}
{{- $digest := toString . }}
{{- if not (regexMatch "^sha256:[a-f0-9]{64}$" $digest) }}
{{- fail (printf "%s.digest (%s) must be sha256: followed by 64 lowercase hex characters" $.key $digest) }}
{{- end }}
{{- $reference = printf "%s@%s" $reference $digest }}
{{- end }}
{{- $reference }}
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
Render the `endpoint` line of a job-storage block (zero-indented), or nothing.

The block's own `endpoint` wins over `storage.s3.endpoint`. When both are empty the key
is left out rather than rendered as `endpoint: ""`: an absent endpoint is how the
services are told to use the regional AWS endpoint.

Call it as `with include ...` and print `.` under it, so that the empty result adds no
line.

Usage: include "icegate.jobsStorageEndpointYaml" (dict "context" . "storage" .Values.ingest.shift.jobsmanager.storage)
*/}}
{{- define "icegate.jobsStorageEndpointYaml" -}}
{{- with .storage.endpoint | default .context.Values.storage.s3.endpoint -}}
endpoint: {{ . | quote }}
{{- end }}
{{- end }}

{{/*
Render a component's tenant policy (zero-indented).
Produces the YAML tagged union: `!single { id }` or `!multi`.
Callers must use `nindent N` to place at the correct indentation level.

`fail` rather than a default: a `single` policy with no id would deserialize into
an empty tenant that no request can name, and the pod would reject every request
— a batch on the write side, a query on the read side.

Usage: include "icegate.tenantYaml" (dict "context" . "component" "query")
*/}}
{{- define "icegate.tenantYaml" -}}
{{- $tenant := (index .context.Values .component).tenant -}}
{{- if eq $tenant.mode "single" -}}
{{- if not $tenant.id }}{{ fail (printf "%s.tenant.id is required when %s.tenant.mode is single" .component .component) }}{{ end -}}
tenant: !single
  id: {{ $tenant.id | quote }}
{{- else if eq $tenant.mode "multi" -}}
tenant: !multi
{{- else -}}
{{ fail (printf "%s.tenant.mode must be single or multi, got %q" .component $tenant.mode) }}
{{- end -}}
{{- end }}

{{/*
Answer whether a listener host is reachable only from inside the container.

Returns a non-empty string for a loopback address and the empty string otherwise,
which is how Helm spells a boolean a caller can test with `if`.

The whole 127.0.0.0/8 range counts, not just the address `values.yaml` ships, and
the IPv6 loopback counts in both spellings: `run_operational_server` accepts an
IPv6 host bare (`::1`) or in brackets (`[::1]`) and binds the same address for
either.

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
The `appProtocol` line of one Service port, or nothing.

`service` is the component's `service` map and `port` a key of its `appProtocol`
map. A key left unset or empty renders nothing: the value names the protocol the
port speaks, the chart picks none, and a render that sets no key is the render
from before the keys existed.

The map is read through `default (dict)` and the parenthesised path, for the
reason `icegate.validateOperationalHost` reads its host that way: Helm reads a
`null` in an overlay as the removal of the key, and `index` refuses the nil that
leaves behind. The value is quoted so that one YAML would read as another type,
`true` or `5`, still reaches the manifest as the string the field is.

Call it as `with include ...` and print `.` under it, so that the empty result
adds no line; `nindent` would add a blank one to every render that sets nothing.

Usage: include "icegate.appProtocol" (dict "service" .Values.query.service "port" "loki")
*/}}
{{- define "icegate.appProtocol" -}}
{{- with index (default (dict) (.service).appProtocol) .port -}}
appProtocol: {{ . | quote }}
{{- end }}
{{- end }}

{{/*
Fail the render when an enabled OTLP receiver carries no bind address.

Both reasons `icegate.validateOperationalHost` states for reading its host
through `required` and the parenthesised path hold here unchanged: Helm reads a
`null` in an overlay as the removal of the key, so the value reaches
`icegate.isLoopbackHost` in `deployment-ingest.yaml` as nil and aborts the render
on a line of this file, naming none of the operator's own values; and the empty
string renders a listener config the ingest pod refuses on load, because the
receiver binds its host as an IP address or a hostname (an IPv6 address bare or
in brackets) together with its port, and an empty host is neither.

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
Fail the render when an entry added to the ingest pod breaks the restricted Pod
Security level, while `podSecurity.checkRestricted` asks for it.

The containers and volumes this chart declares meet the level, which
`scripts/helm-render-test.py` proves for every pod it renders. `ingest.extraContainers`
and `ingest.extraVolumes` are passed through as written, so nothing else holds them to
it. The check is off unless asked for, because it would refuse a sidecar an operator
runs where the level is not enforced.

The rules are those of `restricted_violations` in `scripts/chartlib.py`. A container
needs `allowPrivilegeEscalation` false, `capabilities.drop` holding ALL,
`capabilities.add` empty or NET_BIND_SERVICE alone, `privileged` not true, no
`hostPort`, `runAsNonRoot` true, a `runAsUser` other than 0 and a `seccompProfile.type`
of RuntimeDefault or Localhost. The last three are read as the container states them,
and as `icegate.podSecurityContext` states them for the pod when it states none, so a
sidecar that leaves them out is judged by the pod's: the auth proxy example relies on
that. A volume needs one of the types listed below.

Every violation of every entry is named in one failure, so that an operator fixes the
entry once rather than once per rule. `kindIs "invalid"` and `toString` read a setting
because `default` and plain truthiness treat an explicit `false` or `0` as unset, which
is the value a violation consists of.

Usage: include "icegate.validateRestrictedExtensions" .
*/}}
{{- define "icegate.validateRestrictedExtensions" -}}
{{- if (.Values.podSecurity).checkRestricted }}
{{- $podContext := include "icegate.podSecurityContext" . | fromYaml }}
{{- $volumeTypes := list "configMap" "csi" "downwardAPI" "emptyDir" "ephemeral" "persistentVolumeClaim" "projected" "secret" }}
{{- $violations := list }}
{{- range $container := default (list) .Values.ingest.extraContainers }}
{{- $entry := printf "ingest.extraContainers[%s]" $container.name }}
{{- $context := default (dict) $container.securityContext }}
{{- $capabilities := default (dict) $context.capabilities }}
{{- if ne (toString $context.allowPrivilegeEscalation) "false" }}
{{- $violations = append $violations (printf "%s: allowPrivilegeEscalation (%s) breaks the restricted Pod Security level; set securityContext.allowPrivilegeEscalation to false" $entry (ternary "unset" (toString $context.allowPrivilegeEscalation) (kindIs "invalid" $context.allowPrivilegeEscalation))) }}
{{- end }}
{{- if not (has "ALL" (default (list) $capabilities.drop)) }}
{{- $violations = append $violations (printf "%s: capabilities.drop without ALL breaks the restricted Pod Security level; set securityContext.capabilities.drop to [ALL]" $entry) }}
{{- end }}
{{- range $capability := default (list) $capabilities.add }}
{{- if ne (toString $capability) "NET_BIND_SERVICE" }}
{{- $violations = append $violations (printf "%s: capabilities.add of %s breaks the restricted Pod Security level; remove it, only NET_BIND_SERVICE may be added" $entry $capability) }}
{{- end }}
{{- end }}
{{- if eq (toString $context.privileged) "true" }}
{{- $violations = append $violations (printf "%s: privileged true breaks the restricted Pod Security level; remove securityContext.privileged" $entry) }}
{{- end }}
{{- range $port := default (list) $container.ports }}
{{- if $port.hostPort }}
{{- $violations = append $violations (printf "%s: hostPort %v breaks the restricted Pod Security level; remove hostPort from the port and publish it through the Service" $entry $port.hostPort) }}
{{- end }}
{{- end }}
{{- $runAsNonRoot := $podContext.runAsNonRoot }}
{{- if not (kindIs "invalid" $context.runAsNonRoot) }}
{{- $runAsNonRoot = $context.runAsNonRoot }}
{{- end }}
{{- if ne (toString $runAsNonRoot) "true" }}
{{- $violations = append $violations (printf "%s: runAsNonRoot %s breaks the restricted Pod Security level; set securityContext.runAsNonRoot to true or remove it to follow the pod" $entry (toString $runAsNonRoot)) }}
{{- end }}
{{- $runAsUser := $podContext.runAsUser }}
{{- if not (kindIs "invalid" $context.runAsUser) }}
{{- $runAsUser = $context.runAsUser }}
{{- end }}
{{- if eq (toString $runAsUser) "0" }}
{{- $violations = append $violations (printf "%s: runAsUser 0 breaks the restricted Pod Security level; set securityContext.runAsUser to a non-zero id or remove it to follow the pod" $entry) }}
{{- end }}
{{- $seccompProfile := default $podContext.seccompProfile $context.seccompProfile }}
{{- if not (has (toString $seccompProfile.type) (list "RuntimeDefault" "Localhost")) }}
{{- $violations = append $violations (printf "%s: seccompProfile.type %s breaks the restricted Pod Security level; set securityContext.seccompProfile.type to RuntimeDefault or Localhost, or remove it to follow the pod" $entry (toString $seccompProfile.type)) }}
{{- end }}
{{- end }}
{{- range $volume := default (list) .Values.ingest.extraVolumes }}
{{- range $volumeType, $_ := omit $volume "name" }}
{{- if not (has $volumeType $volumeTypes) }}
{{- $violations = append $violations (printf "%s: volume type %s breaks the restricted Pod Security level; use one of %s" (printf "ingest.extraVolumes[%s]" $volume.name) $volumeType (join ", " $volumeTypes)) }}
{{- end }}
{{- end }}
{{- end }}
{{- if $violations }}
{{- fail (join "\n" $violations) }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Fail the render when an `ingest.extraVolumes` entry takes the name of a volume the chart
itself renders in `deployment-ingest.yaml`. Pod volume names must be unique, so the
collision would otherwise surface only when the API server refuses the Deployment.

Only the names the chart renders for these values count: `cache` and the TLS volume
are reserved only while `ingest.cache.enabled` / `ingest.tls.enabled` add them. The
`config` and `tmp` names here must match the ones `deployment-ingest.yaml` declares.

Usage: include "icegate.validateExtraVolumeNames" .
*/}}
{{- define "icegate.validateExtraVolumeNames" -}}
{{- $chartVolumeNames := list "config" "tmp" }}
{{- if (.Values.ingest.cache).enabled }}
{{- $chartVolumeNames = append $chartVolumeNames "cache" }}
{{- end }}
{{- if (.Values.ingest.tls).enabled }}
{{- $chartVolumeNames = append $chartVolumeNames (include "icegate.tlsVolumeName" .) }}
{{- end }}
{{- range $volume := default (list) .Values.ingest.extraVolumes }}
{{- if has (toString $volume.name) $chartVolumeNames }}
{{- fail (printf "ingest.extraVolumes[%s]: the chart already renders a pod volume named %s; give this volume another name" $volume.name $volume.name) }}
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

Both init-container images go through `icegate.image`, so `component` (the values
section the config comes from, e.g. "ingest") names them in a failure.

Usage: include "icegate.waitForDeps" (dict "context" . "config" .Values.ingest.waitForDependencies "component" "ingest")
*/}}
{{- define "icegate.waitForDeps" -}}
{{- if .config.enabled }}
{{- if or .config.endpoints .config.buckets }}
initContainers:
  {{- if .config.endpoints }}
  - name: wait-for-deps
    image: {{ include "icegate.image" (dict "image" .config.image "key" (printf "%s.waitForDependencies.image" .component)) }}
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
    image: {{ include "icegate.image" (dict "image" .config.awsCliImage "key" (printf "%s.waitForDependencies.awsCliImage" .component)) }}
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

{{/*
The directory a component's TLS Secret is mounted at, and so the directory its
rendered config names the certificate and the key under.

Stated once because the config (`icegate.tlsYaml`) and the container mount
(`icegate.tlsVolumeMount`) must agree on it: a mount somewhere else leaves the
component naming files that are not there, and it refuses to start.

Usage: include "icegate.tlsMountPath" .
*/}}
{{- define "icegate.tlsMountPath" -}}
/etc/icegate-tls
{{- end }}

{{/*
Fail the render when a component serves TLS without what the render needs to wire it.

`secretName` is the Secret the pod mounts: without it the volume would name no Secret.
The CA ConfigMap is asked for where a
ServiceMonitor is rendered for the component (`<component>.metrics.enabled` with
`serviceMonitor.enabled`, the condition of `servicemonitor-*.yaml`), because that is
where it is used: the scrape names it as its CA. A component with its metrics endpoint
off renders no scrape, so it needs none.

Both values are read through the parenthesised path, for the reason
`icegate.validateOperationalHost` reads its host that way: Helm reads a `null` in an
overlay as the removal of the key, so a plain path would abort the render on a line of
this file instead of reaching the messages below.

Invoked from the ConfigMap of each component, which renders whenever the component does.

Usage: include "icegate.validateTls" (dict "context" . "component" "ingest")
*/}}
{{- define "icegate.validateTls" -}}
{{- $component := index .context.Values .component }}
{{- if ($component.tls).enabled }}
{{- if not $component.tls.secretName }}
{{- fail (printf "%s.tls.secretName is required when %s.tls.enabled is true: name a Secret holding tls.crt and tls.key, or turn %s.tls.enabled off" .component .component .component) }}
{{- end }}
{{- if and ($component.metrics).enabled (.context.Values.serviceMonitor).enabled (not (.context.Values.tls).caConfigMapName) }}
{{- fail (printf "tls.caConfigMapName is required when %s.tls.enabled and serviceMonitor.enabled are true: the ServiceMonitor names ca.crt of that ConfigMap as the CA of the scrape. Create a ConfigMap holding the CA certificate under ca.crt and set tls.caConfigMapName to its name, or turn serviceMonitor.enabled off" .component) }}
{{- end }}
{{- end }}
{{- end }}

{{/*
A component's `tls` config section (zero-indented), or nothing while TLS is off.

The section names the certificate and the key under `icegate.tlsMountPath`, the two
files of the Secret. `reload_interval_secs` is rendered only when the operator sets
`<component>.tls.reloadIntervalSecs`: its default is the component's own, in code, and
a copy here would be a second one. The component's upper bound is in code as well, so a
value above it is rendered and refused when the pod loads its config, rather than judged
here by a second copy of the limit. The number goes through `int64` because a values file
hands it over as a float, which prints in exponent form from seven digits up.

Call it as `with include ...` and print `.` under it, so that the empty result adds no
line.

Usage: include "icegate.tlsYaml" (dict "context" . "component" "ingest")
*/}}
{{- define "icegate.tlsYaml" -}}
{{- $tls := default (dict) (index .context.Values .component).tls }}
{{- if $tls.enabled -}}
tls:
  certificate_path: {{ include "icegate.tlsMountPath" .context }}/tls.crt
  private_key_path: {{ include "icegate.tlsMountPath" .context }}/tls.key
  {{- if not (kindIs "invalid" $tls.reloadIntervalSecs) }}
  reload_interval_secs: {{ $tls.reloadIntervalSecs | int64 }}
  {{- end }}
{{- end }}
{{- end }}

{{/*
The name of the pod volume carrying a component's TLS Secret, shared by the volume,
its mount and `icegate.validateExtraVolumeNames`.
*/}}
{{- define "icegate.tlsVolumeName" -}}
tls
{{- end }}

{{/*
The pod volume carrying a component's TLS Secret, mounted by `icegate.tlsVolumeMount`.

Usage: include "icegate.tlsVolume" (dict "context" . "component" "ingest")
*/}}
{{- define "icegate.tlsVolume" -}}
- name: {{ include "icegate.tlsVolumeName" . }}
  secret:
    secretName: {{ (index .context.Values .component).tls.secretName | quote }}
{{- end }}

{{/*
The container mount of the volume `icegate.tlsVolume` declares.

A directory mount, never a `subPath`: the component re-reads both files in place to pick
up a renewal, so the whole Secret volume is mounted rather than single files from it.

Usage: include "icegate.tlsVolumeMount" .
*/}}
{{- define "icegate.tlsVolumeMount" -}}
- name: {{ include "icegate.tlsVolumeName" . }}
  mountPath: {{ include "icegate.tlsMountPath" . }}
  readOnly: true
{{- end }}

{{/*
The `endpoints` fields of a ServiceMonitor that scrape a component over TLS.

`serverName` is the Service's DNS name, the name the serving certificate is expected to
carry. The CA ConfigMap name is read after `icegate.validateTls`, called here as well as
from the ConfigMap: Helm renders templates in an order a template cannot rely on, and the
refusal has to come from the guard, not from a nil read of an unset name.

Usage: include "icegate.serviceMonitorTls" (dict "context" . "component" "ingest")
*/}}
{{- define "icegate.serviceMonitorTls" -}}
{{- include "icegate.validateTls" . -}}
scheme: https
tlsConfig:
  ca:
    configMap:
      name: {{ .context.Values.tls.caConfigMapName | quote }}
      key: ca.crt
  serverName: {{ include "icegate.componentName" (dict "context" .context "component" .component) }}.{{ .context.Release.Namespace }}.svc
{{- end }}

{{/*
A probe as the Deployment renders it: the probe as written, plus `scheme: HTTPS` on its
`httpGet` while the component serves TLS.

The probes address the operational listener, which serves TLS together with the rest of
the component, so an `httpGet` that states no scheme gets the HTTPS one. A scheme the
operator states is kept, whichever it is, and a probe that is not an `httpGet` has no
scheme to set, so both render unchanged. The probe is copied before it
is touched: `.Values` is shared with every other template.

Call it under `with` on the probe, so that an empty probe adds no field.

Usage: include "icegate.probe" (dict "probe" .Values.ingest.livenessProbe "isTlsEnabled" (.Values.ingest.tls).enabled)
*/}}
{{- define "icegate.probe" -}}
{{- $probe := deepCopy .probe }}
{{- if and .isTlsEnabled (kindIs "map" $probe.httpGet) (not (hasKey $probe.httpGet "scheme")) }}
{{- $_ := set $probe.httpGet "scheme" "HTTPS" }}
{{- end }}
{{- toYaml $probe }}
{{- end }}
