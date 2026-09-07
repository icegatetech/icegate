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
Render the auth proxy's HTTP filter chain (zero-indented). Both listeners use the
same one, so it is defined once here.

Four filters, and the order is the contract:
  1. header_mutation drops whatever the client sent as x-scope-orgid. It has to
     be a filter, and it has to be this one: route-level `request_headers_to_remove`
     is applied by the router filter, which runs last, so it would delete the value
     jwt_authn wrote rather than the one the client sent, and every request would
     reach icegate with no tenant at all.
  2. jwt_authn verifies the ingest token and copies its `tenant_id` claim into
     the now-empty x-scope-orgid header the ingest handlers read. `forward: false`
     keeps the token itself from reaching icegate.
  3. rbac checks both halves of the token profile, and a request needs both.
     The JOSE `typ` of the header says the object is an access token rather
     than any other JOSE object the same key signs; the payload `typ` is the
     issuer's own discriminator, which says *which* application token this is.
     The audience replaces neither: it separates the ingest token from the API
     token, not an ingest token from any other token the same issuer signs for
     the same audience. Both markers are pinned by scripts/authproxy-test.sh.
  4. router proxies what survived.

Usage: include "icegate.authProxyHttpFilters" .
*/}}
{{- define "icegate.authProxyHttpFilters" -}}
- name: envoy.filters.http.header_mutation
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.header_mutation.v3.HeaderMutation
    mutations:
      request_mutations:
        - remove: x-scope-orgid
- name: envoy.filters.http.jwt_authn
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.jwt_authn.v3.JwtAuthentication
    providers:
      ingest_token:
        issuer: {{ .Values.ingest.authProxy.jwt.issuer | quote }}
        audiences:
          - {{ .Values.ingest.authProxy.jwt.audience | quote }}
        forward: false
        # Both halves of the token reach rbac below, which is the only filter
        # that reads either: without these keys the metadata carries nothing and
        # a principal stated over it admits every token jwt_authn verified.
        header_in_metadata: jwt_header
        payload_in_metadata: jwt_payload
        clock_skew_seconds: {{ .Values.ingest.authProxy.jwt.clockSkewSeconds }}
        # The only place a token is taken from. With no extractor named,
        # jwt_authn also accepts an `access_token` query parameter, and a URL is
        # written to access logs and traces by everything it passes through.
        from_headers:
          - name: Authorization
            value_prefix: "Bearer "
        claim_to_headers:
          - header_name: x-scope-orgid
            claim_name: tenant_id
        remote_jwks:
          http_uri:
            uri: {{ .Values.ingest.authProxy.jwt.jwksUri | quote }}
            cluster: token_issuer_jwks
            timeout: 5s
          cache_duration: {{ printf "%ds" (int .Values.ingest.authProxy.jwt.jwksCacheDurationSecs) | quote }}
          # Fetched in the background so the proxy starts even while the issuer
          # is unreachable.
          async_fetch: {}
    rules:
      - match: { prefix: / }
        requires: { provider_name: ingest_token }
- name: envoy.filters.http.rbac
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.rbac.v3.RBAC
    rules:
      action: ALLOW
      policies:
        # `principals.metadata` is marked deprecated by Envoy 1.36 (it still
        # loads, with a warning). The replacement is a CEL `condition` over the
        # same jwt_authn metadata; swap it when this image stops accepting the
        # field, not before — the CEL form is longer and harder to read.
        ingest_tokens_only:
          permissions:
            - any: true
          principals:
            - and_ids:
                ids:
                  - metadata:
                      filter: envoy.filters.http.jwt_authn
                      path:
                        - key: jwt_header
                        - key: typ
                      value:
                        string_match: { exact: {{ .Values.ingest.authProxy.jwt.headerType | quote }} }
                  - metadata:
                      filter: envoy.filters.http.jwt_authn
                      path:
                        - key: jwt_payload
                        - key: typ
                      value:
                        string_match: { exact: {{ .Values.ingest.authProxy.jwt.tokenType | quote }} }
- name: envoy.filters.http.router
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
{{- end }}

{{/*
Fail the render when the auth proxy is enabled without the settings it cannot
work around. Every one of them names something outside this chart — the token
issuer, its JWKS endpoint, the audience, and the Secret holding the server
certificate — so there is nothing to default to, and a proxy started without them
either rejects every request or, worse, accepts tokens it should not.

The tenant policy is checked alongside them. The proxy writes x-scope-orgid from
the token's `tenant_id` claim, and a `single` policy accepts that header only
when it names the configured id, so a deployment left on the chart's default id
refuses every batch the issuer signs for anything else — with nothing in the
render to say so. `single` behind the proxy is not forbidden outright: a
single-customer deployment whose issuer puts exactly that id in the token is
legitimate, and it passes as soon as the id is the operator's own rather than the
value shipped in values.yaml.

`ingest.ingress` is refused outright, and for the same reason the list above
exists: the pairing renders cleanly and then fails every request. The Ingress
addresses the Service ports, those ports reach the proxy's listeners, and the
listeners speak TLS — while what makes a controller reach a TLS backend lives in
the controller's own configuration, outside this chart's reach.

Usage: include "icegate.validateAuthProxy" .
*/}}
{{- define "icegate.validateAuthProxy" -}}
{{- if and .Values.ingest.enabled .Values.ingest.authProxy.enabled }}
{{- if not .Values.ingest.authProxy.jwt.issuer }}{{ fail "ingest.authProxy.jwt.issuer is required when ingest.authProxy.enabled" }}{{ end }}
{{- if not .Values.ingest.authProxy.jwt.audience }}{{ fail "ingest.authProxy.jwt.audience is required when ingest.authProxy.enabled" }}{{ end }}
{{- if not .Values.ingest.authProxy.jwt.jwksUri }}{{ fail "ingest.authProxy.jwt.jwksUri is required when ingest.authProxy.enabled" }}{{ end }}
{{- if not .Values.ingest.authProxy.tls.secretName }}{{ fail "ingest.authProxy.tls.secretName is required when ingest.authProxy.enabled" }}{{ end }}
{{- if .Values.ingest.ingress.enabled }}
{{- fail "ingest.ingress.enabled together with ingest.authProxy.enabled: the Service ports the Ingress addresses are the proxy's, and the proxy terminates TLS on them (the OTLP/gRPC one negotiates h2 through ALPN). What an Ingress controller has to be told to reach a TLS backend is the controller's own setting, which this chart neither renders nor can check, so the pairing is refused here instead of creating an Ingress whose every request fails the handshake. Publish the proxy through the Service (its ports already carry TLS), or turn ingest.authProxy.enabled off" }}
{{- end }}
{{- if and (eq .Values.ingest.tenant.mode "single") (eq .Values.ingest.tenant.id "default") }}
{{- fail "ingest.tenant is still the chart default (mode single, id \"default\") while ingest.authProxy.enabled: the proxy writes x-scope-orgid from the token's tenant_id claim, and a single policy on the default id refuses every batch the issuer signs for any other tenant. Set ingest.tenant.mode=multi, or ingest.tenant.id to the one id this deployment's issuer emits" }}
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
