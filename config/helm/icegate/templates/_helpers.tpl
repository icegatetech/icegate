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
  uri: {{ .Values.catalog.rest.uri }}
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
    image: {{ .config.awsCliImage | default "amazon/aws-cli:2.35.15" }}
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
