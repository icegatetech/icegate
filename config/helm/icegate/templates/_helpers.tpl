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
