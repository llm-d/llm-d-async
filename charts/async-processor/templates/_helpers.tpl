{{/*
Expand the name of the chart.
*/}}
{{- define "async-processor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "async-processor.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
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
{{- define "async-processor.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "async-processor.labels" -}}
helm.sh/chart: {{ include "async-processor.chart" . }}
{{ include "async-processor.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "async-processor.selectorLabels" -}}
app.kubernetes.io/name: {{ include "async-processor.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "async-processor.serviceAccountName" -}}
{{- default (include "async-processor.fullname" .) .Values.serviceAccount.name }}
{{- end }}

{{/*
Render gate params as JSON with all values as strings.
The gate params parser expects map[string]string, so numeric values must be quoted.
*/}}
{{- define "async-processor.gateParamsJson" -}}
{{- $out := dict -}}
{{- range $k, $v := .Values.ap.redis.gateParams -}}
{{- $_ := set $out $k ($v | toString) -}}
{{- end -}}
{{- $out | toJson -}}
{{- end }}

{{/*
Resolve the Redis secret name.
If redis.url is set, the chart creates a Secret named <fullname>-redis.
Otherwise, use the user-provided redis.secretName.
*/}}
{{- define "async-processor.redisSecretName" -}}
{{- if .Values.ap.redis.url -}}
{{- printf "%s-redis" (include "async-processor.fullname" .) -}}
{{- else -}}
{{- .Values.ap.redis.secretName -}}
{{- end -}}
{{- end }}

{{/*
Resolve the Redis secret key.
When the chart creates the Secret, the key is always "url".
*/}}
{{- define "async-processor.redisSecretKey" -}}
{{- if .Values.ap.redis.url -}}
url
{{- else -}}
{{- .Values.ap.redis.secretKey -}}
{{- end -}}
{{- end }}