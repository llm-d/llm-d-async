{{/*
Expand the name of the chart.
*/}}
{{- define "llm-d-async.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "llm-d-async.fullname" -}}
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
{{- define "llm-d-async.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "llm-d-async.labels" -}}
helm.sh/chart: {{ include "llm-d-async.chart" . }}
{{ include "llm-d-async.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "llm-d-async.selectorLabels" -}}
app.kubernetes.io/name: {{ include "llm-d-async.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "llm-d-async.serviceAccountName" -}}
{{- default (include "llm-d-async.fullname" .) .Values.serviceAccount.name }}
{{- end }}

{{/*
Effective transport type. Prefers the new ap.transport; otherwise derives it from
the deprecated ap.redis.enabled / ap.gcpPubSub.enabled / ap.messageQueueImpl values.
Renders empty when no backend is selected.
*/}}
{{- define "llm-d-async.transport" -}}
{{- if .Values.ap.transport -}}
{{- .Values.ap.transport -}}
{{- else if .Values.ap.redis.enabled -}}
{{- if eq (.Values.ap.messageQueueImpl | default "redis-pubsub") "redis-sortedset" -}}
redis-sortedset
{{- else -}}
redis-pubsub
{{- end -}}
{{- else if .Values.ap.gcpPubSub.enabled -}}
gcp-pubsub
{{- end -}}
{{- end }}

{{/*
Transport config JSON document passed via --transport-config. On the new surface
it is ap.transportConfig verbatim; otherwise it is synthesized from the deprecated
per-backend values so existing values files keep working.
*/}}
{{- define "llm-d-async.transportConfig" -}}
{{- if .Values.ap.transport -}}
{{- /* urlSecret is a chart-only directive (it wires REDIS_URL from a Secret);
       strip it so it is never passed to the processor or rendered into args. */ -}}
{{- omit .Values.ap.transportConfig "urlSecret" | toJson -}}
{{- else -}}
{{- include "llm-d-async.legacyTransportConfig" . -}}
{{- end -}}
{{- end }}

{{/*
Synthesize a transport config JSON document from the deprecated per-backend chart
values (ap.redis.* / ap.gcpPubSub.* / ap.otel.redisTracing). The Redis "url" is
deliberately left unset so the injected REDIS_URL env var supplies it.
*/}}
{{- define "llm-d-async.legacyTransportConfig" -}}
{{- $ap := .Values.ap -}}
{{- $transport := include "llm-d-async.transport" . -}}
{{- $cfg := dict -}}
{{- if eq $transport "redis-pubsub" -}}
{{- if and $ap.otel.redisTracing $ap.otel.endpoint -}}{{- $_ := set $cfg "enable_tracing" true -}}{{- end -}}
{{- if $ap.redis.queuesConfig -}}
{{- $_ := set $cfg "queues" $ap.redis.queuesConfig -}}
{{- else -}}
{{- $_ := set $cfg "queues" (list (dict "queue_name" "request-queue" "igw_base_url" $ap.igwBaseURL "request_path_url" $ap.redis.requestPathURL)) -}}
{{- end -}}
{{- else if eq $transport "redis-sortedset" -}}
{{- $_ := set $cfg "result_queue_name" ($ap.redis.resultQueueName | default "result-list") -}}
{{- $_ := set $cfg "poll_interval_ms" (int ($ap.redis.pollIntervalMs | default 1000)) -}}
{{- $_ := set $cfg "batch_size" (int ($ap.redis.batchSize | default 10)) -}}
{{- if and $ap.otel.redisTracing $ap.otel.endpoint -}}{{- $_ := set $cfg "enable_tracing" true -}}{{- end -}}
{{- if $ap.redis.queuesConfig -}}
{{- $_ := set $cfg "queues" $ap.redis.queuesConfig -}}
{{- else -}}
{{- $q := dict "queue_name" ($ap.redis.requestQueueName | default "request-sortedset") "igw_base_url" $ap.igwBaseURL "request_path_url" $ap.redis.requestPathURL -}}
{{- if $ap.redis.gateType -}}
{{- $_ := set $q "gate_type" $ap.redis.gateType -}}
{{- $gp := dict -}}
{{- range $k, $v := $ap.redis.gateParams -}}{{- $_ := set $gp $k ($v | toString) -}}{{- end -}}
{{- $_ := set $q "gate_params" $gp -}}
{{- end -}}
{{- $_ := set $cfg "queues" (list $q) -}}
{{- end -}}
{{- else if eq $transport "gcp-pubsub" -}}
{{- $_ := set $cfg "project_id" $ap.gcpPubSub.projectId -}}
{{- $_ := set $cfg "result_topic_id" $ap.gcpPubSub.resultTopicId -}}
{{- if $ap.gcpPubSub.topicsConfig -}}
{{- $_ := set $cfg "topics" $ap.gcpPubSub.topicsConfig -}}
{{- else -}}
{{- $_ := set $cfg "topics" (list (dict "subscriber_id" $ap.gcpPubSub.requestSubscriberId "igw_base_url" $ap.igwBaseURL "request_path_url" $ap.gcpPubSub.requestPathURL)) -}}
{{- end -}}
{{- end -}}
{{- $cfg | toJson -}}
{{- end }}

{{/*
Report whether any deprecated per-backend transport value is in use. Drives the
migration warning in NOTES.txt.
*/}}
{{- define "llm-d-async.usingDeprecatedTransport" -}}
{{- if and (not .Values.ap.transport) (or .Values.ap.redis.enabled .Values.ap.gcpPubSub.enabled) -}}
true
{{- end -}}
{{- end }}

{{/*
Report whether the deprecated ap.redis.* connection inputs are in use. On the new
surface the Redis connection belongs in ap.transportConfig.urlSecret; ap.redis.url
and ap.redis.secretName are retained for backwards compatibility only. Only warn
when the effective transport is Redis and the connection actually comes from
ap.redis.* (i.e. the new urlSecret surface is not configured), so the migration
notice never fires for a non-Redis backend or when urlSecret already supersedes it.
*/}}
{{- define "llm-d-async.usingDeprecatedRedisConn" -}}
{{- $transport := include "llm-d-async.transport" . -}}
{{- $ts := .Values.ap.transportConfig | default dict -}}
{{- $hasUrlSecret := or (dig "urlSecret" "url" "" $ts) (dig "urlSecret" "name" "" $ts) -}}
{{- if and (hasPrefix "redis" $transport) (not $hasUrlSecret) (or .Values.ap.redis.url .Values.ap.redis.secretName) -}}
true
{{- end -}}
{{- end }}

{{/*
Resolve the Redis secret name.
If redis.url is set, the chart creates a Secret named <fullname>-redis.
Otherwise, use the user-provided redis.secretName.
*/}}
{{- define "llm-d-async.redisSecretName" -}}
{{- $ts := .Values.ap.transportConfig | default dict -}}
{{- if and .Values.ap.transport (dig "urlSecret" "url" "" $ts) -}}
{{- printf "%s-redis" (include "llm-d-async.fullname" .) -}}
{{- else if and .Values.ap.transport (dig "urlSecret" "name" "" $ts) -}}
{{- dig "urlSecret" "name" "" $ts -}}
{{- else if .Values.ap.redis.url -}}
{{- printf "%s-redis" (include "llm-d-async.fullname" .) -}}
{{- else -}}
{{- .Values.ap.redis.secretName -}}
{{- end -}}
{{- end }}

{{/*
Resolve the Redis secret key.
When the chart creates the Secret, the key is always "url".
*/}}
{{- define "llm-d-async.redisSecretKey" -}}
{{- $ts := .Values.ap.transportConfig | default dict -}}
{{- if and .Values.ap.transport (dig "urlSecret" "url" "" $ts) -}}
url
{{- else if and .Values.ap.transport (dig "urlSecret" "name" "" $ts) -}}
{{- dig "urlSecret" "key" "url" $ts | default "url" -}}
{{- else if .Values.ap.redis.url -}}
url
{{- else -}}
{{- .Values.ap.redis.secretKey -}}
{{- end -}}
{{- end }}
