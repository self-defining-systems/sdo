{{- define "web.workload" -}}
{{- $kind := .Values.global.workloadKind -}}
{{- if not (has $kind (list "Deployment" "StatefulSet")) -}}{{ fail (printf "web: unsupported workloadKind %q" $kind) }}{{- end -}}
{{ $kind }}
{{- end -}}

{{- define "web.env" -}}
{{- range $name, $value := .Values.extraEnv }}
- name: {{ $name }}
  value: {{ tpl $value $ | quote }}
{{- end }}
{{- end -}}

{{- define "web.scheme" -}}{{ ternary "https" "http" .Values.global.hosts.https }}{{- end -}}
