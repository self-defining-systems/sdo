{{- define "gs.fullname" -}}{{ .Release.Name }}-{{ .Chart.Name }}{{- end -}}

{{/* Key under .Values.global, resolved by name the way umbrella charts do. */}}
{{- define "gs.global" -}}
{{- $v := index .ctx.Values.global .key -}}
{{- if kindIs "string" $v }}{{ tpl $v .ctx }}{{ else }}{{ $v }}{{ end -}}
{{- end -}}

{{- define "gs.kind.sidekiq" -}}bundle exec sidekiq{{- end -}}
{{- define "gs.kind.puma" -}}bundle exec puma{{- end -}}
