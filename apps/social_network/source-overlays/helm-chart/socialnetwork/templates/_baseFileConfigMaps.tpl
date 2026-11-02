{{- /* A volume with files under the chart's files/<volume>/ is a ConfigMap built from them; a volume without any stays an emptyDir. */ -}}
{{- define "socialnetwork.templates.fileKey" -}}
{{ .path | trimPrefix (printf "files/%s/" .volume) | replace "/" "__" }}
{{- end }}

{{- define "socialnetwork.templates.baseFileConfigMaps" }}
{{- range $volume := .Values.volumes }}
{{- $files := $.Files.Glob (printf "files/%s/**" $volume.name) }}
{{- if $files }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ $.Values.name }}-{{ $volume.name }}
  labels:
    socialnetwork/service: {{ $.Values.name }}
data:
  {{- range $path, $_ := $files }}
  {{ include "socialnetwork.templates.fileKey" (dict "volume" $volume.name "path" $path) }}: {{ $.Files.Get $path | toJson }}
  {{- end }}
{{- end }}
{{- end }}
{{- end }}
