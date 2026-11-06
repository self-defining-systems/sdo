{{- define "objectstore.labels" -}}
app.kubernetes.io/name: {{ .Values.name }}
app.kubernetes.io/component: datastore
{{- end -}}
