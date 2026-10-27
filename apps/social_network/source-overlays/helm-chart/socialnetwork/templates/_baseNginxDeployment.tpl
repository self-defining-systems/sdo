{{- define "socialnetwork.templates.baseNginxDeployment" }}
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    service: {{ .Values.name }}
  name: {{ .Values.name }}
spec: 
  replicas: {{ .Values.replicas | default .Values.global.replicas }}
  selector:
    matchLabels:
      service: {{ .Values.name }}
  template:
    metadata:
      labels:
        service: {{ .Values.name }}
        app: {{ .Values.name }}
    spec: 
      containers:
      {{- with .Values.container }}
      - name: "{{ .name }}"
        image: {{ .dockerRegistry | default $.Values.global.dockerRegistry }}/{{ .image }}:{{ .imageVersion | default $.Values.global.defaultImageVersion }}
        imagePullPolicy: {{ .imagePullPolicy | default $.Values.global.imagePullPolicy }}
        ports:
        {{- range $cport := .ports }}
        - containerPort: {{ $cport.containerPort -}}
        {{ end }} 
        {{- if .env }}
        env:
        {{- range $e := .env}}
        - name: {{ $e.name }}
          value: "{{ (tpl ($e.value | toString) $) }}"
        {{ end -}}
        {{ end -}}
        {{- if .command}}
        command: 
        - {{ .command }}
        {{- end -}}
        {{- if .args}}
        args:
        {{- range $arg := .args}}
        - {{ $arg }}
        {{- end -}}
        {{- end }}
        {{- if .resources }}  
        resources:
          {{ toYaml .resources | nindent 10 | trim }}
        {{- else if hasKey $.Values.global "resources" }}           
        resources:
          {{ toYaml $.Values.global.resources | nindent 10 | trim }}
        {{- end }}  
        {{- if $.Values.configMaps }}        
        volumeMounts: 
        {{- range $configMap := $.Values.configMaps }}
        - name: {{ $.Values.name }}-config
          mountPath: {{ $configMap.mountPath }}
          subPath: {{ $configMap.name }}        
        {{- end }}
        {{- range .volumeMounts }}
        - name: {{ .name }}
          mountPath: {{ .mountPath }}
        {{- end }}
        {{- end }}
      {{- end }}

      {{- with .Values.initContainer }}
      initContainers:
      - name: "{{ .name }}"
        image: {{ .dockerRegistry | default $.Values.global.dockerRegistry }}/{{ .image }}:{{ .imageVersion | default $.Values.global.defaultImageVersion }}
        imagePullPolicy: {{ .imagePullPolicy | default $.Values.global.imagePullPolicy }}
        {{- if .command}}
        command: 
        - {{ .command }}
        {{- end -}}
        {{- if .resources }}
        resources:
          {{ toYaml .resources | nindent 10 | trim }}
        {{- else if hasKey $.Values.global "resources" }}
        resources:
          {{ toYaml $.Values.global.resources | nindent 10 | trim }}
        {{- end }}
        {{- if .env }}
        env:
        {{- range $e := .env}}
        - name: {{ $e.name }}
          value: "{{ (tpl ($e.value | toString) $) }}"
        {{ end -}}
        {{ end -}}
        {{- if .args}}
        args:
        {{- range $arg := .args}}
        - {{ $arg }}
        {{- end -}}
        {{- end }}
        {{- if .volumeMounts }}        
        volumeMounts: 
        {{- range .volumeMounts }}
        - name: {{ .name }}
          mountPath: {{ .mountPath }}
        {{- end }}
        {{- end }}
      {{- end -}}

      {{- if $.Values.configMaps }}
      volumes:
      - name: {{ $.Values.name }}-config
        configMap:
          name: {{ $.Values.name }}
      {{- range $.Values.volumes }}
      {{- $volume := .name }}
      {{- $files := $.Files.Glob (printf "files/%s/**" $volume) }}
      - name: {{ $volume }}
      {{- if $files }}
        configMap:
          name: {{ $.Values.name }}-{{ $volume }}
          items:
          {{- range $path, $_ := $files }}
          {{- $key := include "socialnetwork.templates.fileKey" (dict "volume" $volume "path" $path) }}
          - key: {{ $key }}
            path: {{ $path | trimPrefix (printf "files/%s/" $volume) }}
          {{- end }}
      {{- else }}
        emptyDir: {}
      {{- end }}
      {{- end }}
      {{- end }}
      
      {{- if hasKey .Values "topologySpreadConstraints" }}
      topologySpreadConstraints:
        {{ tpl .Values.topologySpreadConstraints . | nindent 6 | trim }}
      {{- else if hasKey $.Values.global  "topologySpreadConstraints" }}
      topologySpreadConstraints:
        {{ tpl $.Values.global.topologySpreadConstraints . | nindent 6 | trim }}
      {{- end }}
      hostname: {{ $.Values.name }}
      restartPolicy: {{ .Values.restartPolicy | default .Values.global.restartPolicy}}

{{ include "socialnetwork.templates.baseHPA" . }}
{{- end}}
