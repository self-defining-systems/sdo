{{- define "socialnetwork.templates.mongo.mongod.conf"  }}
net:
  tls:
    mode: disabled
storage:
  wiredTiger:
    engineConfig:
      cacheSizeGB: 0.25
{{- end }}