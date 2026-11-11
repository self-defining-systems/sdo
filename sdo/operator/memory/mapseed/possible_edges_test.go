package mapseed

import (
	"strings"
	"testing"
)

// Two copies of a small app: the database address lives only in a Secret the
// API reads by env, and the object store's connection only in a mounted
// Secret. Go never reads the values; the names alone suggest the calls.
const secretWired = `
apiVersion: apps/v1
kind: Deployment
metadata: {name: rel-api, namespace: app}
spec:
  selector: {matchLabels: {app: api}}
  template:
    metadata: {labels: {app: api}}
    spec:
      containers:
        - name: api
          image: example/api:1
          env:
            - name: DATABASE_URL
              valueFrom: {secretKeyRef: {name: rel-db-credentials, key: url}}
            - name: SESSION_KEY
              valueFrom: {secretKeyRef: {name: rel-session, key: key}}
          volumeMounts: [{name: storage, mountPath: /etc/storage}]
      volumes:
        - name: storage
          secret: {secretName: rel-object-storage, items: [{key: connection, path: connection}]}
---
apiVersion: v1
kind: Service
metadata: {name: rel-api, namespace: app}
spec: {selector: {app: api}, ports: [{port: 80}]}
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: rel-db, namespace: app}
spec:
  selector: {matchLabels: {app: db}}
  template:
    metadata: {labels: {app: db}}
    spec:
      containers:
        - name: db
          image: postgres:16
          env:
            - name: POSTGRES_PASSWORD
              valueFrom: {secretKeyRef: {name: rel-db-credentials, key: password}}
---
apiVersion: v1
kind: Service
metadata: {name: rel-db, namespace: app}
spec: {selector: {app: db}, ports: [{port: 5432}]}
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: rel-objectstore, namespace: app}
spec:
  selector: {matchLabels: {app: objectstore}}
  template:
    metadata: {labels: {app: objectstore}}
    spec:
      containers: [{name: s3, image: example/s3:1}]
---
apiVersion: v1
kind: Service
metadata: {name: rel-objectstore, namespace: app}
spec: {selector: {app: objectstore}, ports: [{port: 8333}]}
`

func TestSeedListsPossibleEdgesOnlyASecretReferenceNames(t *testing.T) {
	seed := FromObjects(objects(t, secretWired))
	got := map[string]string{}
	for _, e := range seed.Possible {
		got[e.From+" -> "+e.To] = e.Source
	}
	if len(got) != 2 || !strings.Contains(got["rel-api -> rel-db"], "Secret/rel-db-credentials") || !strings.Contains(got["rel-api -> rel-objectstore"], "Secret/rel-object-storage") {
		t.Fatalf("possible edges = %+v", seed.Possible)
	}
	if len(seed.Map.Edges) != 0 {
		t.Fatalf("a possible edge is not an edge: %+v", seed.Map.Edges)
	}
	summary := seed.Summary()
	if !strings.Contains(summary, "- possible edge: rel-api -> rel-db (") || !strings.Contains(summary, "did not read") {
		t.Fatalf("summary does not list the possible edges:\n%s", summary)
	}
}

func TestNamesMatchesAServiceStemWithoutShortPrefixes(t *testing.T) {
	for _, c := range []struct {
		text, stem string
		want       bool
	}{
		{"rel-object-storage connection", "objectstore", true},
		{"DATABASE_URL postgres url", "postgres", true},
		{"REDIS_PASSWORD app-secrets", "redis", true},
		{"webhook-secret", "web", false},
		{"session key", "sessionstore", false},
	} {
		if got := names(c.text, c.stem); got != c.want {
			t.Errorf("names(%q, %q) = %v", c.text, c.stem, got)
		}
	}
}
