package mapseed

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	"sdo.dev/operator/memory/appmap"
)

// chart is a synthetic chart-like rendering of a small multi-component
// application shipped without source: a web front end, an api, a background
// worker, a metrics exporter, a git-storage tier with a custom image, and
// third-party postgres, redis and object-store workloads. Every name is made
// up; nothing here is application specific.
const chart = `
apiVersion: v1
kind: Secret
metadata: {name: shop-db-password, namespace: shop}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: shop-api, namespace: shop}
data:
  database.yml: |
    production:
      adapter: postgresql
      host: "shop-postgresql"
      port: 5432
  cache.yml: "url: redis://:<%= password %>@shop-redis:6379/0"
  storage.toml: "address = \"tcp://shop-git-0.shop-git.shop.svc:8075\""
  notes.txt: "the web tier calls the api; worker keeps queues"
---
apiVersion: v1
kind: ConfigMap
metadata: {name: shop-worker, namespace: shop}
data:
  S3_ENDPOINT: http://shop-objects.shop.svc:9000
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: shop-web, namespace: shop}
spec:
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web, tier: front}}
    spec:
      containers:
      - name: web
        image: registry.invalid/acme/web:v1
        ports: [{name: http, containerPort: 8080}]
        env:
        - {name: API_URL, value: "http://shop-api:8181/"}
        readinessProbe: {httpGet: {path: /-/readiness, port: http}}
        livenessProbe: {httpGet: {path: /-/liveness, port: 8080}}
---
apiVersion: v1
kind: Service
metadata: {name: shop-web, namespace: shop}
spec:
  type: NodePort
  selector: {app: web}
  ports: [{name: http, port: 80, targetPort: http}, {name: http-metrics, port: 9090}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: shop-api, namespace: shop}
spec:
  selector: {matchLabels: {app: api}}
  template:
    metadata: {labels: {app: api}}
    spec:
      containers:
      - name: api
        image: registry.invalid/acme/api:v1
        ports: [{containerPort: 8181}]
      volumes:
      - name: config
        projected:
          sources:
          - configMap: {name: shop-api}
---
apiVersion: v1
kind: Service
metadata: {name: shop-api, namespace: shop}
spec:
  selector: {app: api}
  ports: [{port: 8181}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: shop-worker, namespace: shop}
spec:
  selector: {matchLabels: {app: worker}}
  template:
    metadata: {labels: {app: worker}}
    spec:
      containers:
      - name: worker
        image: registry.invalid/acme/worker:v1
        envFrom: [{configMapRef: {name: shop-worker}}]
        env:
        - {name: REDIS_HOST, value: shop-redis}
        - {name: GREETING, value: "hello shop-api users"}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: shop-exporter, namespace: shop}
spec:
  selector: {matchLabels: {app: exporter}}
  template:
    metadata: {labels: {app: exporter}}
    spec:
      containers:
      - name: exporter
        image: quay.invalid/prometheuscommunity/postgres-exporter:v0.15.0
        env:
        - {name: DATA_SOURCE_NAME, value: "postgresql://metrics@shop-postgresql:5432/shop?sslmode=disable"}
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: shop-git, namespace: shop}
spec:
  serviceName: shop-git
  selector: {matchLabels: {app: git}}
  template:
    metadata: {labels: {app: git}}
    spec:
      containers:
      - {name: git, image: "registry.invalid/acme/git-storage:v1"}
  volumeClaimTemplates:
  - metadata: {name: repo-data}
---
apiVersion: v1
kind: Service
metadata: {name: shop-git, namespace: shop}
spec:
  clusterIP: None
  selector: {app: git}
  ports: [{port: 8075}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: shop-postgresql, namespace: shop}
spec:
  selector: {matchLabels: {app.kubernetes.io/name: shop-postgresql}}
  template:
    metadata: {labels: {app.kubernetes.io/name: shop-postgresql}}
    spec:
      containers:
      - {name: postgres, image: "postgres:17@sha256:0000000000000000000000000000000000000000000000000000000000000000"}
      - {name: metrics, image: "quay.invalid/prometheuscommunity/postgres-exporter:v0.15.0"}
  volumeClaimTemplates:
  - metadata: {name: data}
---
apiVersion: v1
kind: Service
metadata: {name: shop-postgresql, namespace: shop}
spec:
  selector: {app.kubernetes.io/name: shop-postgresql}
  ports: [{name: postgresql, port: 5432}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: shop-redis, namespace: shop}
spec:
  selector: {matchLabels: {app: redis}}
  template:
    metadata: {labels: {app: redis}}
    spec:
      containers:
      - {name: redis, image: "redis:7.4"}
      volumes:
      - {name: data, emptyDir: {}}
---
apiVersion: v1
kind: Service
metadata: {name: shop-redis, namespace: shop}
spec:
  selector: {app: redis}
  ports: [{name: redis, port: 6379}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: shop-objects, namespace: shop}
spec:
  selector: {matchLabels: {app: objects}}
  template:
    metadata: {labels: {app: objects}}
    spec:
      containers:
      - {name: objects, image: "docker.invalid/minio/minio:RELEASE.2025"}
      volumes:
      - {name: data, persistentVolumeClaim: {claimName: shop-objects}}
---
apiVersion: v1
kind: Service
metadata: {name: shop-objects, namespace: shop}
spec:
  selector: {app: objects}
  ports: [{name: s3, port: 9000}]
---
apiVersion: batch/v1
kind: Job
metadata: {name: shop-migrations, namespace: shop}
spec:
  template:
    spec:
      containers:
      - name: migrate
        image: registry.invalid/acme/api:v1
        env:
        - {name: DB_HOST, value: shop-postgresql}
`

func objects(t *testing.T, text string) []*unstructured.Unstructured {
	t.Helper()
	var out []*unstructured.Unstructured
	for _, doc := range strings.Split(text, "\n---\n") {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		var object map[string]any
		if err := yaml.Unmarshal([]byte(doc), &object); err != nil {
			t.Fatalf("fixture: %v\n%s", err, doc)
		}
		out = append(out, &unstructured.Unstructured{Object: object})
	}
	return out
}

func components(m appmap.Map) map[string]appmap.Component {
	out := map[string]appmap.Component{}
	for _, c := range m.Components {
		out[c.Name] = c
	}
	return out
}

func edges(m appmap.Map) map[string]appmap.Edge {
	out := map[string]appmap.Edge{}
	for _, e := range m.Edges {
		out[e.From+" -> "+e.To] = e
	}
	return out
}

func TestSeedProposesComponentsAndDataRolesFromManifests(t *testing.T) {
	seed := FromObjects(objects(t, chart))
	got := components(seed.Map)
	want := map[string]struct {
		workload  string
		services  string
		holdsData bool
		engine    string
	}{
		"shop-web":        {"Deployment/shop-web", "shop-web", false, ""},
		"shop-api":        {"Deployment/shop-api", "shop-api", false, ""},
		"shop-worker":     {"Deployment/shop-worker", "", false, ""},
		"shop-exporter":   {"Deployment/shop-exporter", "", false, ""},
		"shop-git":        {"StatefulSet/shop-git", "shop-git", true, appmap.EngineOther},
		"shop-postgresql": {"StatefulSet/shop-postgresql", "shop-postgresql", true, appmap.EnginePostgres},
		"shop-redis":      {"StatefulSet/shop-redis", "shop-redis", false, appmap.EngineRedis},
		"shop-objects":    {"StatefulSet/shop-objects", "shop-objects", true, appmap.EngineOther},
	}
	if len(got) != len(want) {
		t.Fatalf("components = %+v", seed.Map.Components)
	}
	for name, w := range want {
		c, ok := got[name]
		if !ok {
			t.Fatalf("no component %s in %+v", name, seed.Map.Components)
		}
		if c.Workload != w.workload || strings.Join(c.Services, ",") != w.services || c.HoldsData != w.holdsData {
			t.Errorf("component %s = %+v, want %+v", name, c, w)
		}
		switch {
		case w.engine == "" && c.Data != nil:
			t.Errorf("component %s: data = %+v, want none", name, c.Data)
		case w.engine != "" && (c.Data == nil || c.Data.Engine != w.engine || c.Data.Role != appmap.RoleAuthoritative):
			t.Errorf("component %s: data = %+v, want authoritative %s", name, c.Data, w.engine)
		}
		if strings.Join(c.Evidence, ",") != appmap.EvidenceManifest {
			t.Errorf("component %s: evidence = %v, want [manifest]", name, c.Evidence)
		}
	}
}

func TestSeedFindsEdgesInEnvAndConfigMaps(t *testing.T) {
	seed := FromObjects(objects(t, chart))
	got := edges(seed.Map)
	want := map[string]string{
		"shop-web -> shop-api":             "Deployment/shop-web env API_URL",
		"shop-api -> shop-postgresql":      "Deployment/shop-api ConfigMap/shop-api key database.yml",
		"shop-api -> shop-redis":           "Deployment/shop-api ConfigMap/shop-api key cache.yml",
		"shop-api -> shop-git":             "Deployment/shop-api ConfigMap/shop-api key storage.toml",
		"shop-worker -> shop-redis":        "Deployment/shop-worker env REDIS_HOST",
		"shop-worker -> shop-objects":      "Deployment/shop-worker ConfigMap/shop-worker key S3_ENDPOINT",
		"shop-exporter -> shop-postgresql": "Deployment/shop-exporter env DATA_SOURCE_NAME",
	}
	for label, source := range want {
		e, ok := got[label]
		if !ok {
			t.Errorf("missing edge %s", label)
			continue
		}
		if e.Source != source || strings.Join(e.Evidence, ",") != appmap.EvidenceManifest {
			t.Errorf("edge %s = %+v, want source %q and evidence [manifest]", label, e, source)
		}
	}
	for label := range got {
		if _, ok := want[label]; !ok {
			t.Errorf("unexpected edge %s (prose, a bare word or a Job is not a call)", label)
		}
	}
}

func TestSeedListsEntryCandidatesWithHealthEndpoints(t *testing.T) {
	seed := FromObjects(objects(t, chart))
	if len(seed.Entries) != 1 {
		t.Fatalf("entries = %+v, want the web Service's application port only (not its metrics port)", seed.Entries)
	}
	e := seed.Entries[0]
	if e.Component != "shop-web" || e.Service != "shop-web" || e.Port != 80 || !strings.Contains(e.Exposure, "NodePort") {
		t.Fatalf("entry = %+v", e)
	}
	if strings.Join(e.Health, "; ") != "readiness GET /-/readiness port 8080; liveness GET /-/liveness port 8080" {
		t.Fatalf("health = %q", e.Health)
	}
}

// A Job of the rendered manifests is a procedure the application ships: Go
// seeds it as a re-created Job with a long effect, and the designer corrects
// the effect class and adds the procedures only source or docs reveal.
func TestSeedRecordsEachJobAsAProcedure(t *testing.T) {
	seed := FromObjects(objects(t, chart))
	if len(seed.Map.Procedures) != 1 {
		t.Fatalf("procedures = %+v", seed.Map.Procedures)
	}
	p := seed.Map.Procedures[0]
	if p.ID != "shop-migrations" || p.Job != "Job/shop-migrations" || p.Effect != "long" || p.Source != "manifest Job/shop-migrations" {
		t.Fatalf("procedure = %+v", p)
	}
	if !strings.Contains(seed.Region(), "procedures:\n  - {id: shop-migrations, job: \"Job/shop-migrations\", effect: long, source: \"manifest Job/shop-migrations\"}\n") {
		t.Fatalf("region = %s", seed.Region())
	}
}

// The seeded region is an application map the memory check accepts against
// the inventory of the same objects: only the non-blocking note for edges no
// operation exercises yet.
func TestSeedRegionPassesTheMapCheck(t *testing.T) {
	objs := objects(t, chart)
	seed := FromObjects(objs)
	region, found, err := appmap.Find("# inventory\n\n" + seed.Region())
	if !found || err != nil {
		t.Fatalf("region = %q (%v, %v)", seed.Region(), found, err)
	}
	parsed, err := appmap.Parse(region.YAML)
	if err != nil {
		t.Fatalf("seeded map does not parse: %v\n%s", err, region.YAML)
	}
	inv := appmap.Inventory{Workloads: map[string]bool{}, Services: map[string]bool{}}
	for _, o := range objs {
		inv.Workloads[o.GetKind()+"/"+o.GetName()] = true
		if o.GetKind() == "Service" {
			inv.Services[o.GetName()] = true
		}
	}
	// The seed has no operations yet: its edges are unexercised, and those
	// from an entry component are the designer's to cover.
	for _, f := range parsed.Check(inv, nil) {
		if f.Rule != appmap.RuleUnexercisedEdge && f.Rule != appmap.RuleUncoveredEdge {
			t.Errorf("finding %+v", f)
		}
	}
	if len(parsed.Components) != len(seed.Map.Components) || len(parsed.Edges) != len(seed.Map.Edges) || len(parsed.Procedures) != len(seed.Map.Procedures) {
		t.Fatalf("round trip lost entries:\n%s", region.YAML)
	}
}

// A cache whose callers also call an authoritative store is derived from it.
func TestSeedDerivesACacheFromTheStoresItsCallersUse(t *testing.T) {
	text := chart + `
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: shop-cache, namespace: shop}
spec:
  selector: {matchLabels: {app: cache}}
  template:
    metadata: {labels: {app: cache}}
    spec:
      containers:
      - {name: memcached, image: "memcached:1.6"}
---
apiVersion: v1
kind: Service
metadata: {name: shop-cache, namespace: shop}
spec:
  selector: {app: cache}
  ports: [{port: 11211}]
---
apiVersion: v1
kind: ConfigMap
metadata: {name: shop-exporter, namespace: shop}
data:
  MEMCACHED_SERVERS: shop-cache:11211
`
	text = strings.Replace(text, "        env:\n        - {name: DATA_SOURCE_NAME", "        envFrom: [{configMapRef: {name: shop-exporter}}]\n        env:\n        - {name: DATA_SOURCE_NAME", 1)
	cache := components(FromObjects(objects(t, text)).Map)["shop-cache"]
	if cache.HoldsData || cache.Data == nil || cache.Data.Engine != appmap.EngineMemcached || cache.Data.Role != appmap.RoleDerived || strings.Join(cache.Data.DerivedFrom, ",") != "shop-postgresql" {
		t.Fatalf("cache = %+v data %+v", cache, cache.Data)
	}
}

func TestSeedIsDeterministic(t *testing.T) {
	first := FromObjects(objects(t, chart)).Region()
	objs := objects(t, chart)
	for i, j := 0, len(objs)-1; i < j; i, j = i+1, j-1 {
		objs[i], objs[j] = objs[j], objs[i]
	}
	if again := FromObjects(objs).Region(); again != first {
		t.Fatalf("object order changed the seed:\n%s\n---\n%s", first, again)
	}
}
