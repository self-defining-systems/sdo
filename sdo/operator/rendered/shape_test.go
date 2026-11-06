package rendered_test

import (
	"context"
	"strings"
	"testing"

	"sdo.dev/operator/render"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
)

const shapeBase = `apiVersion: apps/v1
kind: Deployment
metadata: {name: web, labels: {a: b}}
spec:
  replicas: 2
  template:
    spec:
      containers:
      - name: c
        image: example/web:1
        args: ["--x"]
        env: [{name: K, value: v}]
        resources: {limits: {cpu: "1"}}
        ports: [{containerPort: 80}]
        readinessProbe: {httpGet: {path: /ready, port: 80}}
---
apiVersion: v1
kind: Service
metadata: {name: web}
spec: {selector: {app: web}, ports: [{port: 80}]}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: cfg}
data: {k: v}
---
apiVersion: v1
kind: Secret
metadata: {name: creds}
stringData: {password: one}
`

func shapeOf(t *testing.T, manifest string) string {
	t.Helper()
	set, err := rendered.Desired(context.Background(), rendertest.MapFS{"k8s/all.yaml": manifest}, render.Env{Namespace: "app"}, catalog(t), rawSource(t, "k8s"))
	if err != nil {
		t.Fatal(err)
	}
	if set.ShapeDigest == "" {
		t.Fatal("empty shape digest")
	}
	return set.ShapeDigest
}

// TestShapeDigestDefinition lists what the shape includes and what it does
// not. A benign source change (new image, args, env, replicas, labels, data)
// must not move it; the topology the health judge reasons about must.
func TestShapeDigestDefinition(t *testing.T) {
	base := shapeOf(t, shapeBase)
	replace := func(old, new string) string {
		if !strings.Contains(shapeBase, old) {
			t.Fatalf("fixture lacks %q", old)
		}
		return strings.Replace(shapeBase, old, new, 1)
	}
	for _, tc := range []struct {
		name, manifest string
		same           bool
	}{
		{"image", replace("example/web:1", "example/web:2"), true},
		{"image digest", replace("example/web:1", "example/web@sha256:"+strings.Repeat("a", 64)), true},
		{"args", replace(`["--x"]`, `["--y"]`), true},
		{"env", replace("value: v}]", "value: w}]"), true},
		{"resources", replace(`cpu: "1"`, `cpu: "2"`), true},
		{"replicas", replace("replicas: 2", "replicas: 5"), true},
		{"labels", replace("labels: {a: b}", "labels: {a: c}"), true},
		{"configmap data", replace("data: {k: v}", "data: {k: changed}"), true},
		{"secret data", replace("password: one", "password: two"), true},
		{"object order", orderReversed(), true},

		{"workload renamed", replace("metadata: {name: web, labels", "metadata: {name: api, labels"), false},
		{"workload added", shapeBase + "---\napiVersion: apps/v1\nkind: Deployment\nmetadata: {name: extra}\nspec: {template: {spec: {containers: [{name: c, image: x}]}}}\n", false},
		{"workload removed", shapeBase[strings.Index(shapeBase, "---\napiVersion: v1\nkind: Service"):][4:], false},
		{"container renamed", replace("- name: c\n", "- name: main\n"), false},
		{"container port", replace("containerPort: 80", "containerPort: 8080"), false},
		{"probe path", replace("path: /ready", "path: /healthz"), false},
		{"probe removed", replace("        readinessProbe: {httpGet: {path: /ready, port: 80}}\n", ""), false},
		{"service port", replace("ports: [{port: 80}]", "ports: [{port: 81}]"), false},
		{"service selector", replace("app: web", "app: other"), false},
		{"configmap added", shapeBase + "---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: more}\n", false},
		{"secret renamed", replace("name: creds", "name: creds2"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := shapeOf(t, tc.manifest)
			if (got == base) != tc.same {
				t.Fatalf("shape equal = %v, want %v", got == base, tc.same)
			}
		})
	}
}

func orderReversed() string {
	docs := strings.Split(shapeBase, "---\n")
	for i, j := 0, len(docs)-1; i < j; i, j = i+1, j-1 {
		docs[i], docs[j] = docs[j], docs[i]
	}
	return strings.Join(docs, "---\n")
}

func TestSourceShapeOfTracksShapeAcrossRevisions(t *testing.T) {
	shape := func(manifest string) string {
		src := rendered.Source{Repository: "/synthetic", Deployment: rawSource(t, "k8s"), Catalog: catalog(t), Git: treeGit(rendertest.MapFS{"k8s/all.yaml": manifest, "main.go": "package main"})}
		got, err := src.ShapeOf("app")(context.Background(), "/synthetic", syntheticCommit)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if shape(shapeBase) != shape(strings.Replace(shapeBase, "example/web:1", "example/web:2", 1)) {
		t.Fatal("an image change moved the shape")
	}
	if shape(shapeBase) == shape(strings.Replace(shapeBase, "containerPort: 80", "containerPort: 81", 1)) {
		t.Fatal("a port change did not move the shape")
	}
	if (rendered.Source{}).ShapeOf("app") == nil {
		t.Fatal("a source-less install falls back to the effective tree ID, never to nil")
	}
}

// A Job named after a hash of its inputs is renamed by every release that
// changes them. The shape (what the health judge's policy is written against)
// keeps its stable identity, so an image-only release is not a shape change;
// a renamed Deployment or a differently named Job still is.
func TestShapeDigestKeepsAGeneratedJobNameStable(t *testing.T) {
	job := func(name, image string) string {
		return shapeBase + "---\napiVersion: batch/v1\nkind: Job\nmetadata: {name: " + name + "}\nspec: {template: {spec: {containers: [{name: m, image: '" + image + "'}]}}}\n"
	}
	base := shapeOf(t, job("migrate-d63dd88", "example/migrate:1"))
	if got := shapeOf(t, job("migrate-0a1b2c3", "example/migrate:2")); got != base {
		t.Error("a Job renamed by its generated suffix moved the shape")
	}
	if got := shapeOf(t, job("seed-0a1b2c3", "example/migrate:1")); got == base {
		t.Error("a Job with another stable name kept the shape")
	}
	if got := shapeOf(t, strings.Replace(job("migrate-d63dd88", "example/migrate:1"), "name: web,", "name: web2,", 1)); got == base {
		t.Error("a renamed Deployment kept the shape")
	}
}
