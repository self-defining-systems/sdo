package rendered_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"sdo.dev/operator/render"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
)

// podManifests has one manifest per built-in kind that carries a pod spec,
// each with one init and one regular container.
var podManifests = map[string]string{
	"Deployment":            "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: x}\nspec: {template: {spec: {initContainers: [{name: i, image: 'r/%[1]s-init:1'}], containers: [{name: c, image: 'r/%[1]s:1'}]}}}\n",
	"StatefulSet":           "apiVersion: apps/v1\nkind: StatefulSet\nmetadata: {name: x}\nspec: {template: {spec: {initContainers: [{name: i, image: 'r/%[1]s-init:1'}], containers: [{name: c, image: 'r/%[1]s:1'}]}}}\n",
	"DaemonSet":             "apiVersion: apps/v1\nkind: DaemonSet\nmetadata: {name: x}\nspec: {template: {spec: {initContainers: [{name: i, image: 'r/%[1]s-init:1'}], containers: [{name: c, image: 'r/%[1]s:1'}]}}}\n",
	"ReplicaSet":            "apiVersion: apps/v1\nkind: ReplicaSet\nmetadata: {name: x}\nspec: {template: {spec: {initContainers: [{name: i, image: 'r/%[1]s-init:1'}], containers: [{name: c, image: 'r/%[1]s:1'}]}}}\n",
	"ReplicationController": "apiVersion: v1\nkind: ReplicationController\nmetadata: {name: x}\nspec: {template: {spec: {initContainers: [{name: i, image: 'r/%[1]s-init:1'}], containers: [{name: c, image: 'r/%[1]s:1'}]}}}\n",
	"Job":                   "apiVersion: batch/v1\nkind: Job\nmetadata: {name: x}\nspec: {template: {spec: {initContainers: [{name: i, image: 'r/%[1]s-init:1'}], containers: [{name: c, image: 'r/%[1]s:1'}]}}}\n",
	"CronJob":               "apiVersion: batch/v1\nkind: CronJob\nmetadata: {name: x}\nspec: {jobTemplate: {spec: {template: {spec: {initContainers: [{name: i, image: 'r/%[1]s-init:1'}], containers: [{name: c, image: 'r/%[1]s:1'}]}}}}}\n",
	"Pod":                   "apiVersion: v1\nkind: Pod\nmetadata: {name: x}\nspec: {initContainers: [{name: i, image: 'r/%[1]s-init:1'}], containers: [{name: c, image: 'r/%[1]s:1'}]}\n",
}

func podManifest(kind string) string {
	return strings.ReplaceAll(podManifests[kind], "%[1]s", strings.ToLower(kind))
}

func TestPodManifestsCoverTheTable(t *testing.T) {
	kinds := rendered.PodKinds()
	if len(kinds) != len(podManifests) {
		t.Fatalf("table kinds %v, manifests for %d kinds", kinds, len(podManifests))
	}
	for _, kind := range kinds {
		if _, ok := podManifests[kind]; !ok {
			t.Errorf("no manifest for table kind %s", kind)
		}
	}
}

// Every kind with a pod spec is a workload: its images are image sites (so
// the registry allowlist and image resolution see them) and the plan must
// name it.
func TestEveryPodKindIsAWorkloadWithImageSites(t *testing.T) {
	for _, kind := range rendered.PodKinds() {
		t.Run(kind, func(t *testing.T) {
			lower := strings.ToLower(kind)
			set, err := rendered.Desired(context.Background(), rendertest.MapFS{"k8s/all.yaml": podManifest(kind)}, render.Env{Namespace: "app"}, catalog(t), rawSource(t, "k8s"))
			if err != nil {
				t.Fatal(err)
			}
			if got, want := strings.Join(rendered.ImageSites(set), " "), "r/"+lower+"-init:1 r/"+lower+":1"; got != want {
				t.Fatalf("image sites = %q, want %q", got, want)
			}
			id := kind + "/app/x"
			if err := rendered.CheckExpectedWorkloads(set, []string{id}); err != nil {
				t.Fatal(err)
			}
			if err := rendered.CheckExpectedWorkloads(set, nil); err == nil {
				t.Fatalf("a plan without %s was accepted", id)
			}
			obj := set.Objects()[0]
			if !obj.Workload {
				t.Fatalf("%s is not marked a workload", kind)
			}
			spec, err := rendered.PodSpec(obj.Object.Object)
			if err != nil || spec["containers"] == nil {
				t.Fatalf("PodSpec = %v, %v", spec, err)
			}
		})
	}
}

func TestPodKindWithoutContainersIsRejected(t *testing.T) {
	for _, manifest := range []string{
		"apiVersion: batch/v1\nkind: CronJob\nmetadata: {name: x}\nspec: {schedule: '* * * * *'}\n",
		"apiVersion: v1\nkind: Pod\nmetadata: {name: x}\nspec: {}\n",
	} {
		_, err := rendered.Desired(context.Background(), rendertest.MapFS{"k8s/all.yaml": manifest}, render.Env{Namespace: "app"}, catalog(t), rawSource(t, "k8s"))
		if err == nil {
			t.Errorf("accepted %q", manifest)
		}
	}
}

// Only podtemplate.go may spell out where a pod spec lives. The exempt
// packages cannot import rendered (sdk, which rendered imports) or read other
// documents than SDO's desired state (renderers, the cluster simulation, the
// API history projection, kubectl read helpers).
//
//ci:whole-tree
func TestPodTemplateTableIsTheOnlyLocator(t *testing.T) {
	exempt := []string{"sdk", "render", "runtime/sim", "runtime/replay", "apihistory", "kubereads"}
	root := ".."
	seen := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if d.Name() == "testdata" || d.Name() == "gen" {
				return fs.SkipDir
			}
			for _, e := range exempt {
				if rel == e {
					return fs.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || rel == "rendered/podtemplate.go" {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, src, 0)
		if err != nil {
			return err
		}
		seen++
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, _ := strconv.Unquote(lit.Value)
			if value == "jobTemplate" || value == "initContainers" {
				t.Errorf("%s: %s spells out a pod-spec location; use rendered.PodSpec / rendered.ContainerFields", rel, lit.Value)
			}
			return true
		})
		// A second list of pod-bearing kinds in a deploy-path package is a
		// second table. deployment/backend.go switches on kind to judge
		// readiness; TestEveryPodKindIsAttested (deployment) proves that
		// switch covers the table.
		if !deployPath(rel) || rel == "deployment/backend.go" {
			return nil
		}
		kinds := map[string]bool{}
		ast.Inspect(file, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if value, _ := strconv.Unquote(lit.Value); value == "StatefulSet" || value == "DaemonSet" || value == "CronJob" {
					kinds[value] = true
				}
			}
			return true
		})
		if len(kinds) >= 2 {
			t.Errorf("%s lists several pod-bearing kinds (%v); use rendered.WorkloadKind / rendered.PodKinds", rel, kinds)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen < 50 {
		t.Fatalf("scanned %d files; the walk is broken", seen)
	}
}

func deployPath(rel string) bool {
	for _, dir := range []string{"rendered/", "deployment/", "lifecycle/", "operability/", "imagesrc/", "memory/source_inventory.go", "runtime/source_dependencies.go"} {
		if strings.HasPrefix(rel, dir) {
			return true
		}
	}
	return false
}
