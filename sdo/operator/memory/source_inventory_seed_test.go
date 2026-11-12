package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

func seedWorkload(kind, name, image string, env ...string) *unstructured.Unstructured {
	var vars []any
	for i := 0; i+1 < len(env); i += 2 {
		vars = append(vars, map[string]any{"name": env[i], "value": env[i+1]})
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": kind, "metadata": map[string]any{"name": name, "namespace": "shop"},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": name}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": name}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": name, "image": image, "env": vars}}},
			},
		},
	}}
}

func seedService(name string, port int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": name, "namespace": "shop"},
		"spec": map[string]any{"selector": map[string]any{"app": name}, "ports": []any{map[string]any{"port": port}}},
	}}
}

// seedObjects is a synthetic source-less application: web -> api -> db
// (postgres) and cache (redis), declared only through env values.
func seedObjects() []*unstructured.Unstructured {
	return []*unstructured.Unstructured{
		seedWorkload("Deployment", "web", "registry.invalid/web:v1", "API_URL", "http://api:8080"),
		seedService("web", 80),
		seedWorkload("Deployment", "api", "registry.invalid/api:v1", "DATABASE_URL", "postgres://app@db:5432/app", "REDIS_HOST", "cache"),
		seedService("api", 8080),
		seedWorkload("StatefulSet", "db", "postgres:17"),
		seedService("db", 5432),
		seedWorkload("Deployment", "cache", "redis:7.4"),
		seedService("cache", 6379),
	}
}

// The first architecture of an application with no map carries the map Go
// seeded from the rendered objects, and Go installs a store reader for each
// seeded store it can read.
func TestFirstArchitectureCarriesTheSeededMapAndItsStoreReaders(t *testing.T) {
	checkout := t.TempDir()
	if err := WriteArchitecture(checkout, "shop", "c1", ArchitectureInventory(SourceFactsOf(seedObjects(), "raw"))); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(checkout, layout.ArchitectureFile))
	if err != nil {
		t.Fatal(err)
	}
	region, found, err := appmap.Find(string(data))
	if !found || err != nil {
		t.Fatalf("no seeded region (%v):\n%s", err, data)
	}
	parsed, err := appmap.Parse(region.YAML)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range parsed.Check(appmap.ParseInventory(string(data)), nil) {
		if f.Rule != appmap.RuleCoverage {
			t.Errorf("seeded map finding: %+v", f)
		}
	}
	if !strings.Contains(appmap.Outside(string(data)), "Entry candidates:\n- Service web port 80, component web") {
		t.Errorf("no entry candidate for web:\n%s", data)
	}
	var readers []string
	for _, d := range plannedStoreStateDetectors(checkout) {
		readers = append(readers, d.manifest.ID)
	}
	if strings.Join(readers, ",") != "store-state-cache,store-state-db" {
		t.Fatalf("store readers = %v, want both seeded stores", readers)
	}
}

// Once the designer has a map, a regenerated inventory carries it and never
// the seed.
func TestRegeneratedArchitectureKeepsTheDesignersMapOverTheSeed(t *testing.T) {
	checkout := t.TempDir()
	path := filepath.Join(checkout, layout.ArchitectureFile)
	if err := WriteArchitecture(checkout, "shop", "c1", "# Manifest inventory\n\n- Deployment/shop/web (source: raw)\n"); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	designer := appmap.BeginMarker + "\n```yaml\nversion: 1\n# the designer's\n```\n" + appmap.EndMarker + "\n"
	if err := os.WriteFile(path, append(first, []byte("\n"+designer)...), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteArchitecture(checkout, "shop", "c2", ArchitectureInventory(SourceFactsOf(seedObjects(), "raw"))); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	region, found, err := appmap.Find(string(second))
	if err != nil || !found || region.Block != designer {
		t.Fatalf("region = %q (%v, %v)\n%s", region.Block, found, err, second)
	}
}

// The inventory records how each Service is exposed outside the cluster and
// its metrics ports, and the application map's coverage reads them back from
// the architecture text.
func TestInventoryRecordsServiceExposureForCoverage(t *testing.T) {
	objects := seedObjects()
	web := objects[1]
	web.Object["spec"].(map[string]any)["type"] = "NodePort"
	web.Object["spec"].(map[string]any)["ports"] = []any{map[string]any{"name": "http", "port": int64(80)}, map[string]any{"name": "metrics", "port": int64(9090)}}
	text := ArchitectureInventory(SourceFactsOf(objects, "raw"))
	inv := appmap.ParseInventory(text)
	if got := inv.Exposure["web"]; len(got) != 1 || got[0] != "NodePort" {
		t.Fatalf("web exposure = %v\n%s", got, text)
	}
	if got := inv.MetricsPorts["web"]; len(got) != 1 || got[0] != 9090 {
		t.Fatalf("web metrics ports = %v", got)
	}
	if len(inv.Exposure["api"]) != 0 || !inv.Services["api"] {
		t.Fatalf("api is a ClusterIP Service: %+v", inv)
	}
}
