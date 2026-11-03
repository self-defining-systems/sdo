//go:build integration

package sim_test

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
)

// schemaRoots are the kinds the simulated API serves with real server-side
// apply semantics (list-map keys, atomic lists, ownership granularity).
var schemaRoots = []string{
	"core.v1.Pod", "core.v1.Service", "core.v1.ConfigMap", "core.v1.Secret", "core.v1.Endpoints", "core.v1.ServiceAccount", "core.v1.PersistentVolumeClaim", "core.v1.Node", "core.v1.Event",
	"apps.v1.Deployment", "apps.v1.StatefulSet", "apps.v1.DaemonSet", "apps.v1.ReplicaSet",
	"batch.v1.Job", "batch.v1.CronJob",
	"networking.v1.Ingress", "networking.v1.NetworkPolicy",
	"autoscaling.v2.HorizontalPodAutoscaler", "policy.v1.PodDisruptionBudget", "discovery.v1.EndpointSlice", "coordination.v1.Lease",
}

// TestRegenerateSimSchema rewrites sim/testdata/openapi-subset.json from the
// pinned real API server: only definitions reachable from schemaRoots, without
// prose. It is a no-op unless SDO_REGEN_SIM_SCHEMA=1.
func TestRegenerateSimSchema(t *testing.T) {
	if os.Getenv("SDO_REGEN_SIM_SCHEMA") != "1" {
		t.Skip("set SDO_REGEN_SIM_SCHEMA=1 to refresh sim/testdata/openapi-subset.json")
	}
	b := startEnvtest(t)
	ctx := context.Background()
	// OpenAPI v3 keeps the schema defaults (such as a port's protocol) that
	// server-side apply needs to key list items; v2 drops them.
	indexRaw, err := b.rest.Get().AbsPath("/openapi/v3").SetHeader("Accept", "application/json").Do(ctx).Raw()
	if err != nil {
		t.Fatal(err)
	}
	var index struct {
		Paths map[string]struct {
			URL string `json:"serverRelativeURL"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(indexRaw, &index); err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Definitions map[string]map[string]any `json:"definitions"`
	}
	doc.Definitions = map[string]map[string]any{}
	for _, group := range []string{"api/v1", "apis/apps/v1", "apis/batch/v1", "apis/networking.k8s.io/v1", "apis/autoscaling/v2", "apis/policy/v1", "apis/discovery.k8s.io/v1", "apis/coordination.k8s.io/v1"} {
		entry, ok := index.Paths[group]
		if !ok {
			t.Fatalf("openapi v3 index lacks %s", group)
		}
		raw, err := b.rest.Get().AbsPath(strings.SplitN(entry.URL, "?", 2)[0]).Param("hash", strings.TrimPrefix(entry.URL[strings.Index(entry.URL, "?hash="):], "?hash=")).SetHeader("Accept", "application/json").Do(ctx).Raw()
		if err != nil {
			t.Fatal(err)
		}
		var part struct {
			Components struct {
				Schemas map[string]map[string]any `json:"schemas"`
			} `json:"components"`
		}
		if err := json.Unmarshal([]byte(strings.ReplaceAll(string(raw), "#/components/schemas/", "#/definitions/")), &part); err != nil {
			t.Fatal(err)
		}
		for name, def := range part.Components.Schemas {
			doc.Definitions[name] = def
		}
	}
	keep := map[string]map[string]any{}
	var visit func(name string)
	var strip func(v any)
	strip = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			delete(x, "description")
			for k, e := range x {
				if k == "$ref" {
					if s, ok := e.(string); ok {
						visit(strings.TrimPrefix(s, "#/definitions/"))
					}
				}
				strip(e)
			}
		case []any:
			for _, e := range x {
				strip(e)
			}
		}
	}
	visit = func(name string) {
		if _, ok := keep[name]; ok {
			return
		}
		def, ok := doc.Definitions[name]
		if !ok {
			t.Fatalf("definition %s missing", name)
		}
		keep[name] = def
		strip(def)
	}
	for _, root := range schemaRoots {
		visit("io.k8s.api." + root)
	}
	names := make([]string, 0, len(keep))
	for n := range keep {
		names = append(names, n)
	}
	sort.Strings(names)
	t.Logf("%d definitions", len(names))
	out, err := json.Marshal(map[string]any{"definitions": keep})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("testdata/openapi-subset.json", out, 0o644); err != nil {
		t.Fatal(err)
	}
}
