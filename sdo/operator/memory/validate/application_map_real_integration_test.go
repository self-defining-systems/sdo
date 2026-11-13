//go:build integration

package validate

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

const mapGenerators = `package generators

import "sdo.dev/operator/sdk/traffic"

var web = traffic.Target{Service: "web", Port: 80}

func Scenarios() traffic.Catalog {
	read := func(id string, depends ...string) traffic.Scenario {
		return traffic.Scenario{
			ID: id, Description: "read items", Target: web, DependsOn: depends, SideEffect: traffic.SideEffectRead,
			Detects: []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},
			Steps:   []traffic.Step{{Name: "list", Endpoint: traffic.GET("/items", traffic.Params{}).Contains("items")}},
		}
	}
	write := traffic.Scenario{
		ID: "write-item", Description: "add an item", Target: web, DependsOn: []string{"web", "api", "db"},
		SideEffect: traffic.SideEffectIdempotentWrite, Marker: "sdo-synthetic",
		Detects: []traffic.FaultClass{traffic.FaultWrongBody},
		Steps:   []traffic.Step{{Name: "add", Endpoint: traffic.POST("/items", traffic.Params{"name": traffic.Const("sdo-synthetic")}).Contains("ok")}},
	}
	later := read("items-later", "web", "api", "db")
	later.Marker = "sdo-synthetic"
	later.Steps = []traffic.Step{{Name: "list", Endpoint: traffic.GET("/items", traffic.Params{}).ReadsExistingRecords("items", "name", "^.+$")}}
	later.Setup = []traffic.Step{{Name: "ensure", Endpoint: traffic.POST("/items", traffic.Params{"name": traffic.Const("sdo-synthetic-fixture")}).OrExisting(409, "exists")}}
	return traffic.Catalog{read("call-all", "web", "api", "db"), write, later}
}
`

// mapCompileTree is the compile fixture with the shop's map, workloads and generators.
func mapCompileTree(t *testing.T, dependsOnFix string) string {
	root := compileTree(t)
	write(t, root, layout.ArchitectureFile, mapInventory+"\n"+mapRegion(mapYAML))
	write(t, root, ".sdo/diagnostics/traffic/generators/generators.go", strings.Replace(mapGenerators, `read("call-all", "web", "api", "db")`, dependsOnFix, 1))
	dir := ".sdo/diagnostics/traffic/workloads/"
	write(t, root, dir+"probe-call.yaml", mapWorkload("probe-call", "call-all"))
	write(t, root, dir+"probe-write.yaml", mapWorkload("probe-write", "write-item"))
	write(t, root, dir+"probe-read-later.yaml", mapWorkload("probe-read-later", "items-later"))
	return root
}

func TestApplicationMapAgainstCompiledScenarios(t *testing.T) {
	ctx := context.Background()
	t.Run("matching scenarios pass", func(t *testing.T) {
		t.Parallel()
		root := mapCompileTree(t, `read("call-all", "web", "api", "db")`)
		request := compileRequest(root)
		request.Actor, request.Phase = HealthJudge, InitialPhase
		got, err := CompileCandidate(ctx, request)
		if err != nil || len(got) != 0 {
			t.Fatalf("rejected: %v %v", got, err)
		}
	})
	t.Run("DependsOn missing a component blocks the designer and notes for others", func(t *testing.T) {
		t.Parallel()
		root := mapCompileTree(t, `read("call-all", "web", "api")`)
		request := compileRequest(root)
		request.Actor, request.Phase = HealthJudge, InitialPhase
		got, err := CompileCandidate(ctx, request)
		if err != nil || len(got) != 1 || got[0].Rule != RuleMapPathMismatch || !strings.Contains(got[0].Message, "component db") || got[0].Path != layout.ArchitectureFile {
			t.Fatalf("designer: %v %v", got, err)
		}
		var notes []Diagnostic
		request.Actor, request.Phase, request.CoverageGaps = Responder, ReflectionPhase, &notes
		got, err = CompileCandidate(ctx, request)
		if err != nil || len(got) != 0 {
			t.Fatalf("responder was rejected: %v %v", got, err)
		}
		if !slices.ContainsFunc(notes, func(d Diagnostic) bool { return d.Rule == RuleMapPathMismatch }) {
			t.Fatalf("no map.path-mismatch note: %v", notes)
		}
	})
	t.Run("a probe that checks only the status blocks the designer", func(t *testing.T) {
		t.Parallel()
		root := mapCompileTree(t, `func() traffic.Scenario { s := read("call-all", "web", "api", "db"); s.Detects = nil; s.Steps = []traffic.Step{{Name: "list", Endpoint: traffic.GET("/items", traffic.Params{})}}; return s }()`)
		request := compileRequest(root)
		request.Actor, request.Phase = HealthJudge, InitialPhase
		got, err := CompileCandidate(ctx, request)
		if err != nil || len(got) != 1 || got[0].Rule != RuleMapSchema || !strings.Contains(got[0].Message, "only the status") {
			t.Fatalf("designer: %v %v", got, err)
		}
	})
	t.Run("an unmarked records read blocks the designer", func(t *testing.T) {
		t.Parallel()
		root := mapCompileTree(t, `read("call-all", "web", "api", "db")`)
		path := filepath.Join(root, ".sdo/diagnostics/traffic/generators/generators.go")
		text, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		unmarked := strings.Replace(string(text), `.ReadsExistingRecords("items", "name", "^.+$")`, `.Contains("items")`, 1)
		if unmarked == string(text) {
			t.Fatal("the generators carry no marked step")
		}
		write(t, root, ".sdo/diagnostics/traffic/generators/generators.go", unmarked)
		request := compileRequest(root)
		request.Actor, request.Phase = HealthJudge, InitialPhase
		got, err := CompileCandidate(ctx, request)
		if err != nil || len(got) != 1 || got[0].Rule != RuleMapRecordsUnread {
			t.Fatalf("designer: %v %v", got, err)
		}
	})
	// Go's client-links detector, installed from the map and the link-probe
	// workload, compiles and loads in the controller with the designer's tree.
	t.Run("client links compile and load", func(t *testing.T) {
		t.Parallel()
		root := mapCompileTree(t, `read("call-all", "web", "api", "db")`)
		write(t, root, ".sdo/diagnostics/traffic/workloads/links.yaml", "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: links\npurpose: link-probe\nlinks:\n  - {from: api, to: db, port: 5432, protocol: postgres}\n  - {from: web, to: api, port: 8080, protocol: http, http: {path: /health}}\n")
		if err := memory.PrepareHealth(root); err != nil {
			t.Fatal(err)
		}
		if !detectorInstalled(root, memory.ClientLinksDetectorID) {
			t.Fatal("client-links was not installed")
		}
		request := compileRequest(root)
		request.Actor, request.Phase = HealthJudge, InitialPhase
		got, err := CompileCandidate(ctx, request)
		if err != nil || len(got) != 0 {
			t.Fatalf("rejected: %v %v", got, err)
		}
	})
}

func detectorInstalled(root, id string) bool {
	_, err := os.Stat(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, id), layout.DetectorFileName))
	return err == nil
}
