//go:build integration

package validate

import (
	"context"
	"slices"
	"strings"
	"testing"

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
	return traffic.Catalog{read("call-all", "web", "api", "db"), write, read("items-later", "web", "api", "db")}
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
}
