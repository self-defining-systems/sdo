package validate

import (
	"context"
	"strings"
	"testing"
)

// The generated traffic test produces the notes and the compile layer turns
// them into non-blocking coverage gaps: a read-only health probe leaves the
// checkout -> payment edge undriven and keeps the default latency bound,
// which hides a saturated service; a scenario with its own median bound is
// not noted.
func TestCompileNotesTrafficCoverageWithoutRejecting(t *testing.T) {
	root := compileTree(t)
	write(t, root, ".sdo/diagnostics/traffic/generators/shop.go", `package generators

import "sdo.dev/operator/sdk/traffic"

func Scenarios() traffic.Catalog {
	read := func(id, path string) traffic.Scenario {
		return traffic.Scenario{ID: id, Target: traffic.Target{Service: "frontend", Port: 8080},
			DependsOn: []string{"frontend", "catalog"}, SideEffect: traffic.SideEffectRead,
			Steps: []traffic.Step{{Name: "get", Endpoint: traffic.GET(path, nil)}}}
	}
	return traffic.Catalog{read("browse", "/products"), read("ads", "/ads")}
}
`)
	write(t, root, ".sdo/diagnostics/traffic/workloads/health.yaml", `apiVersion: sdo.dev/v1alpha1
kind: TrafficWorkload
name: health
purpose: health-probe
scenarios:
  - id: browse
  - id: ads
    slo: {latencyPercentile: 50, maxLatency: 150ms}
`)
	write(t, root, ".sdo/diagnostics/traffic/workloads/links.yaml", `apiVersion: sdo.dev/v1alpha1
kind: TrafficWorkload
name: links
purpose: link-probe
links:
  - {from: frontend, to: catalog, port: 8080}
  - {from: checkout, to: payment, port: 8080}
`)
	var gaps []Diagnostic
	request := compileRequest(root)
	request.CoverageGaps = &gaps
	if ds, err := CompileCandidate(context.Background(), request); err != nil || len(ds) != 0 {
		t.Fatalf("traffic coverage notes must not reject memory: %v %v", ds, err)
	}
	notes := map[string][]string{}
	for _, gap := range gaps {
		if gap.Path != ".sdo/diagnostics/traffic/workloads" {
			continue
		}
		notes[gap.Rule] = append(notes[gap.Rule], gap.Message)
	}
	if edges := notes[RuleTrafficUndrivenEdge]; len(edges) != 1 || !strings.Contains(edges[0], "checkout -> payment:8080") {
		t.Fatalf("want the undriven checkout -> payment edge, got %q (all %v)", edges, gaps)
	}
	if bounds := notes[RuleTrafficDefaultLatency]; len(bounds) != 1 || !strings.Contains(bounds[0], "scenario browse ") {
		t.Fatalf("want only browse on the default latency bound, got %q (all %v)", bounds, gaps)
	}
}
