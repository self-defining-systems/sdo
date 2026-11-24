package validate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

// writeGenericDetector installs a synthetic health detector package under id,
// as Go's bootstrap does before the health judge runs.
func writeGenericDetector(t *testing.T, root, id string) {
	t.Helper()
	dir := filepath.Join(".sdo", "diagnostics", "detectors", "health")
	manifest, err := os.ReadFile(filepath.Join(root, dir, "health", "manifest.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	write(t, root, filepath.Join(dir, id, "manifest.yaml"), strings.Replace(string(manifest), "id: health", "id: "+id, 1))
	write(t, root, filepath.Join(dir, id, "detector.go"), "package "+strings.ReplaceAll(id, "-", "")+"\n")
}

func judgeInitialOwnership(t *testing.T, root string) []string {
	t.Helper()
	request := Request{Root: root, Base: "HEAD", Actor: HealthJudge, Phase: InitialPhase, Layers: Ownership}
	return ruleSet(diagnose(t, request))
}

// The initial proposal is judged on the tree it produces: Go's generic detectors
// already in the base satisfy the rule when the judge's diff holds only the map
// and traffic files, and a probe the map declares needs its traffic detector.
func TestInitialNeedsDetectorsEvaluatesTheResultingTree(t *testing.T) {
	probeDetectors := []string{"traffic-probe-call", "traffic-probe-write", "traffic-probe-read-later"}
	judgeDiff := func(t *testing.T, root, purpose, skip string) {
		write(t, root, layout.ArchitectureFile, mapInventory+"\n"+mapRegion(mapYAML))
		dir := ".sdo/diagnostics/traffic/workloads/"
		for _, w := range [][2]string{{"probe-call", "call-all"}, {"probe-write", "write-item"}, {"probe-read-later", "items-later"}} {
			if w[0] != skip {
				write(t, root, dir+w[0]+".yaml", strings.Replace(mapWorkload(w[0], w[1]), "purpose: health-probe", "purpose: "+purpose, 1))
			}
		}
	}
	cases := []struct {
		name      string
		base      []string
		removeAll bool
		purpose   string
		want      []string
	}{
		{name: "generic detectors in the base satisfy the rule", base: probeDetectors},
		// Go installs traffic-<workload> when it prepares the tree, after the
		// judge's check and before publication; the rule counts those.
		{name: "traffic detectors Go has not installed yet", base: []string{"health"}},
		{name: "a declared probe workload Go installs no detector for", base: []string{"health"}, purpose: "incident-probe", want: []string{RuleInitialNeedsDetectors}},
		// A fresh onboarding base holds no health detector at all: Go installs
		// the generic ones when it prepares the tree, so the rule must not
		// demand the judge write them.
		{name: "fresh onboarding base with no detectors", removeAll: true},
		{name: "fresh base and a declared probe Go installs no detector for", removeAll: true, purpose: "incident-probe", want: []string{RuleInitialNeedsDetectors}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := goodTree(t)
			if tc.removeAll {
				remove(t, root, ".sdo/diagnostics/detectors")
			}
			for _, id := range tc.base {
				writeGenericDetector(t, root, id)
			}
			write(t, root, layout.ArchitectureFile, mapInventory)
			commit(t, root, "base")
			purpose := tc.purpose
			if purpose == "" {
				purpose = "health-probe"
			}
			judgeDiff(t, root, purpose, "")
			if got := judgeInitialOwnership(t, root); !same(got, tc.want) {
				t.Fatalf("rules = %v, want %v", got, tc.want)
			}
		})
	}
}

// A declared probe whose workload file is missing still fails validation: the
// application map rule reports it, and Go cannot prepare a detector for it.
func TestInitialProposalWithoutADeclaredProbeWorkloadFails(t *testing.T) {
	root := goodTree(t)
	writeGenericDetector(t, root, "health")
	write(t, root, layout.ArchitectureFile, mapInventory)
	commit(t, root, "base")
	write(t, root, layout.ArchitectureFile, mapInventory+"\n"+mapRegion(mapYAML))
	dir := ".sdo/diagnostics/traffic/workloads/"
	write(t, root, dir+"probe-call.yaml", mapWorkload("probe-call", "call-all"))
	write(t, root, dir+"probe-write.yaml", mapWorkload("probe-write", "write-item"))
	request := Request{Root: root, Base: "HEAD", Actor: HealthJudge, Phase: InitialPhase, Layers: Static}
	if got := ruleSet(diagnose(t, request)); len(got) == 0 {
		t.Fatal("a map probe without its workload file passed validation")
	}
}

func TestUpkeepAllowsOnlyTheGeneratedRequestMetricsRefresh(t *testing.T) {
	const entry = `entries:
  - {service: web, metric: http_requests_total, labels: {route: route, status: code}, errors: "5.."}
`
	initialMap := mapYAML + entry
	root := mapTree(t, &initialMap)
	if err := memory.PrepareHealthWith(root, memory.HealthOptions{RequestMetrics: true}); err != nil {
		t.Fatal(err)
	}
	// Keep Go's foreground generation, but commit the legacy job map it
	// would have been generated for before the policy recognized job metrics.
	jobMap := strings.Replace(initialMap, "http_requests_total", "jobs_total", 1)
	write(t, root, layout.ArchitectureFile, mapInventory+"\n"+mapRegion(jobMap))
	commit(t, root, "legacy request-metrics policy")
	if err := memory.WriteSchema(root); err != nil {
		t.Fatal(err)
	}
	req := Request{Root: root, Base: "HEAD", Actor: Upkeep, Phase: InitialPhase, Layers: Ownership}
	if got := ruleSet(diagnose(t, req)); len(got) != 0 {
		t.Fatalf("Upkeep denied an exact policy refresh: %v", got)
	}
	path := layout.DetectorDir(layout.ClassHealth, memory.RequestMetricsDetectorID) + "/" + layout.DetectorFileName
	edit(t, root, path, "Background: true", "Background: false")
	if got := ruleSet(diagnose(t, req)); !same(got, []string{RuleUpkeepMigration}) {
		t.Fatalf("Upkeep allowed a manual change to the generated policy: %v", got)
	}
}
