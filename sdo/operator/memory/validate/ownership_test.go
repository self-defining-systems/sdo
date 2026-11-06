package validate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	judgeDiff := func(t *testing.T, root string) {
		write(t, root, layout.ArchitectureFile, mapInventory+"\n"+mapRegion(mapYAML))
		dir := ".sdo/diagnostics/traffic/workloads/"
		write(t, root, dir+"probe-call.yaml", mapWorkload("probe-call", "call-all"))
		write(t, root, dir+"probe-write.yaml", mapWorkload("probe-write", "write-item"))
		write(t, root, dir+"probe-read-later.yaml", mapWorkload("probe-read-later", "items-later"))
	}
	cases := []struct {
		name      string
		base      []string
		removeAll bool
		want      []string
	}{
		{name: "generic detectors in the base satisfy the rule", base: probeDetectors},
		{name: "a declared probe without its traffic detector", base: probeDetectors[:1], want: []string{RuleInitialNeedsDetectors}},
		{name: "no detectors in the resulting tree", removeAll: true, want: []string{RuleInitialNeedsDetectors}},
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
			judgeDiff(t, root)
			if got := judgeInitialOwnership(t, root); !same(got, tc.want) {
				t.Fatalf("rules = %v, want %v", got, tc.want)
			}
		})
	}
}
