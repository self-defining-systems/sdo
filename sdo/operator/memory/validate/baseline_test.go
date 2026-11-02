package validate

import "testing"

func TestBaselineGateIncludesExecutableMemoryDependencies(t *testing.T) {
	root := goodTree(t)
	for _, test := range []struct {
		path string
		want bool
	}{
		{".sdo/diagnostics/detectors/incidents/cache/detector.go", true},
		{".sdo/playbooks/base/diagnose.go", true},
		{".sdo/playbooks/base/mitigate.go", true},
		{".sdo/lib/ports/ports.go", true},
		{".sdo/diagnostics/detectors/health/health/detector.go", false},
		{".sdo/playbooks/base/README.md", false},
		{".sdo/playbooks/base/scripts/fix.sh", false},
	} {
		t.Run(test.path, func(t *testing.T) {
			got, err := BaselineGateRequired(root, []string{test.path})
			if err != nil || got != test.want {
				t.Fatalf("healthy replay gate for %s=%v, %v; want %v", test.path, got, err, test.want)
			}
		})
	}
}
