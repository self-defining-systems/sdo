package builder

import (
	"strings"
	"testing"
)

// The generated consume check must say which file is missing and which Go-owned
// package is stale; "not a health-probe workload" gave the judge nothing to fix.
func TestGeneratedConsumeCheckNamesMissingWorkloadAndStalePackage(t *testing.T) {
	for _, want := range []string{"workloads/%s.yaml does not exist", "stale package", "detectors/health/%s"} {
		if !strings.Contains(trafficTest, want) {
			t.Errorf("generated traffic test lacks %q", want)
		}
	}
}
