package runtime

import (
	"os"
	"strings"
	"testing"
)

// Every holder of the observation lock must go through lockObservation, which
// logs a hold long enough to stop detection. A direct Lock would hide the next
// closure-class stall from the logs.
func TestObservationLockIsTakenOnlyThroughLockObservation(t *testing.T) {
	paths, err := globAll("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") || path == "closure_evidence.go" {
			continue
		}
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(source), "observationMu.Lock()") {
			t.Errorf("%s takes observationMu directly; use lockObservation(holder)", path)
		}
	}
}
