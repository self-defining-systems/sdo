package memory

import (
	"os"
	"path/filepath"
	"sdo.dev/operator/sdk/servicehealth"
	"strings"
	"testing"
	"time"
)

func stateBaseline() *servicehealth.ReachabilityBaseline {
	return &servicehealth.ReachabilityBaseline{Version: 1, Authorization: servicehealth.BaselineAuthorization{SourceInputs: strings.Repeat("a", 64), ObjectiveDigest: strings.Repeat("b", 64), Reason: "healthy-baseline", GoalHealthy: true}, CapturedAt: time.Now().UTC(), Exclusions: []servicehealth.ReachabilityExclusion{}}
}

// The reachability capture is per-cluster controller state: it round-trips
// below the state root, outside any operational-memory tree.
func TestReachabilityBaselineIsClusterStateOutsideMemory(t *testing.T) {
	state := t.TempDir()
	if b, err := ReadReachabilityBaselineState(state); err != nil || b != nil {
		t.Fatalf("missing state must read as no record: %+v %v", b, err)
	}
	want := stateBaseline()
	if err := WriteReachabilityBaselineState(state, want); err != nil {
		t.Fatal(err)
	}
	path := ReachabilityBaselineStatePath(state)
	if rel, _ := filepath.Rel(state, path); strings.HasPrefix(rel, ".sdo/") || strings.HasPrefix(rel, "..") {
		t.Fatalf("state path %s is not private controller state", rel)
	}
	got, err := ReadReachabilityBaselineState(state)
	if err != nil || got == nil || !got.CapturedAt.Equal(want.CapturedAt) {
		t.Fatalf("state did not round-trip: %+v %v", got, err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"authorization":{"goalHealthy":false}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadReachabilityBaselineState(state); err == nil {
		t.Fatal("forged state accepted")
	}
}

// Memories written before the move keep working: health preparation strips the
// legacy capture, valid or not, and never requires it.
func TestGenericHealthStripsLegacyReachabilityBaseline(t *testing.T) {
	for name, content := range map[string]string{
		"valid":     `{"version":1,"authorization":{"sourceInputs":"` + strings.Repeat("a", 64) + `","objectiveDigest":"` + strings.Repeat("b", 64) + `","reason":"healthy-baseline","goalHealthy":true},"capturedAt":"2026-10-28T16:19:23Z","exclusions":[]}`,
		"malformed": `{"version":1,"authorization":{"goalHealthy":false}}`,
	} {
		t.Run(name, func(t *testing.T) {
			root := preparedHealthRoot(t)
			legacy := filepath.Join(root, LegacyReachabilityBaselinePath)
			if err := os.WriteFile(legacy, []byte(content+"\n"), 0644); err != nil {
				t.Fatal(err)
			}
			if err := PrepareHealth(root); err != nil {
				t.Fatalf("legacy capture blocked health preparation: %v", err)
			}
			if _, err := os.Stat(legacy); !os.IsNotExist(err) {
				t.Fatalf("legacy capture kept in memory: %v", err)
			}
		})
	}
}
