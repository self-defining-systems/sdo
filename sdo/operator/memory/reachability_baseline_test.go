package memory

import (
	"os"
	"path/filepath"
	"sdo.dev/operator/sdk/servicehealth"
	"strings"
	"testing"
	"time"
)

func TestGenericHealthAllowsOnlyValidatedReachabilityBaseline(t *testing.T) {
	root := preparedHealthRoot(t)
	baseline := &servicehealth.ReachabilityBaseline{Version: 1, Authorization: servicehealth.BaselineAuthorization{SourceInputs: strings.Repeat("a", 64), ObjectiveDigest: strings.Repeat("b", 64), Reason: "healthy-baseline", GoalHealthy: true}, CapturedAt: time.Now().UTC(), Exclusions: []servicehealth.ReachabilityExclusion{}}
	if err := WriteReachabilityBaseline(root, baseline); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealth(root); err != nil {
		t.Fatalf("valid visible health diagnostic rejected: %v", err)
	}
	path := filepath.Join(root, ReachabilityBaselinePath)
	if err := os.WriteFile(path, []byte(`{"version":1,"authorization":{"goalHealthy":false}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealth(root); err == nil {
		t.Fatal("forged baseline accepted")
	}
}
