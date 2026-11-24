package sdk_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// An opener that judges a pod-backed workload "not ready" after the self-heal
// hold must also ask whether the pod is still inside its own startup
// (StillStarting or PodStarting); the hold alone is shorter than many
// workloads' declared startup.
//
//ci:whole-tree
func TestReadinessOpenersConsultTheStartupAllowance(t *testing.T) {
	tokens := []string{"ReadyReplicas", "NumberReady", "ContainersReady"}
	checked := 0
	for _, dir := range []string{"workloadhealth", "generichealth"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			src := string(raw)
			judges := false
			for _, token := range tokens {
				judges = judges || strings.Contains(src, token)
			}
			if !judges || !strings.Contains(src, "HeldPastSelfHeal") && !strings.Contains(src, ".held(") {
				continue
			}
			checked++
			if !strings.Contains(src, "StillStarting") && !strings.Contains(src, "PodStarting") && !strings.Contains(src, "workloadreadiness.Deficit(") {
				t.Errorf("%s judges readiness after the self-heal hold without the startup allowance (sdk.StillStarting or sdk.PodStarting)", file)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no readiness opener found; the guard checks nothing")
	}
}
