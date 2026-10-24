package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

// This is an offline startup probe: stop immediately after policy loading and
// before Kubernetes setup, so launch preparation needs no cluster or model.
func TestControllerLogsLoadedNonemptySourcePlanPolicyBeforeClusterAccess(t *testing.T) {
	raw, err := os.ReadFile("../config/plan-policies/mixed10.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("SDO_TIME_SCALE", "offline-policy-probe")
	var output bytes.Buffer
	err = RunWithOptions(context.Background(), nil, RuntimeOptions{Args: []string{"--plan-source-policy-json", string(raw)}, Stdout: &output, Stderr: &output})
	if err == nil || !strings.Contains(err.Error(), "SDO_TIME_SCALE") {
		t.Fatalf("offline probe did not stop before Kubernetes setup: %v", err)
	}
	var event struct {
		Loaded       bool   `json:"plan_source_policy_loaded"`
		Applications int    `json:"applications"`
		CreateKinds  int    `json:"create_kinds"`
		DeleteKinds  int    `json:"delete_kinds"`
		SHA256       string `json:"sha256"`
	}
	if err := json.Unmarshal(output.Bytes(), &event); err != nil || !event.Loaded || event.Applications != 4 || event.CreateKinds == 0 || event.DeleteKinds == 0 || event.SHA256 != fmt.Sprintf("%x", sha256.Sum256(raw)) {
		t.Fatalf("controller did not log its loaded non-empty reviewed policy: %s (%v)", output.String(), err)
	}
	t.Log("controller_policy_startup", strings.TrimSpace(output.String()))
}

func TestControllerDoesNotLogPolicyLoadedWithoutValidAuthority(t *testing.T) {
	for _, raw := range []string{"", `{}`, `{"authority":"accepted-source-and-incident","applications":{}}`} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv("SDO_TIME_SCALE", "offline-policy-probe")
			var output bytes.Buffer
			args := []string{}
			if raw != "" {
				args = []string{"--plan-source-policy-json", raw}
			}
			if err := RunWithOptions(context.Background(), nil, RuntimeOptions{Args: args, Stdout: &output, Stderr: &output}); err == nil {
				t.Fatal("invalid/offline configuration started")
			}
			if strings.Contains(output.String(), "plan_source_policy_loaded") {
				t.Fatal("unconfigured/invalid policy logged successful authority load")
			}
		})
	}
}
