//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"time"
)

func TestRunScriptFailureNeverAutomaticallyUndoesAndOutputStaysPrivate(t *testing.T) {
	client := &scriptPlanFake{failed: true}
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	plan := program.Plan{Actions: []program.Action{program.RunScript{Script: "exit 1"}}}
	result, err := executor.Apply(context.Background(), IncidentRequest{IncidentID: "failure", Namespace: "app", ResponseDeadline: time.Now().Add(2 * time.Second)}, plan, false)
	if err == nil || !result.Applied || result.RolledBack || result.RollbackError != "" || client.request.Timeout > 2*time.Second {
		t.Fatalf("script failure/budget/undo: %+v %v", result, err)
	}
	receipt, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(receipt)
	if strings.Contains(string(raw), "private-output-canary") {
		t.Fatal("private script output reached the durable outcome")
	}
	if result.ActionReports[0].Script.StdoutBytes == 0 || result.ActionReports[0].Script.ResultRef == "" {
		t.Fatal("private output reference missing")
	}
}

