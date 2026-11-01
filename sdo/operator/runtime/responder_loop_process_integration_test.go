//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

func TestColdLearningCapturesResponderSourceReads(t *testing.T) {
	source, commit := foreignSource(t)
	snapshot := policyWorkloadSnapshot()
	request := IncidentRequest{IncidentID: "cold-learning", Namespace: "app", SourceCommit: commit}
	loop := NewResponderLoop(request, nil, &changingProvider{snapshot: snapshot}, nil, PlanExecutor{Source: source}, 1)
	loop.CorpusSessionRoot = t.TempDir()
	loop.ReadView = func(s sdk.DetectionContext) program.ReadView {
		return LiveReadView{Carrier: inputs.New(s), Source: source, SourceCommit: commit}
	}
	loop.InitialDiagnose(context.Background())
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	if response := loop.Handle(context.Background(), ToolRequest{Command: "source", Ref: &ref}); response.Error != "" {
		t.Fatal(response.Error)
	}
	dir, err := memory.IncidentCorpusDir(loop.CorpusSessionRoot, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(raw, &calls); err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}
	if _, err := view.DiffAgainstSource(ref); err != nil {
		t.Fatalf("cold source inspection was not replayable: %v", err)
	}
}
