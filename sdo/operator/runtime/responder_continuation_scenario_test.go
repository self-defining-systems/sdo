package runtime

import (
	"context"
	"testing"
)

func TestContinuationRepairsSurviveRestartAndDeduplicateReceipts(t *testing.T) {
	request := goldenRequest(t)
	loop := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 3)
	result := completedResult(request.IncidentID)
	result.RepairActions = []RepairActionReceipt{{ActionID: "live-action", Kind: "kubectl", Summary: "recorded repair", Success: true}}
	repairs := loop.continuationRepairs(result)
	if len(repairs) != 1 || repairs[0].Kind != "recorded-action" || repairs[0].Receipt.ActionID != "live-action" {
		t.Fatalf("missing live receipt: %+v", repairs)
	}
	encoded, err := continuationFeedback(1, ToolResponse{Summary: "third remains"}, repairs...)
	if err != nil {
		t.Fatal(err)
	}
	message, err := incidentResultContract(result)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	loop.saveToolState = func(_ context.Context, payload []byte) error { persisted = append([]byte(nil), payload...); return nil }
	if err = loop.saveContinuation(context.Background(), responderContinuation{Attempts: 1, Feedback: string(encoded), Result: message}); err != nil {
		t.Fatal(err)
	}
	restored := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 3)
	if err = restored.restoreToolSnapshot(persisted); err != nil {
		t.Fatal(err)
	}
	// A resumed provider may omit old receipts; the continuation still retains them.
	next := restored.continuationRepairs(IncidentResult{})
	if len(next) != 1 || next[0].ActionID != "live-action" {
		t.Fatalf("restart lost completed repair: %+v", next)
	}
	next = restored.continuationRepairs(result)
	if len(next) != 1 {
		t.Fatalf("duplicate resumed receipt: %+v", next)
	}
}
