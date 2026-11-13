package runtime

import (
	"context"
	"testing"
	"time"
)

func verifyWaitLoop(receipts []RepairActionReceipt, pending bool) (*ResponderLoop, *[]time.Duration) {
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident"}, nil, nil, nil, PlanExecutor{}, 3)
	waits := []time.Duration{}
	loop.VerifyClosure = func(context.Context) ToolResponse { return ToolResponse{Command: "verify", Summary: "cached"} }
	loop.WaitClosure = func(_ context.Context, wait time.Duration) ToolResponse {
		waits = append(waits, wait)
		return ToolResponse{Command: "verify", Summary: "waited"}
	}
	loop.ExternalReceipts = func() ([]RepairActionReceipt, bool, string, error) { return receipts, pending, "r", nil }
	return loop, &waits
}

// Without a recorded write since the incident opened there is nothing to
// settle: --wait returns the current status at once.
func TestVerifyWaitReturnsImmediatelyWithoutAgentWrites(t *testing.T) {
	loop, waits := verifyWaitLoop(nil, false)
	response := loop.Handle(context.Background(), ToolRequest{Command: "verify", WaitSeconds: 60})
	if len(*waits) != 0 || response.Summary != "cached" {
		t.Fatalf("verify --wait blocked with no recorded write: waits=%v response=%+v", *waits, response)
	}
}

func TestVerifyWaitStillWaitsAfterAgentWrites(t *testing.T) {
	loop, waits := verifyWaitLoop([]RepairActionReceipt{{}}, false)
	loop.Handle(context.Background(), ToolRequest{Command: "verify", WaitSeconds: 60})
	if len(*waits) != 1 {
		t.Fatalf("verify --wait did not wait after a recorded write: %v", *waits)
	}
}

func TestVerifyWaitStillWaitsWhenTheRecordingIsPending(t *testing.T) {
	loop, waits := verifyWaitLoop(nil, true)
	loop.Handle(context.Background(), ToolRequest{Command: "verify", WaitSeconds: 60})
	if len(*waits) != 1 {
		t.Fatalf("verify --wait skipped the wait while the recording is pending: %v", *waits)
	}
}

// With agent.recording off there is no ExternalReceipts and so no way to know the agent wrote nothing: --wait
// falls back to waiting for the settle instead of skipping it, and nothing errors on the missing recording.
func TestVerifyWaitStillWaitsWithNoRecording(t *testing.T) {
	loop, waits := verifyWaitLoop(nil, false)
	loop.ExternalReceipts = nil
	loop.Handle(context.Background(), ToolRequest{Command: "verify", WaitSeconds: 60})
	if len(*waits) != 1 {
		t.Fatalf("verify --wait skipped the settle with no recording: %v", *waits)
	}
}
