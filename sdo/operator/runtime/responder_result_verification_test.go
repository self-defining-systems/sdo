package runtime

import (
	"context"
	"testing"
)

func TestResultVerificationRunsTheControllerCheckAlone(t *testing.T) {
	loop := NewResponderLoop(detectRequest(), nil, &detectWorld{}, nil, PlanExecutor{}, 10)
	closureChecks := 0
	loop.VerifyClosure = func(context.Context) ToolResponse {
		closureChecks++
		return ToolResponse{Command: "verify", Healthy: true}
	}
	resultChecks := 0
	loop.VerifyResult = func(context.Context, IncidentResult) ToolResponse {
		resultChecks++
		return ToolResponse{Command: "verify", Healthy: true}
	}
	if response := resultVerification(context.Background(), loop, IncidentResult{}); !response.Healthy {
		t.Fatalf("verification = %+v, want healthy", response)
	}
	if resultChecks != 1 || closureChecks != 0 {
		t.Fatalf("result check ran %d times and the tool read %d times, want 1 and 0 (a second capture only repeats the first)", resultChecks, closureChecks)
	}
}
