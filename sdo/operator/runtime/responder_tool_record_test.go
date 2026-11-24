package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/sdk/program"
)

func TestRejectedSecretToolCallDoesNotPersistPayload(t *testing.T) {
	const requestCanary = "synthetic-request-canary"
	body := `{"apiVersion":"v1","kind":"Secret","metadata":{"namespace":"app","name":"creds"},"stringData":{"password":"` + requestCanary + `"}}`
	_, loop := targetLoop(t)
	request := ToolRequest{Command: "mitigate", Targets: []MitigateTarget{{Name: "ApplyObject", Parameters: map[string]BindValues{
		"ref":         {`{"apiVersion":"v1","kind":"Secret","namespace":"app","name":"creds"}`},
		"object_json": {body}, "reason": {"synthetic test"},
	}}}}
	// Two named options route through the batch planner's real refusal path.
	request.Targets = append(request.Targets, request.Targets[0])
	if response := loop.Handle(context.Background(), request); !strings.Contains(response.Error, "Secret data is unavailable") {
		t.Fatalf("expected Secret validation refusal, got %q", response.Error)
	}
	calls := loop.ToolCalls()
	if len(calls) != 1 || calls[0].Error == "" {
		t.Fatalf("expected one rejected call, got %d", len(calls))
	}
	recorded, _ := json.Marshal(calls[0])
	result, err := incidentResultContract(IncidentResult{ProgramToolCalls: calls})
	if err != nil {
		t.Fatal(err)
	}
	published, err := protojson.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{recorded, published} {
		if strings.Contains(string(data), requestCanary) {
			t.Fatal("tool call evidence retained credential material")
		}
	}
}

func TestErroredAutomaticApplyKeepsValueFreeReceipt(t *testing.T) {
	const canary = "synthetic-error-canary"
	loop := &ResponderLoop{}
	loop.recordToolCall(ToolRequest{Command: "apply", PlanIDs: []string{"plan-1"}, stage: true}, ToolResponse{
		Command: "apply", ApplicationID: "application-1", Error: canary, Summary: canary,
		ProgramRuns: []ProgramRun{{ProgramID: "repair", Verdict: program.Verdict{
			Result: program.Unknown, Evidence: []program.Evidence{{Observed: canary}},
		}}},
	}, nil)
	call := loop.ToolCalls()[0]
	if !strings.Contains(call.RequestJSON, `"stage":true`) {
		t.Fatal("automatic stage attribution was lost")
	}
	var response ToolResponse
	if err := json.Unmarshal([]byte(call.ResponseJSON), &response); err != nil {
		t.Fatal(err)
	}
	if response.ApplicationID != "application-1" || len(response.ProgramRuns) != 1 || response.ProgramRuns[0].Verdict.Result != program.Unknown {
		t.Fatal("partial application receipt was lost")
	}
	if strings.Contains(call.ResponseJSON+call.Error+call.Summary, canary) {
		t.Fatal("errored receipt contained arbitrary error or evidence text")
	}
}
