package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/sdk/program"
)

func TestRejectedSecretToolCallDoesNotPersistPayload(t *testing.T) {
	for _, count := range []int{1, 2} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			const canary = "synthetic-request-canary"
			body := `{"apiVersion":"v1","kind":"Secret","metadata":{"namespace":"app","name":"creds"},"stringData":{"password":"` + canary + `"}}`
			_, loop := targetLoop(t)
			target := MitigateTarget{Name: "ApplyObject", Parameters: map[string]BindValues{
				"ref":         {`{"apiVersion":"v1","kind":"Secret","namespace":"app","name":"creds"}`},
				"object_json": {body}, "reason": {"restore expected credentials"},
			}}
			request := ToolRequest{Command: "mitigate", Targets: []MitigateTarget{target}}
			if count == 2 {
				request.Targets = append(request.Targets, target)
			}
			response := loop.Handle(context.Background(), request)
			diagnostic := response.Error
			if count == 1 && len(response.MenuExecution) == 1 {
				diagnostic = response.MenuExecution[0].Error
			}
			if !strings.Contains(diagnostic, "Secret data is unavailable") {
				t.Fatalf("expected real Secret refusal, got %q", diagnostic)
			}
			call := loop.ToolCalls()[0]
			if call.Error == "" || call.Summary == "" || !strings.Contains(call.ResponseJSON, "error_code") {
				t.Error("missing value-free rejection code or text")
			}
			for _, field := range []string{"ApplyObject", "creds", "restore expected credentials"} {
				if !strings.Contains(call.ResponseJSON, field) {
					t.Errorf("missing safe action evidence %q", field)
				}
			}
			recorded, _ := json.Marshal(call)
			result, err := incidentResultContract(IncidentResult{ProgramToolCalls: []ToolCall{call}})
			if err != nil {
				t.Fatal(err)
			}
			published, err := protojson.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			for _, data := range [][]byte{recorded, published} {
				if strings.Contains(string(data), canary) || strings.Contains(string(data), "stringData") {
					t.Fatal("tool call evidence retained credential material")
				}
			}
		})
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

func TestRecordedMitigateErrorDoesNotEchoRejectedParameter(t *testing.T) {
	const canary = "synthetic-echo-canary"
	_, loop := targetLoop(t)
	request := ToolRequest{Command: "mitigate", Targets: []MitigateTarget{{Name: "ApplyObject", Parameters: map[string]BindValues{
		"ref": {"Secret/app/creds"}, canary: {"invalid"},
	}}}}
	response := loop.Handle(context.Background(), request)
	if len(response.MenuExecution) != 1 || !strings.Contains(response.MenuExecution[0].Error, canary) {
		t.Fatal("test must exercise the real error that echoes an input")
	}
	raw, err := protojson.Marshal(toolCallContract(loop.ToolCalls()[0]))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), canary) {
		t.Fatal("stored rejection echoed the invalid input")
	}
}

func TestRecordedPlanApplyPreservesTrustFields(t *testing.T) {
	bindings := program.Bindings{Values: map[string]any{"mode": "repair"}}
	plan := selectedPlan{id: "plan-1", programID: "repair", bindings: bindings}
	loop := &ResponderLoop{plans: map[string]selectedPlan{plan.id: plan}}
	loop.recordToolCall(ToolRequest{Command: "plan"}, ToolResponse{Command: "plan", PlanID: plan.id}, nil)
	loop.recordToolCall(ToolRequest{Command: "apply", stage: true}, ToolResponse{
		Command: "apply", ApplicationID: "application-1", ProgramRuns: []ProgramRun{{
			ProgramID: plan.programID, ProgramRevision: "revision-1", Bindings: bindings, Verdict: program.Verdict{Result: program.NoMatch},
		}},
	}, []selectedPlan{plan})
	calls := loop.ToolCalls()
	request, response, ok := parseProgramToolCall(toolCallContract(calls[0]))
	if !ok || request.ProgramID != plan.programID || !request.Bindings.Equal(bindings) || response.PlanID != plan.id {
		t.Fatal("plan evidence is no longer readable by the trust parser")
	}
	request, response, ok = parseProgramToolCall(toolCallContract(calls[1]))
	if !ok || len(request.PlanIDs) != 1 || request.PlanIDs[0] != plan.id || response.ApplicationID != "application-1" ||
		len(response.ProgramRuns) != 1 || !response.ProgramRuns[0].Bindings.Equal(bindings) ||
		response.ProgramRuns[0].ProgramRevision != "revision-1" || response.ProgramRuns[0].Verdict.Result != program.NoMatch {
		t.Fatal("apply evidence is no longer readable by the trust parser")
	}
}
