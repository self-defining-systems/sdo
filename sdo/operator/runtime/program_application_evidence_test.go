package runtime

import (
	"encoding/json"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
	"testing"
	"time"
)

const (
	repeatProgram     = "service-targetport-mismatch"
	repeatApplication = "app-1"
	repeatRevision    = "rev1"
)

var repeatService = &contracts.ObjectRef{Kind: "Service", Namespace: "default", Name: "reservation"}

func receipt(kind, id string) *contracts.RepairActionReceipt {
	at := timestamppb.New(time.Unix(1_700_000_000, 0))
	return &contracts.RepairActionReceipt{ActionId: id, Kind: kind, Target: "Service/reservation", Summary: kind, Details: kind, Success: true, StartedAt: at, CompletedAt: at}
}

func toolCall(t *testing.T, at time.Time, request ToolRequest, response ToolResponse) *contracts.ProgramToolCall {
	t.Helper()
	response.Command = request.Command
	req, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.ProgramToolCall{Command: request.Command, At: timestamppb.New(at), RequestJson: string(req), ResponseJson: string(resp)}
}

func verifiedRepeat(t *testing.T) *contracts.OutcomeRecord {
	t.Helper()
	applied := time.Unix(1_700_000_100, 0)
	bindings := program.Bindings{"service": {Kind: "Service", Namespace: "default", Name: "reservation"}}
	return &contracts.OutcomeRecord{
		Classification: "success",
		RepairActions: []*contracts.RepairActionReceipt{
			receipt("sdo-apply", repeatApplication),
			receipt("program-plan", "sdo-program-"+repeatApplication),
		},
		PlanApplications: []*contracts.PlanApplication{{
			Plan: &contracts.Plan{}, ProgramIds: []string{repeatProgram}, Applied: true, ApplicationId: repeatApplication,
			CompletedAt:              timestamppb.New(applied),
			FinalEquivalenceVerified: proto.Bool(true), VerificationCompleted: true, VerificationPassed: true,
			Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH},
		}},
		ProgramRuns: []*contracts.ProgramRun{
			{ProgramId: repeatProgram, ProgramRevision: repeatRevision, Duration: durationpb.New(time.Second), Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_MATCH}, Bindings: []*contracts.Binding{{Name: "service", Object: repeatService}}},
			{ProgramId: "image-pull-source-drift", ProgramRevision: repeatRevision, Duration: durationpb.New(time.Second), Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}},
		},
		ProgramToolCalls: []*contracts.ProgramToolCall{
			toolCall(t, applied.Add(-time.Minute), ToolRequest{Command: "plan", ProgramID: repeatProgram, Bindings: bindings}, ToolResponse{PlanID: "plan-1"}),
			toolCall(t, applied.Add(time.Second), ToolRequest{Command: "apply", PlanIDs: []string{"plan-1"}}, ToolResponse{
				ApplicationID: repeatApplication,
				ProgramRuns:   []ProgramRun{{ProgramID: repeatProgram, ProgramRevision: repeatRevision, Bindings: bindings, Verdict: program.Verdict{Result: program.NoMatch}}},
			}),
		},
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "wrong targetPort", Resources: []*contracts.ObjectRef{repeatService}}},
	}
}
