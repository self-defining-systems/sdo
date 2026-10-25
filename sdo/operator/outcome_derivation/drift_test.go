package outcome_derivation

import (
	"context"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
)

func TestBenignDriftAssessmentUsesExactControllerObservation(t *testing.T) {
	observed := &contracts.DriftEvent{Ref: &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Manager: "operator", Fields: []*contracts.DriftField{{Path: "spec.ports[0].targetPort", New: "8081", Source: "8080"}}, Classification: "unexplained"}
	label := proto.Clone(observed).(*contracts.DriftEvent)
	label.InvestigationVerdict = "benign"
	label.InvestigationReason = "The selected pods intentionally listen on both ports"
	label.InvestigationEvidence = []*contracts.DriftEvidence{{Kind: "observed_impact", Observation: "live socket reads and connectivity establish both ports work"}}
	for _, tc := range []struct {
		name   string
		change func(*contracts.DriftEvent)
		want   int
	}{
		{"exact", func(*contracts.DriftEvent) {}, 1},
		{"old values excluded", func(e *contracts.DriftEvent) { e.Fields[0].Old = "1234" }, 1},
		{"changed current", func(e *contracts.DriftEvent) { e.Fields[0].New = "9999" }, 0},
		{"changed source", func(e *contracts.DriftEvent) { e.Fields[0].Source = "8082" }, 0},
		{"changed manager", func(e *contracts.DriftEvent) { e.Manager = "someone-else" }, 0},
		{"wrong object", func(e *contracts.DriftEvent) { e.Ref.Name = "peer" }, 0},
		{"no evidence", func(e *contracts.DriftEvent) { e.InvestigationEvidence = nil }, 0},
		{"invalid evidence kind", func(e *contracts.DriftEvent) { e.InvestigationEvidence[0].Kind = "quiet-health" }, 0},
		{"unsupported bug", func(e *contracts.DriftEvent) { e.InvestigationVerdict = "bug" }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := proto.Clone(label).(*contracts.DriftEvent)
			tc.change(candidate)
			if checked := CheckedDriftAssessments([]*contracts.DriftEvent{observed}, []*contracts.DriftEvent{candidate}); len(checked) != tc.want {
				t.Fatalf("checked=%+v", checked)
			}
		})
	}
	closure := fixtureClosure()
	closure.Request.DriftEvents = []*contracts.DriftEvent{observed}
	closure.Result.DriftEvents = []*contracts.DriftEvent{label}
	closure.Result.RepairActions = nil
	closure.Result.ConfirmedRootCauses = nil
	result, err := Derive(context.Background(), closure, "", "test", "test")
	if err != nil || result.Record.GetClassification() != "cleared_without_action" || result.RequiresReflection || result.Record.GetDriftEvents()[0].GetInvestigationVerdict() != "benign" {
		t.Fatalf("benign no-op earned credit or lost label: %+v %v", result, err)
	}
}
