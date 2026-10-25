package runtime

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestPartialReflectionRetainsRecoveredUnwrittenSymptoms(t *testing.T) {
	outcome := coldPartialRepairOutcome(t)
	outcome.ProgramToolCalls = nil
	deployment := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	service := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	outcome.ConfirmedRootCauses[0].Resources = []*contracts.ObjectRef{deployment, service}
	outcome.RepairActions[0].Resources = []*contracts.ObjectRef{deployment}
	outcome.Findings[0].PrimaryResource = deployment
	outcome.Findings = append(outcome.Findings, &contracts.Finding{DetectorId: "endpoints", Status: "active", PrimaryResource: service, Fingerprint: "endpoints/api"})
	outcome.DetectorHistory = []*contracts.DetectorEvaluation{{DetectorId: "health-objective", Status: "firing", Fingerprints: []string{"health-objective/unverified"}, EvaluatedAt: timestamppb.New(time.Unix(120, 0))}, {DetectorId: "endpoints", Status: "clear", EvaluatedAt: timestamppb.New(time.Unix(120, 0))}}
	if got := partialRepairCauses(outcome); len(got) != 1 || got[0] != 0 {
		t.Fatalf("unwritten Service hid verified Deployment repair: %v", got)
	}
	projected := partialReflectionOutcome(outcome)
	if len(projected.ConfirmedRootCauses) != 1 || len(projected.ConfirmedRootCauses[0].Resources) != 2 || len(projected.RepairActions) != 1 || projected.Classification != "failed" {
		t.Fatalf("partial learning projection lost symptoms or upgraded closure: %v", projected)
	}
	for name, mutate := range map[string]func(*contracts.OutcomeRecord){
		"no symptom open":  func(o *contracts.OutcomeRecord) { o.Findings = o.Findings[:2] },
		"no symptom fixed": func(o *contracts.OutcomeRecord) { o.DetectorHistory = o.DetectorHistory[:1] },
		"symptom still broken": func(o *contracts.OutcomeRecord) {
			o.DetectorHistory[1].Status = "firing"
			o.DetectorHistory[1].Fingerprints = []string{"endpoints/api"}
		},
		"symptom stale":             func(o *contracts.OutcomeRecord) { o.DetectorHistory[1].EvaluatedAt = timestamppb.New(time.Unix(99, 0)) },
		"untrusted workload repair": func(o *contracts.OutcomeRecord) { o.RepairActions[0].ActionId = "worker-claim" },
		"symptom actually written without proof": func(o *contracts.OutcomeRecord) {
			a := proto.Clone(o.RepairActions[0]).(*contracts.RepairActionReceipt)
			a.ActionId = "worker-claim"
			a.Resources = []*contracts.ObjectRef{service}
			o.RepairActions = append(o.RepairActions, a)
		},
	} {
		t.Run(name, func(t *testing.T) {
			o := proto.Clone(outcome).(*contracts.OutcomeRecord)
			mutate(o)
			if got := partialRepairCauses(o); len(got) != 0 {
				t.Fatalf("unproven symptom admitted: %v", got)
			}
		})
	}
}
