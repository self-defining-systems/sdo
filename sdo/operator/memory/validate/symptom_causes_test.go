package validate

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
	"time"
)

func TestVerifiedCauseAllowsObservedRecoveredSymptomWithoutWrite(t *testing.T) {
	at := time.Unix(100, 0)
	deployment := &contracts.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}
	service := &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	outcome := &contracts.OutcomeRecord{
		Classification:      "success",
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{deployment, service}}},
		Findings:            []*contracts.Finding{{DetectorId: "workload", Status: "active", Fingerprint: "deployment", PrimaryResource: deployment}, {DetectorId: "endpoints", Status: "active", Fingerprint: "service", PrimaryResource: service}},
		RepairActions:       []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{deployment}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}},
		DetectorHistory:     []*contracts.DetectorEvaluation{{DetectorId: "workload", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}, {DetectorId: "endpoints", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}},
	}
	if got := VerifiedReflectionCauses(outcome); len(got) != 1 || len(got[0].Resources) != 2 {
		t.Fatalf("observed symptom disqualified hand repair: %v", got)
	}
	for name, change := range map[string]func(*contracts.OutcomeRecord){
		"missing symptom opening":   func(o *contracts.OutcomeRecord) { o.Findings = o.Findings[:1] },
		"missing symptom clearance": func(o *contracts.OutcomeRecord) { o.DetectorHistory = o.DetectorHistory[:1] },
		"stale symptom clearance":   func(o *contracts.OutcomeRecord) { o.DetectorHistory[1].EvaluatedAt = timestamppb.New(at) },
		"still broken symptom": func(o *contracts.OutcomeRecord) {
			o.DetectorHistory[1].Status = "firing"
			o.DetectorHistory[1].Fingerprints = []string{"service"}
		},
		"no repair":        func(o *contracts.OutcomeRecord) { o.RepairActions = nil },
		"unobserved write": func(o *contracts.OutcomeRecord) { o.Findings = o.Findings[1:] },
	} {
		t.Run(name, func(t *testing.T) {
			o := proto.Clone(outcome).(*contracts.OutcomeRecord)
			change(o)
			if got := VerifiedReflectionCauses(o); len(got) != 0 {
				t.Fatalf("unproven cause authorized: %v", got)
			}
		})
	}
}
