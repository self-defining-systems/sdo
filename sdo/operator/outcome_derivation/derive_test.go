package outcome_derivation

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	outcomeclass "sdo.dev/operator/contracts/outcome"
)

func outOfReachClosure(reach string, healthy bool) *contracts.IncidentClosure {
	at := func(seconds int) *timestamppb.Timestamp {
		return timestamppb.New(time.Date(2026, 11, 10, 12, 0, seconds, 0, time.UTC))
	}
	store := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "store"}
	closure := &contracts.IncidentClosure{
		Request: &contracts.IncidentRequest{IncidentId: "incident-1", Findings: []*contracts.Finding{{
			DetectorId: "traffic-links", RuleId: "link-reachability.web.store.5432", Status: "active", Severity: "critical",
			Summary: "link web -> store:5432 is slow", Evidence: "p50 2s", PrimaryResource: store,
		}}},
		Result: &contracts.IncidentResult{IncidentId: "incident-1", Status: "completed", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{
			Summary: "delay on the network path to store", Resources: []*contracts.ObjectRef{store}, CauseClass: "network-delay", Reach: reach,
			Evidence: []*contracts.RootCauseEvidence{{Kind: "detector-finding", Source: "traffic-links", Observation: "p50 2s against 5ms healthy"}},
		}}},
		DetectedAt: at(0), DispatchedAt: at(1), ResponderCompletedAt: at(2),
	}
	if healthy {
		closure.FinalDetectorStates = []*contracts.DetectorEvaluation{{DetectorId: "traffic-links", Status: "clear", EvaluatedAt: at(3)}}
		closure.VerifiedAt = at(3)
	}
	return closure
}

func TestOutOfReachCauseWithoutRecoveryIsEscalatedNotSuccess(t *testing.T) {
	derived, err := Derive(context.Background(), outOfReachClosure(outcomeclass.ReachOutOfReach, false), "", "codex", "model")
	if err != nil {
		t.Fatal(err)
	}
	if got := derived.Record.GetClassification(); got != outcomeclass.Escalated {
		t.Fatalf("classification = %q, want %q", got, outcomeclass.Escalated)
	}
	if derived.RequiresReflection {
		t.Fatal("an escalated outcome must not ask for repair learning")
	}
	if cause := derived.Record.GetConfirmedRootCauses()[0]; cause.GetCauseClass() != "network-delay" || cause.GetReach() != outcomeclass.ReachOutOfReach {
		t.Fatalf("outcome lost cause class or reach: %v", cause)
	}
}

func TestEscalationNeedsOutOfReachAndNeverOverridesVerifiedHealth(t *testing.T) {
	for name, tc := range map[string]struct {
		reach   string
		healthy bool
		want    string
	}{
		"in scope, not recovered": {outcomeclass.ReachInScope, false, outcomeclass.Failed},
		"out of reach, recovered": {outcomeclass.ReachOutOfReach, true, outcomeclass.ClearedWithoutAction},
	} {
		t.Run(name, func(t *testing.T) {
			derived, err := Derive(context.Background(), outOfReachClosure(tc.reach, tc.healthy), "", "codex", "model")
			if err != nil {
				t.Fatal(err)
			}
			if got := derived.Record.GetClassification(); got != tc.want {
				t.Fatalf("classification = %q, want %q", got, tc.want)
			}
		})
	}
}
