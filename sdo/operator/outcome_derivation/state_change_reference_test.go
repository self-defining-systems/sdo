package outcome_derivation

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestParseKeyRejectsCompoundReferences(t *testing.T) {
	for _, source := range []string{
		"Service/geo; Service/recommendation",
		"Service/geo, Service/recommendation",
		"Service/geo Service/recommendation",
		"Service/geo;/Service/recommendation",
	} {
		if key, ok := parseKey(source); ok {
			t.Errorf("parseKey(%q) = %+v, want rejection", source, key)
		}
	}
	for source, want := range map[string]objectKey{
		"Service/geo":       {kind: "service", name: "geo", label: "Service/geo"},
		"Service/hotel/geo": {kind: "service", name: "geo", namespace: "hotel", label: "Service/geo"},
	} {
		if key, ok := parseKey(source); !ok || key != want {
			t.Errorf("parseKey(%q) = %+v, %v, want %+v", source, key, ok, want)
		}
	}
}

func TestDeriveVerifiesEachSeparateStateChangeEntry(t *testing.T) {
	closure := fixtureClosure()
	stamp := func(offset time.Duration) *timestamppb.Timestamp {
		return timestamppb.New(closure.DetectedAt.AsTime().Add(offset))
	}
	cause := closure.Result.ConfirmedRootCauses[0]
	cause.Evidence = []*contractsv1alpha1.RootCauseEvidence{
		{Kind: "state-change", Source: "Service/geo", Observation: "targetPort changed"},
		{Kind: "state-change", Source: "Service/recommendation", Observation: "targetPort changed"},
	}
	closure.ObservedStateChanges = []*contractsv1alpha1.ObservedStateChange{
		{Kind: "Service", Name: "geo", FirstObservedAt: stamp(10 * time.Second)},
		{Kind: "Service", Name: "recommendation", FirstObservedAt: stamp(10 * time.Second)},
	}
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	verification := result.Record.GetDiagnosisVerification()[0]
	for _, check := range verification.GetEvidence()[:2] {
		if check.Verified == nil || !*check.Verified {
			t.Fatalf("separate state-change entry not verified: %+v", check)
		}
	}
	if verification.GetVerdict() != "confirmed" {
		t.Fatalf("verdict = %s, want confirmed: %+v", verification.GetVerdict(), verification)
	}
}

// These fixtures model the controller's namespace-local state diffs, not run output.
func TestDeriveResolvesStateChangeReferencesWithinIncidentNamespace(t *testing.T) {
	for _, observation := range []string{"dispatch", "observed", "final"} {
		for _, source := range []string{"Deployment/app", "Deployment/hotel/app", "deploy/hotel/app", "Deployment/foreign/app", "Deployment/app; Deployment/other"} {
			t.Run(observation+"/"+source, func(t *testing.T) {
				closure := fixtureClosure()
				closure.Request.Namespace = "hotel"
				cause := closure.Result.ConfirmedRootCauses[0]
				cause.Resources = nil // Evidence alone must identify the repaired object.
				cause.Evidence = []*contractsv1alpha1.RootCauseEvidence{{Kind: "state-change", Source: source}}
				change := &contractsv1alpha1.StateChange{Kind: "Deployment", Name: "app", Change: "modified", Fields: []*contractsv1alpha1.StateFieldChange{{Field: "image", Before: stringPointer("good"), After: stringPointer("bad")}}}
				switch observation {
				case "dispatch":
					closure.Request.StateChanges = &contractsv1alpha1.StateChanges{Changes: []*contractsv1alpha1.StateChange{change}}
					closure.FinalStateChanges = &contractsv1alpha1.StateChanges{ObservedAt: closure.VerifiedAt}
					closure.Result.RepairActions[0].CompletedAt = closure.HealthClearedAt
					cause.ExplainedDetectors = nil // Baseline restoration must supply the causal check.
				case "observed":
					closure.ObservedStateChanges = []*contractsv1alpha1.ObservedStateChange{{Kind: "Deployment", Name: "app", FirstObservedAt: closure.DetectedAt}}
				case "final":
					closure.FinalStateChanges = &contractsv1alpha1.StateChanges{Changes: []*contractsv1alpha1.StateChange{change}}
				}
				result, err := Derive(context.Background(), closure, "repair-commit", "scripted", "none")
				if err != nil {
					t.Fatal(err)
				}
				verification := result.Record.GetDiagnosisVerification()[0]
				check := verification.GetEvidence()[0]
				valid := !strings.Contains(source, "foreign") && !strings.Contains(source, ";")
				if check.Verified == nil || check.GetVerified() != valid {
					t.Fatalf("evidence verified = %v, want %v: %+v", check.Verified, valid, verification)
				}
				if valid {
					if verification.GetVerdict() != "confirmed" || !verification.GetRepair().GetAttributed() || result.Record.GetClassification() != "success" || !result.RequiresReflection {
						t.Fatalf("local citation did not reach learnable success: %+v", result.Record)
					}
					if observation == "dispatch" && (len(verification.GetEvidence()) != 2 || verification.GetEvidence()[1].GetKind() != "baseline-restoration") {
						t.Fatalf("missing baseline restoration: %+v", verification)
					}
				} else {
					if check.GetReason() == "" || verification.GetRepair().GetAttributed() || len(verification.GetEvidence()) != 1 || result.Record.GetClassification() == "success" || result.RequiresReflection {
						t.Fatalf("invalid citation received repair/restoration/learning credit: %+v", result.Record)
					}
					if strings.Contains(source, "foreign") && !strings.Contains(check.GetReason(), "namespace") {
						t.Fatalf("foreign namespace rejection lacks reason: %+v", check)
					}
				}
			})
		}
	}
}

func TestDeriveNamespacedCitationCannotClaimRespondersOwnEdit(t *testing.T) {
	for _, source := range []string{"Deployment/app", "Deployment/hotel/app"} {
		t.Run(source, func(t *testing.T) {
			closure := fixtureClosure()
			closure.Request.Namespace = "hotel"
			closure.Result.ConfirmedRootCauses[0].Evidence = []*contractsv1alpha1.RootCauseEvidence{{Kind: "state-change", Source: source}}
			closure.ObservedStateChanges = []*contractsv1alpha1.ObservedStateChange{{Kind: "Deployment", Name: "app", FirstObservedAt: closure.Result.RepairActions[0].StartedAt}}
			result, err := Derive(context.Background(), closure, "repair-commit", "scripted", "none")
			if err != nil {
				t.Fatal(err)
			}
			check := result.Record.GetDiagnosisVerification()[0].GetEvidence()[0]
			if check.GetVerified() || !strings.Contains(check.GetReason(), "repair-1") || result.RequiresReflection {
				t.Fatalf("own edit did not remain contradicted: %+v", result.Record)
			}
		})
	}
}
