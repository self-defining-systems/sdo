package outcome_derivation

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func fixtureClosure() *contractsv1alpha1.IncidentClosure {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	stamp := func(offset time.Duration) *timestamppb.Timestamp { return timestamppb.New(now.Add(offset)) }
	return &contractsv1alpha1.IncidentClosure{
		Request: &contractsv1alpha1.IncidentRequest{
			IncidentId: "incident-1", SourceCommit: "source", DeployedCommit: "deployed",
			Findings:        []*contractsv1alpha1.Finding{{DetectorId: "health", RuleId: "unhealthy", Status: "active"}},
			DetectorHistory: []*contractsv1alpha1.DetectorEvaluation{{DetectorId: "health", Status: "firing", EvaluatedAt: stamp(0)}},
		},
		Result: &contractsv1alpha1.IncidentResult{
			IncidentId: "incident-1", Status: "completed",
			ConfirmedRootCauses: []*contractsv1alpha1.ConfirmedRootCause{{
				Summary: "bad replica", Resources: []*contractsv1alpha1.ObjectRef{{Kind: "Deployment", Name: "app"}},
				Evidence:           []*contractsv1alpha1.RootCauseEvidence{{Kind: "detector-finding", Source: "health"}},
				ExplainedDetectors: []string{"health"},
			}},
			RepairActions: []*contractsv1alpha1.RepairActionReceipt{{
				ActionId: "repair-1", Target: "Deployment/app", Success: true, StartedAt: stamp(time.Minute),
				Resources: []*contractsv1alpha1.ObjectRef{{Kind: "Deployment", Name: "app"}},
			}},
			Usage: &contractsv1alpha1.UsageMetrics{},
		},
		FinalDetectorStates: []*contractsv1alpha1.DetectorEvaluation{{DetectorId: "health", Status: "clear", EvaluatedAt: stamp(3 * time.Minute)}},
		DetectedAt:          stamp(0), DispatchedAt: stamp(time.Minute), ResponderCompletedAt: stamp(2 * time.Minute),
		VerifiedAt: stamp(3 * time.Minute), HealthClearedAt: stamp(2 * time.Minute),
	}
}

func TestDeriveGoldenClosureDiagnosisParity(t *testing.T) {
	for _, tc := range []struct {
		file, verdict, classification string
		reflect                       bool
	}{
		{"closure_repaired.json", "confirmed", "success", true},
		{"closure_own_edit.json", "contradicted", "partial", false},
	} {
		t.Run(tc.file, func(t *testing.T) {
			path := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "contracts", "go", tc.file)
			payload, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			closure := new(contractsv1alpha1.IncidentClosure)
			if err := protojson.Unmarshal(payload, closure); err != nil {
				t.Fatal(err)
			}
			result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
			if err != nil {
				t.Fatal(err)
			}
			record := result.Record
			if record.GetClassification() != tc.classification || len(record.GetDiagnosisVerification()) != 1 || record.GetDiagnosisVerification()[0].GetVerdict() != tc.verdict || result.RequiresReflection != tc.reflect {
				t.Fatalf("unexpected derived outcome: %+v, reflect=%v", record, result.RequiresReflection)
			}
		})
	}
}

func TestDeriveAttributesVerifiedRepair(t *testing.T) {
	result, err := Derive(context.Background(), fixtureClosure(), "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	if result.Record == nil || result.Record.GetIncidentId() != "incident-1" || result.Record.GetClassification() != "success" {
		t.Fatalf("derivation did not return a typed outcome: %+v", result.Record)
	}
	record := result.Record
	if record.GetClassification() != "success" || len(record.GetDiagnosisVerification()) != 1 || record.GetDiagnosisVerification()[0].GetVerdict() != "confirmed" || !record.GetDiagnosisVerification()[0].GetRepair().GetAttributed() || record.GetTimestamps().GetCompletedAt() == nil || !result.RequiresReflection {
		t.Fatalf("unexpected outcome: %+v, reflect=%v", record, result.RequiresReflection)
	}
}

func TestDeriveVerifiesQualifiedDetectorFinding(t *testing.T) {
	closure := fixtureClosure()
	closure.Request.Findings[0] = &contractsv1alpha1.Finding{
		DetectorId: "health-objective", RuleId: "deployment-unavailable", Status: "active",
		Fingerprint: "health-objective/deployment-unavailable/hotel-reservation/search",
	}
	closure.Request.DetectorHistory[0].DetectorId = "health-objective"
	closure.Result.ConfirmedRootCauses[0].Evidence[0].Source = "health-objective/deployment-unavailable/hotel-reservation/search"
	closure.Result.ConfirmedRootCauses[0].ExplainedDetectors = []string{"health-objective"}
	closure.FinalDetectorStates[0].DetectorId = "health-objective"
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	check := result.Record.GetDiagnosisVerification()[0].GetEvidence()[0]
	if check.Verified == nil || !check.GetVerified() || result.Record.GetClassification() != "success" || !result.RequiresReflection {
		t.Fatalf("qualified finding did not verify diagnosis: %+v", result.Record)
	}
}

func TestDeriveCreditsDetectorFiringDuringSettleBeforeResponderTool(t *testing.T) {
	closure := fixtureClosure()
	closure.Result.ConfirmedRootCauses[0].ExplainedDetectors = []string{"health", "deployment-image-pull-backoff"}
	closure.DetectorTimeline = []*contractsv1alpha1.DetectorTimelineEntry{{DetectorId: "deployment-image-pull-backoff", Fingerprint: "deployment-image-pull-backoff/hotel-reservation/search", FirstActivatedAt: timestamppb.New(closure.DispatchedAt.AsTime().Add(time.Second)), LastSeenAt: timestamppb.New(closure.DispatchedAt.AsTime().Add(10 * time.Second))}}
	closure.Result.ProgramToolCalls = []*contractsv1alpha1.ProgramToolCall{{Command: "diagnose", At: timestamppb.New(closure.DispatchedAt.AsTime().Add(30 * time.Second))}}
	closure.Result.RepairActions[0].StartedAt = timestamppb.New(closure.DispatchedAt.AsTime().Add(45 * time.Second))
	closure.FinalDetectorStates = append(closure.FinalDetectorStates, &contractsv1alpha1.DetectorEvaluation{DetectorId: "deployment-image-pull-backoff", Status: "clear", EvaluatedAt: closure.VerifiedAt})
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	flip := result.Record.GetDiagnosisVerification()[0].GetDetectors()[1]
	if !flip.GetFiredAtDispatch() || !flip.GetFlipped() {
		t.Fatalf("settle-window detector firing was not credited: %+v", flip)
	}
	closure.DetectorTimeline[0].FirstActivatedAt = timestamppb.New(closure.DispatchedAt.AsTime().Add(46 * time.Second))
	closure.DetectorTimeline[0].LastSeenAt = closure.DetectorTimeline[0].FirstActivatedAt
	result, err = Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	if result.Record.GetDiagnosisVerification()[0].GetDetectors()[1].GetFiredAtDispatch() {
		t.Fatal("detector firing after repair was credited as dispatch evidence")
	}
}

func TestDeriveConfirmsVerifiedFindingWithSupplementalLiveObservation(t *testing.T) {
	closure := fixtureClosure()
	closure.Result.ConfirmedRootCauses[0].ExplainedDetectors = append(closure.Result.ConfirmedRootCauses[0].ExplainedDetectors, "deployment-image-pull-backoff")
	closure.Result.ConfirmedRootCauses[0].Evidence = append(closure.Result.ConfirmedRootCauses[0].Evidence,
		&contractsv1alpha1.RootCauseEvidence{Kind: "live-observation", Source: "kubectl get service"})
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	verification := result.Record.GetDiagnosisVerification()[0]
	if verification.GetVerdict() != "confirmed" || result.Record.GetClassification() != "success" || !result.RequiresReflection {
		t.Fatalf("verified detector finding and flip were downgraded: %+v", result.Record)
	}
	if len(verification.GetEvidence()) != 2 || verification.GetEvidence()[1].Verified != nil {
		t.Fatalf("supplemental observation was not retained as unverified: %+v", verification.GetEvidence())
	}
}

func TestDeriveVerifiesDetectorHistoryFingerprint(t *testing.T) {
	closure := fixtureClosure()
	closure.Request.DetectorHistory[0].Fingerprints = []string{"health/unhealthy/hotel-reservation/Deployment/app"}
	closure.Result.ConfirmedRootCauses[0].Evidence[0].Source = "health/unhealthy/hotel-reservation/Deployment/app"
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	check := result.Record.GetDiagnosisVerification()[0].GetEvidence()[0]
	if check.Verified == nil || !check.GetVerified() {
		t.Fatalf("detector history fingerprint did not verify: %+v", check)
	}
}

func TestDeriveDoesNotContradictUnknownFindingSource(t *testing.T) {
	closure := fixtureClosure()
	closure.Result.ConfirmedRootCauses[0].Evidence[0].Source = "unknown/rule/namespace/resource"
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	check := result.Record.GetDiagnosisVerification()[0].GetEvidence()[0]
	if check.Verified != nil || result.Record.GetDiagnosisVerification()[0].GetVerdict() == "contradicted" {
		t.Fatalf("unknown source was treated as contradicted: %+v", result.Record.GetDiagnosisVerification()[0])
	}
}

func TestDeriveDoesNotClassifyContradictedDiagnosisAsSuccess(t *testing.T) {
	closure := fixtureClosure()
	cause := *closure.Result.ConfirmedRootCauses[0]
	cause.Evidence = []*contractsv1alpha1.RootCauseEvidence{{Kind: "detector-finding", Source: "other"}}
	closure.Result.ConfirmedRootCauses = append(closure.Result.ConfirmedRootCauses, &cause)
	closure.Request.Findings = append(closure.Request.Findings, &contractsv1alpha1.Finding{DetectorId: "other", RuleId: "healthy", Status: "clear"})
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	if result.Record.GetClassification() == "success" || result.RequiresReflection {
		t.Fatalf("contradicted diagnosis classified as success: %+v", result.Record)
	}
}

func TestDeriveAttributesLatentRepairAfterEarlierHealthClear(t *testing.T) {
	closure := fixtureClosure()
	closure.Request.Findings[0].DetectorId = "drift/investigator"
	closure.Request.Findings[0].PrimaryResource = &contractsv1alpha1.ObjectRef{Kind: "Deployment", Name: "app"}
	closure.Request.DetectorHistory = []*contractsv1alpha1.DetectorEvaluation{{DetectorId: "drift/investigator", Status: "firing", EvaluatedAt: closure.DetectedAt}}
	closure.Result.ConfirmedRootCauses[0].ExplainedDetectors = []string{"drift/investigator"}
	closure.Result.ConfirmedRootCauses[0].Evidence[0].Source = "drift/investigator"
	closure.FinalDetectorStates = []*contractsv1alpha1.DetectorEvaluation{{DetectorId: "health", Status: "clear", EvaluatedAt: closure.VerifiedAt}}
	closure.IncidentDetectorStates = []*contractsv1alpha1.DetectorEvaluation{{DetectorId: "drift/investigator", Status: "clear", EvaluatedAt: closure.VerifiedAt}}
	closure.HealthClearedAt = timestamppb.New(closure.DetectedAt.AsTime().Add(-time.Minute))
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	if result.Record.GetClassification() != "success" || !result.Record.GetDiagnosisVerification()[0].GetRepair().GetAttributed() || !result.RequiresReflection {
		t.Fatalf("latent repair lost attribution: %+v", result.Record)
	}
	if result.Record.GetTimestamps().GetMitigatedAt().AsTime().Before(closure.DispatchedAt.AsTime()) {
		t.Fatalf("latent incident was mitigated before dispatch: %+v", result.Record.GetTimestamps())
	}
}

func TestDeriveCarriesClassifiedDriftEvidence(t *testing.T) {
	closure := fixtureClosure()
	closure.Request.DriftEvents = []*contractsv1alpha1.DriftEvent{{Ref: &contractsv1alpha1.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"},
		Fields: []*contractsv1alpha1.DriftField{{Path: "spec.ports[0].targetPort", Old: "8080", New: "9999"}},
		Source: "differs from accepted manifest", Manager: "unknown-editor", Classification: "fault", ProgramId: "targetport", ObservedAt: closure.DetectedAt}}
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil || len(result.Record.GetDriftEvents()) != 1 || result.Record.GetDriftEvents()[0].GetProgramId() != "targetport" {
		t.Fatalf("classified drift lost from outcome: %+v, %v", result.Record, err)
	}
}

func TestDerivePreservesTrustedFastPathEvidence(t *testing.T) {
	closure := fixtureClosure()
	closure.Result.ResolutionPath = "fast_path"
	closure.Result.FastPathPrograms = []string{"targetport"}
	closure.Result.FastPathRelationCoveredFindings = []*contractsv1alpha1.RelationCoveredFinding{{FindingFingerprint: "health/service/api", ProgramId: "targetport", Bindings: []*contractsv1alpha1.Binding{{Name: "deployment", Object: &contractsv1alpha1.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}}}}}
	closure.Result.PlanApplications = []*contractsv1alpha1.PlanApplication{{ApplicationId: "fast-path-1", Applied: true, ProgramIds: []string{"targetport"}, Verification: &contractsv1alpha1.Verdict{Result: contractsv1alpha1.Verdict_RESULT_NO_MATCH}}}
	derived, err := Derive(context.Background(), closure, "", "codex", "test-model")
	if err != nil || derived.Record.GetResolutionPath() != "fast_path" || len(derived.Record.GetFastPathPrograms()) != 1 || len(derived.Record.GetPlanApplications()) != 1 || len(derived.Record.GetFastPathRelationCoveredFindings()) != 1 || derived.Record.GetFastPathRelationCoveredFindings()[0].GetBindings()[0].GetObject().GetName() != "api" {
		t.Fatalf("fast path evidence missing: %+v %v", derived.Record, err)
	}
}

func TestDerivePreservesFastPathSkipReason(t *testing.T) {
	closure := fixtureClosure()
	closure.Result.ResolutionPath = "responder"
	closure.Result.FastPathSkippedReason = "uncovered_finding"
	derived, err := Derive(context.Background(), closure, "", "codex", "test-model")
	if err != nil || derived.Record.GetFastPathSkippedReason() != "uncovered_finding" {
		t.Fatalf("fast-path skip reason missing from outcome: %+v %v", derived.Record, err)
	}
}

func TestDeriveConfirmsLatentCauseFromProgramVerdictFlip(t *testing.T) {
	// TODO: Add a hermetic e2e latent targetPort repair case with pre-fix
	// Match and verified post-fix NoMatch once that suite lands here.
	closure := fixtureClosure()
	cause := closure.Result.ConfirmedRootCauses[0]
	cause.ExplainedDetectors = []string{"latent"}
	binding := []*contractsv1alpha1.Binding{{Name: "deployment", Object: &contractsv1alpha1.ObjectRef{Kind: "Deployment", Name: "app"}}}
	closure.Request.InitialProgramRuns = []*contractsv1alpha1.ProgramRun{{ProgramId: "deployment-fault", Bindings: binding, Verdict: &contractsv1alpha1.Verdict{Result: contractsv1alpha1.Verdict_RESULT_MATCH}}}
	closure.Result.ProgramRuns = []*contractsv1alpha1.ProgramRun{{ProgramId: "deployment-fault", Bindings: binding, Verdict: &contractsv1alpha1.Verdict{Result: contractsv1alpha1.Verdict_RESULT_NO_MATCH}}}
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	verification := result.Record.GetDiagnosisVerification()[0]
	if verification.GetVerdict() != "confirmed" || !result.RequiresReflection || len(verification.GetEvidence()) < 2 {
		t.Fatalf("program flip did not verify latent cause: %+v", verification)
	}
}

func TestDeriveConfirmsLatentCauseWhenFaultedFieldReturnsToBaseline(t *testing.T) {
	closure := fixtureClosure()
	cause := closure.Result.ConfirmedRootCauses[0]
	cause.ExplainedDetectors = []string{"latent"}
	cause.Evidence = []*contractsv1alpha1.RootCauseEvidence{{Kind: "state-change", Source: "Deployment/app", Observation: "image drifted"}}
	closure.Request.StateChanges = &contractsv1alpha1.StateChanges{Changes: []*contractsv1alpha1.StateChange{{Kind: "Deployment", Name: "app", Change: "modified", Fields: []*contractsv1alpha1.StateFieldChange{{Field: "image", Before: stringPointer("good"), After: stringPointer("bad")}}}}}
	closure.Result.RepairActions[0].CompletedAt = timestamppb.New(closure.DetectedAt.AsTime().Add(90 * time.Second))
	closure.FinalStateChanges = &contractsv1alpha1.StateChanges{ObservedAt: closure.VerifiedAt}
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	verification := result.Record.GetDiagnosisVerification()[0]
	if verification.GetVerdict() != "confirmed" || len(verification.GetEvidence()) < 2 {
		t.Fatalf("reverted fault did not verify latent cause: %+v", verification)
	}
}

func TestDeriveDoesNotConfirmLatentCauseWithoutMatchingPostFixEvidence(t *testing.T) {
	closure := fixtureClosure()
	closure.Result.ConfirmedRootCauses[0].ExplainedDetectors = []string{"latent"}
	closure.Request.InitialProgramRuns = []*contractsv1alpha1.ProgramRun{{ProgramId: "fault", Bindings: []*contractsv1alpha1.Binding{{Name: "deployment", Object: &contractsv1alpha1.ObjectRef{Kind: "Deployment", Name: "app"}}}, Verdict: &contractsv1alpha1.Verdict{Result: contractsv1alpha1.Verdict_RESULT_MATCH}}}
	closure.Result.ProgramRuns = []*contractsv1alpha1.ProgramRun{{ProgramId: "fault", Bindings: []*contractsv1alpha1.Binding{{Name: "deployment", Object: &contractsv1alpha1.ObjectRef{Kind: "Deployment", Name: "other"}}}, Verdict: &contractsv1alpha1.Verdict{Result: contractsv1alpha1.Verdict_RESULT_NO_MATCH}}}
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Record.GetDiagnosisVerification()[0].GetVerdict(); got != "unverified" {
		t.Fatalf("unrelated binding verified latent cause: %s", got)
	}
	if result.Record.GetClassification() == "success" {
		t.Fatalf("unverified diagnosis classified as success: %+v", result.Record)
	}
}

func TestDeriveDoesNotConfirmLatentCauseWhileFaultedObjectStillDiffers(t *testing.T) {
	closure := fixtureClosure()
	cause := closure.Result.ConfirmedRootCauses[0]
	cause.ExplainedDetectors = []string{"latent"}
	cause.Evidence = []*contractsv1alpha1.RootCauseEvidence{{Kind: "state-change", Source: "Deployment/app", Observation: "image drifted"}}
	change := &contractsv1alpha1.StateChange{Kind: "Deployment", Name: "app", Change: "modified", Fields: []*contractsv1alpha1.StateFieldChange{{Field: "image", Before: stringPointer("good"), After: stringPointer("bad")}}}
	closure.Request.StateChanges = &contractsv1alpha1.StateChanges{Changes: []*contractsv1alpha1.StateChange{change}}
	closure.FinalStateChanges = &contractsv1alpha1.StateChanges{ObservedAt: closure.VerifiedAt, Changes: []*contractsv1alpha1.StateChange{change}}
	closure.Result.RepairActions[0].CompletedAt = timestamppb.New(closure.DetectedAt.AsTime().Add(90 * time.Second))
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Record.GetDiagnosisVerification()[0].GetVerdict(); got != "unverified" {
		t.Fatalf("remaining drift verified latent cause: %s", got)
	}
}

func TestDeriveMitigationTimeFromVerifiedRepair(t *testing.T) {
	closure := fixtureClosure()
	closure.Result.RepairActions[0].CompletedAt = timestamppb.New(closure.DetectedAt.AsTime().Add(90 * time.Second))
	closure.ResponderCompletedAt = timestamppb.New(closure.DetectedAt.AsTime().Add(10 * time.Minute))
	closure.VerifiedAt = timestamppb.New(closure.DetectedAt.AsTime().Add(11 * time.Minute))
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Record.GetTimestamps().GetMitigatedAt().AsTime(); !got.Equal(closure.Result.RepairActions[0].CompletedAt.AsTime()) {
		t.Fatalf("mitigated at %v, want verified repair completion %v", got, closure.Result.RepairActions[0].CompletedAt.AsTime())
	}
}

func TestDerivePreservesResponderProgramEvidenceAsProtoJSON(t *testing.T) {
	closure := fixtureClosure()
	closure.Result.ProgramToolCalls = []*contractsv1alpha1.ProgramToolCall{{Command: "apply", At: timestamppb.Now(), RequestJson: `{"plan_ids":["plan-1"]}`}}
	closure.Result.ProgramRuns = []*contractsv1alpha1.ProgramRun{{ProgramId: "targetport", Verdict: &contractsv1alpha1.Verdict{Result: contractsv1alpha1.Verdict_RESULT_NO_MATCH}}}
	closure.Result.PlanApplications = []*contractsv1alpha1.PlanApplication{{ApplicationId: "application-1", ProgramIds: []string{"targetport"}, Applied: true, Verification: &contractsv1alpha1.Verdict{Result: contractsv1alpha1.Verdict_RESULT_NO_MATCH}}}
	closure.Result.ResponseLoopsUsed = 1
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := protojson.Marshal(result.Record)
	if err != nil {
		t.Fatal(err)
	}
	recorded := new(contractsv1alpha1.OutcomeRecord)
	if err := protojson.Unmarshal(encoded, recorded); err != nil {
		t.Fatal(err)
	}
	if len(recorded.GetProgramToolCalls()) != 1 || len(recorded.GetProgramRuns()) != 1 || len(recorded.GetPlanApplications()) != 1 || recorded.GetResponseLoopsUsed() != 1 {
		t.Fatalf("program outcome evidence lost: %+v", recorded)
	}
}

func TestDeriveRejectsExternalRecoveryAndNoAction(t *testing.T) {
	for _, tc := range []struct {
		name           string
		mutate         func(*contractsv1alpha1.IncidentClosure)
		classification string
	}{
		{"external", func(c *contractsv1alpha1.IncidentClosure) { c.Result.RepairActions[0].Resources[0].Name = "other" }, "external_recovery"},
		{"no-action", func(c *contractsv1alpha1.IncidentClosure) { c.Result.RepairActions = nil }, "cleared_without_action"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			closure := fixtureClosure()
			tc.mutate(closure)
			result, err := Derive(context.Background(), closure, "", "codex", "test-model")
			if err != nil {
				t.Fatal(err)
			}
			if result.Record.GetClassification() != tc.classification || result.RequiresReflection {
				t.Fatalf("classification=%s reflect=%v", result.Record.GetClassification(), result.RequiresReflection)
			}
		})
	}
}

func TestDeriveRejectsReversedControllerTimes(t *testing.T) {
	closure := fixtureClosure()
	closure.DispatchedAt = timestamppb.New(closure.GetResponderCompletedAt().AsTime().Add(time.Minute))
	if _, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model"); err == nil {
		t.Fatal("reversed controller timeline must not produce a durable outcome")
	}
}
