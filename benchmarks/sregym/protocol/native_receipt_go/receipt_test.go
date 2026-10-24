package receipt

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	c "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// Hand-authored generated messages, never experiment outputs.
func fixture() *c.NativeProductionReceipt {
	stamp := timestamppb.New(time.Date(2026, 10, 18, 3, 0, 0, 0, time.UTC))
	proposal, outcome, reflection := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	return &c.NativeProductionReceipt{
		SchemaVersion: "sdo.production-receipt/v2", RecordedAt: stamp, BrokerMode: "native", WorkerMountMode: "proposal-only", IncidentId: "incident-1", Namespace: "demo", ControlNamespace: "demo-sdo", ControllerImage: "controller:go", ResponderImage: "responder:python", ValidatorImage: "validator:go", ControllerPodUid: "pod-uid",
		NativeLedger: &c.BrokerLedger{
			IncidentId: "incident-1", Acknowledged: true, ProposalCommit: proposal, OutcomeCommit: outcome, ReflectionCommit: reflection, RequiresReflection: true, ReflectionProcessed: true,
			ReflectionDecision: &c.ReflectionDecision{LearningDecision: "no_change"},
			Receipt:            &c.ClosureReceipt{IncidentId: "incident-1", ProposalCommit: proposal, OutcomeCommit: outcome, ReflectionCommit: reflection, AckToken: "token"},
			Closure: &c.IncidentClosure{
				Request:             &c.IncidentRequest{IncidentId: "incident-1", Namespace: "demo", RepairPolicy: "recorded-actions"},
				Result:              &c.IncidentResult{IncidentId: "incident-1", Status: "completed", RepairActions: []*c.RepairActionReceipt{{ActionId: "repair-1", Success: true}}, ConfirmedRootCauses: []*c.ConfirmedRootCause{{Summary: "unavailable replica"}}},
				FinalDetectorStates: []*c.DetectorEvaluation{{DetectorId: "health-objective", Status: "clear"}}, VerifiedAt: stamp,
			},
			OutcomeDocument: &c.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident-1", Classification: "success", RepairCommit: proto.String(proposal)},
		},
		ResponderJobEvidence: []*c.ResponderJobEvidence{{IncidentId: "incident-1", JobName: "sdo-incident-1-hash", JobUid: "worker-uid", Succeeded: true}},
	}
}

func encoded(t testing.TB, message proto.Message) []byte {
	t.Helper()
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestReceiptBuildAndValidation(t *testing.T) {
	result := Evaluate(encoded(t, fixture()), true)
	if result.GetFailure() != nil {
		t.Fatal(result.GetFailure())
	}
	if result.GetReceipt().GetResolution() != "sdo_mitigated" || result.GetReceipt().GetOutcomeCommit() != strings.Repeat("b", 40) {
		t.Fatal(result)
	}
	if second := Evaluate(encoded(t, result.GetReceipt()), false); second.GetFailure() != nil {
		t.Fatal(second.GetFailure())
	}
	result.Receipt.OutcomeCommit = strings.Repeat("c", 40)
	if got := Evaluate(encoded(t, result.Receipt), false); got.GetFailure() == nil {
		t.Fatal("validation repaired a contradictory flat summary")
	}
}

func TestReceiptRejectsConflictingEvidence(t *testing.T) {
	cases := map[string]func(*c.NativeProductionReceipt){
		"partial":        func(r *c.NativeProductionReceipt) { r.NativeLedger.OutcomeDocument.Classification = "partial" },
		"unacknowledged": func(r *c.NativeProductionReceipt) { r.NativeLedger.Acknowledged = false },
		"identity":       func(r *c.NativeProductionReceipt) { r.NativeLedger.Closure.Result.IncidentId = "another" },
		"namespace":      func(r *c.NativeProductionReceipt) { r.NativeLedger.Closure.Request.Namespace = "another" },
		"receipt":        func(r *c.NativeProductionReceipt) { r.NativeLedger.Receipt.OutcomeCommit = strings.Repeat("c", 40) },
		"health":         func(r *c.NativeProductionReceipt) { r.NativeLedger.Closure.FinalDetectorStates[0].Status = "firing" },
		"fingerprints": func(r *c.NativeProductionReceipt) {
			r.NativeLedger.Closure.FinalDetectorStates[0].Fingerprints = []string{"still active"}
		},
		"updated reflection": func(r *c.NativeProductionReceipt) {
			r.NativeLedger.ReflectionCommit = ""
			r.NativeLedger.Receipt.ReflectionCommit = ""
			r.NativeLedger.ReflectionDecision.LearningDecision = "updated"
		},
		"failed reflection": func(r *c.NativeProductionReceipt) { r.NativeLedger.ReflectionDecision.LearningDecision = "failed" },
		"rollout": func(r *c.NativeProductionReceipt) {
			r.NativeLedger.RolloutExpectation = &c.ControllerRolloutExpectation{IncidentId: "incident-1", ReflectionCommit: strings.Repeat("c", 40), BeforeDetectorFingerprint: "old", AfterDetectorFingerprint: "new"}
		},
		"missing outcome": func(r *c.NativeProductionReceipt) { r.NativeLedger.OutcomeDocument = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			receipt := fixture()
			mutate(receipt)
			got := Evaluate(encoded(t, receipt), true)
			if got.GetFailure().GetCode() != c.ReceiptFailure_CODE_REJECTED_EVIDENCE || got.GetFailure().GetIncidentId() != receipt.GetIncidentId() {
				t.Fatalf("missing correlated typed rejection: %v", got)
			}
		})
	}
}

func TestFailedLearningRetainsVerifiedMitigation(t *testing.T) {
	r := fixture()
	l := r.NativeLedger
	l.ReflectionCommit = ""
	l.Receipt.ReflectionCommit = ""
	l.ReflectionDecision = &c.ReflectionDecision{LearningDecision: "failed", Error: "invalid authored program"}
	if result := Evaluate(encoded(t, r), true); result.GetFailure() != nil || result.GetReceipt().GetResolution() != "sdo_mitigated" {
		t.Fatal(result)
	}
}

func TestVerifiedFastPathNeedsNoResponderJob(t *testing.T) {
	r := fixture()
	l := r.NativeLedger
	r.ResponderJobEvidence = nil
	l.ProposalCommit = ""
	l.Receipt.ProposalCommit = ""
	l.OutcomeDocument.RepairCommit = nil
	l.RequiresReflection = false
	l.ReflectionDecision = nil
	l.ReflectionCommit = ""
	l.Receipt.ReflectionCommit = ""
	l.ReflectionSkippedReason = "trusted program fast path"
	l.OutcomeDocument.ResolutionPath = "fast_path"
	l.OutcomeDocument.FastPathPrograms = []string{"targetport"}
	l.Closure.Result.ResolutionPath = "fast_path"
	l.Closure.Result.FastPathPrograms = []string{"targetport"}
	l.OutcomeDocument.PlanApplications = []*c.PlanApplication{{Applied: true, Verification: &c.Verdict{Result: c.Verdict_RESULT_NO_MATCH}}}
	if result := Evaluate(encoded(t, r), true); result.GetFailure() != nil {
		t.Fatal(result)
	}
	l.OutcomeDocument.PlanApplications[0].RolledBack = true
	if result := Evaluate(encoded(t, r), true); result.GetFailure() == nil {
		t.Fatal("rolled-back plan was accepted")
	}
}

func settledFastPathFixture() *c.NativeProductionReceipt {
	r := fixture()
	l := r.NativeLedger
	r.ResponderJobEvidence = nil
	l.ProposalCommit, l.Receipt.ProposalCommit = "", ""
	l.OutcomeDocument.RepairCommit = nil
	l.OutcomeDocument.ResolutionPath, l.Closure.Result.ResolutionPath = "fast_path", "fast_path"
	l.OutcomeDocument.FastPathPrograms, l.Closure.Result.FastPathPrograms = []string{"targetport"}, []string{"targetport"}
	l.OutcomeDocument.ReflectionEligibility = &c.ReflectionEligibility{Version: 1}
	l.OutcomeDocument.PlanApplications = []*c.PlanApplication{{Applied: true, Verification: &c.Verdict{Result: c.Verdict_RESULT_NO_MATCH}}}
	return r
}

func TestFastPathAcceptsCompletedVersionedSettlement(t *testing.T) {
	if result := Evaluate(encoded(t, settledFastPathFixture()), true); result.GetFailure() != nil {
		t.Fatal(result.GetFailure())
	}
	for name, change := range map[string]func(*c.NativeProductionReceipt){
		"missing settlement": func(r *c.NativeProductionReceipt) { r.NativeLedger.OutcomeDocument.ReflectionEligibility = nil },
		"unfinished":         func(r *c.NativeProductionReceipt) { r.NativeLedger.ReflectionProcessed = false },
		"rejected":           func(r *c.NativeProductionReceipt) { r.NativeLedger.ReflectionRejected = true },
		"missing decision":   func(r *c.NativeProductionReceipt) { r.NativeLedger.ReflectionDecision = nil },
		"worker dispatch":    func(r *c.NativeProductionReceipt) { r.ResponderJobEvidence = fixture().ResponderJobEvidence },
		"program mismatch":   func(r *c.NativeProductionReceipt) { r.NativeLedger.Closure.Result.FastPathPrograms = []string{"other"} },
		"rollback": func(r *c.NativeProductionReceipt) {
			r.NativeLedger.OutcomeDocument.PlanApplications[0].RolledBack = true
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := settledFastPathFixture()
			change(r)
			if result := Evaluate(encoded(t, r), true); result.GetFailure() == nil {
				t.Fatal("incomplete or conflicting fast-path evidence accepted")
			}
		})
	}
}

func TestWireRejectionDoesNotPoisonNextIncident(t *testing.T) {
	for _, bad := range []string{`{`, `{"native_ledger":[]}`, `{"responder_job_evidence":[{"succeeded":1}]}`, `{"schema_version":22}`} {
		got := Evaluate([]byte(bad), true)
		if got.GetFailure().GetCode() != c.ReceiptFailure_CODE_INVALID_WIRE {
			t.Fatalf("%q was not a typed wire failure: %v", bad, got)
		}
		if next := Evaluate(encoded(t, fixture()), true); next.GetFailure() != nil {
			t.Fatalf("failed incident blocked next: %v", next)
		}
	}
}

func FuzzNativeReceiptBoundary(f *testing.F) {
	f.Add(encoded(f, fixture()))
	f.Add([]byte(`{"native_ledger":[]}`))
	f.Add([]byte{0xff, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		got := Evaluate(data, true)
		if got.GetFailure() == nil {
			if next := Evaluate(encoded(t, got.GetReceipt()), false); next.GetFailure() != nil {
				t.Fatalf("accepted receipt cannot validate again: %v", next)
			}
		} else if got.GetFailure().GetCode() == c.ReceiptFailure_CODE_UNSPECIFIED {
			t.Fatal("untyped failure")
		}
		if next := Evaluate(encoded(t, fixture()), true); next.GetFailure() != nil {
			t.Fatalf("receipt failure poisoned next incident: %v", next)
		}
	})
}

func TestReceiptPolicyCoversEveryOutcomeClassification(t *testing.T) {
	for _, classification := range wire.StringIn(&c.OutcomeRecord{}, "classification") {
		t.Run(classification, func(t *testing.T) {
			r := fixture()
			r.NativeLedger.OutcomeDocument.Classification = classification
			got := Evaluate(encoded(t, r), true)
			want := classification == "success" || classification == "cleared_without_action"
			if (got.GetFailure() == nil) != want {
				t.Fatalf("classification %s acceptance drifted: %v", classification, got)
			}
		})
	}
}

func TestReceiptAcceptsVerifiedSuccessWithoutDiagnosisEvidence(t *testing.T) {
	r := fixture()
	r.NativeLedger.Closure.Result.ConfirmedRootCauses = nil
	if got := Evaluate(encoded(t, r), true); got.GetFailure() != nil {
		t.Fatal(got.GetFailure())
	}
}
