package runtime

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

var creditEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func creditAt(minutes int) *timestamppb.Timestamp {
	return timestamppb.New(creditEpoch.Add(time.Duration(minutes) * time.Minute))
}

func creditRef(name string) *contracts.ObjectRef {
	return &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: name}
}

func creditComponent(name string) []*contracts.Binding {
	return []*contracts.Binding{{Name: "component", Object: creditRef(name)}}
}

func creditApplication(id, playbook string, done int) *contracts.PlanApplication {
	return &contracts.PlanApplication{ApplicationId: id, ProgramIds: []string{playbook}, Applied: true, VerificationCompleted: true, VerificationPassed: true, StartedAt: creditAt(done - 1), CompletedAt: creditAt(done)}
}

// creditOutcome is a closure in which the automatic stage applied playbook
// "fix" to Deployment/front and Recovered held on the opening alert "k1".
func creditOutcome(classification string) *contracts.OutcomeRecord {
	application := creditApplication("app-1", "fix", 10)
	return &contracts.OutcomeRecord{
		IncidentId: "i1", Classification: classification,
		RecoveryVerification: &contracts.RecoveryVerification{OpeningKeys: []string{"k1"}},
		PlanApplications:     []*contracts.PlanApplication{application},
		PriorStages: []*contracts.StageReport{{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{
			{PlaybookId: "fix", Bindings: creditComponent("front"), ApplyReceipt: application, Result: "applied_cleared"},
		}}},
	}
}

func TestProgramCreditsCreditTheFinalWriteOnSuccess(t *testing.T) {
	credits := ProgramCreditIndex([]*contracts.OutcomeRecord{creditOutcome("success")})
	if !credits.Credited("fix", "k1", sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "front"}) {
		t.Fatalf("credits = %v, want (k1, Deployment/front) for fix", credits)
	}
	if credits.Credited("fix", "k1", sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}) {
		t.Fatal("a same-Kind component without its own credited success was credited")
	}
	if credits.Credited("fix", "k2", sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "front"}) {
		t.Fatal("a symptom the incident did not open on was credited")
	}
	if !credits.Has("fix") || credits.Has("other") {
		t.Fatalf("Has: %v", credits)
	}
}

func TestProgramCreditsEarnedOnlyByASuccessClosure(t *testing.T) {
	for _, classification := range []string{"escalated", "failed", "unconfirmed", "cleared_without_action"} {
		if credits := ProgramCreditIndex([]*contracts.OutcomeRecord{creditOutcome(classification)}); credits.Has("fix") {
			t.Fatalf("%s closure credited %v", classification, credits)
		}
	}
}

// A coincidental recovery in which another change came last never widens the key.
func TestProgramCreditsRequireTheApplicationToBeTheFinalWrite(t *testing.T) {
	outcome := creditOutcome("success")
	outcome.RepairActions = []*contracts.RepairActionReceipt{{ActionId: "hand-1", Kind: "mitigation-menu", Success: true, StartedAt: creditAt(12), CompletedAt: creditAt(13)}}
	if credits := ProgramCreditIndex([]*contracts.OutcomeRecord{outcome}); credits.Has("fix") {
		t.Fatalf("an application followed by another write was credited: %v", credits)
	}
	// The application's own receipts are not a later write.
	own := creditOutcome("success")
	own.RepairActions = []*contracts.RepairActionReceipt{{ActionId: "sdo-program-app-1", Kind: "program-plan", Success: true, StartedAt: creditAt(9), CompletedAt: creditAt(11)}}
	if credits := ProgramCreditIndex([]*contracts.OutcomeRecord{own}); !credits.Has("fix") {
		t.Fatal("the application's own receipt disqualified it")
	}
}

func TestProgramCreditsNeedAVerifiedComponentBinding(t *testing.T) {
	failed := creditOutcome("success")
	failed.PlanApplications[0].VerificationPassed = false
	rolledBack := creditOutcome("success")
	rolledBack.PlanApplications[0].RolledBack = true
	unrolled := creditOutcome("success")
	unrolled.PriorStages[0].Mitigations[0].Bindings = []*contracts.Binding{{Name: "workload", Object: creditRef("front")}}
	for name, outcome := range map[string]*contracts.OutcomeRecord{"unverified": failed, "rolled back": rolledBack, "no component role": unrolled} {
		if credits := ProgramCreditIndex([]*contracts.OutcomeRecord{outcome}); credits.Has("fix") {
			t.Fatalf("%s application credited %v", name, credits)
		}
	}
}

func TestProgramCreditsAccumulateAcrossIncidentsWithoutDuplicates(t *testing.T) {
	second := creditOutcome("success")
	second.IncidentId = "i2"
	second.PriorStages[0].Mitigations[0].Bindings = creditComponent("api")
	credits := ProgramCreditIndex([]*contracts.OutcomeRecord{creditOutcome("success"), creditOutcome("success"), second})
	if got := len(credits["fix"]); got != 2 {
		t.Fatalf("pairs = %v, want front and api once each", credits["fix"])
	}
}

func TestOutcomeCaseContextNamesOpeningKeysAndSuspects(t *testing.T) {
	outcome := creditOutcome("success")
	outcome.Findings = []*contracts.Finding{
		{PrimaryResource: &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "front"}, RelatedResources: []*contracts.ObjectRef{creditRef("front")}},
		{PrimaryResource: &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "front"}},
	}
	got := outcomeCaseContext(outcome)
	if len(got.OpeningKeys) != 1 || got.OpeningKeys[0] != "k1" || len(got.Suspects) != 2 {
		t.Fatalf("context = %+v, want key k1 and two distinct suspects", got)
	}
}

// A run bound through a credited pair is persisted with the incident's
// request, so its binding source must be a value the wire contract accepts.
func TestCreditedBindingSourceIsAValidProgramRunContract(t *testing.T) {
	run := ProgramRun{ProgramID: "fix", BindingSource: BindingSourceCredited, Verdict: program.Verdict{Result: program.Match}, Bindings: program.Bindings{Objects: program.ObjectBindings{ComponentRole: sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "front"}}}}
	if err := wire.Validate(programRunContract(run)); err != nil {
		t.Fatalf("a credited run is not a valid ProgramRun: %v", err)
	}
	if got := programRunState(programRunContract(run)).BindingSource; got != BindingSourceCredited {
		t.Fatalf("binding source round trip = %q", got)
	}
}
