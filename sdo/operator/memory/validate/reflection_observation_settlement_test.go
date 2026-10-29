package validate

import (
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func confirmedLifecycleOutcome(kind string) *contracts.OutcomeRecord {
	o := repairCoverageOutcome()
	ref := &contracts.ObjectRef{ApiVersion: "example.test/v1", Kind: kind, Namespace: "app", Name: "policy"}
	a := o.PlanApplications[0]
	a.StartedAt = timestamppb.New(time.Unix(99, 0))
	a.Error = "wait for the cache to observe plan writes: write observation unknown: context deadline exceeded"
	a.VerificationCompleted, a.VerificationPassed = false, false
	action := &contracts.Action{Kind: &contracts.Action_DeleteObject{DeleteObject: &contracts.DeleteObject{Ref: ref}}}
	a.Plan.Actions, a.ActionsApplied = []*contracts.Action{action}, []*contracts.Action{proto.Clone(action).(*contracts.Action)}
	a.ActionReports = []*contracts.ActionReport{{Status: "applied"}}
	a.ObjectEffects = []*contracts.PlanObjectEffect{{Operation: "delete", Ref: ref, Confirmed: true, Irreversible: true}}
	o.RepairActions[0].Resources = []*contracts.ObjectRef{ref}
	stage := o.PriorStages[0].Mitigations[0]
	stage.Result, stage.Error, stage.ProgramRevision = "rollout_failed", a.Error, "revision"
	stage.ClearedFindingFingerprints = []string{"own"}
	stage.PostApplyDiagnose = &contracts.ProgramRun{ProgramId: "base", ProgramRevision: "revision", Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}}
	o.FinalHealthDetectorState = []*contracts.DetectorEvaluation{{Status: "clear"}}
	o.Timestamps = &contracts.OutcomeTimestamps{VerifiedAt: timestamppb.New(time.Unix(102, 0))}
	stage.ApplyReceipt = proto.Clone(a).(*contracts.PlanApplication)
	return o
}

func TestReflectionConfirmedLifecycleObservationFailureIsNotStale(t *testing.T) {
	for _, kind := range []string{"ResourceQuota", "LimitRange", "SyntheticPolicy"} {
		t.Run(kind, func(t *testing.T) {
			o := confirmedLifecycleOutcome(kind)
			before := proto.Clone(o)
			ds := ReflectionRepairDiagnostics(repairTree(t, "peer"), o, &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "covered"}, nil)
			if len(ds) != 0 {
				t.Fatalf("confirmed effect and cleared diagnosis require a program rewrite: %v", ds)
			}
			if !proto.Equal(before, o) {
				t.Fatal("validation changed immutable outcome")
			}
		})
	}
}

func TestReflectionObservationSettlementRequiresCompleteIndependentProof(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*contracts.OutcomeRecord)
	}{
		{"failed closure", func(o *contracts.OutcomeRecord) { o.Classification = "failed" }},
		{"no final verification", func(o *contracts.OutcomeRecord) { o.Timestamps = nil }},
		{"verification before write", func(o *contracts.OutcomeRecord) { o.Timestamps.VerifiedAt = timestamppb.New(time.Unix(98, 0)) }},
		{"no final detectors", func(o *contracts.OutcomeRecord) { o.FinalHealthDetectorState = nil }},
		{"unresolved detector", func(o *contracts.OutcomeRecord) { o.FinalHealthDetectorState[0].Status = "pending" }},
		{"no cleared finding", func(o *contracts.OutcomeRecord) { o.PriorStages[0].Mitigations[0].ClearedFindingFingerprints = nil }},
		{"remaining own finding", func(o *contracts.OutcomeRecord) {
			o.PriorStages[0].Mitigations[0].RemainingFindingFingerprints = []string{"own"}
		}},
		{"unknown diagnosis", func(o *contracts.OutcomeRecord) {
			o.PriorStages[0].Mitigations[0].PostApplyDiagnose.Verdict.Result = contracts.Verdict_RESULT_UNKNOWN
		}},
		{"foreign diagnosis", func(o *contracts.OutcomeRecord) { o.PriorStages[0].Mitigations[0].PostApplyDiagnose.ProgramId = "peer" }},
		{"other revision", func(o *contracts.OutcomeRecord) {
			o.PriorStages[0].Mitigations[0].PostApplyDiagnose.ProgramRevision = "other"
		}},
		{"other diagnosis binding", func(o *contracts.OutcomeRecord) {
			o.PriorStages[0].Mitigations[0].PostApplyDiagnose.Bindings = []*contracts.Binding{{Name: "foreign"}}
		}},
		{"unconfirmed effect", func(o *contracts.OutcomeRecord) { o.PlanApplications[0].ObjectEffects[0].Confirmed = false }},
		{"wrong effect resource", func(o *contracts.OutcomeRecord) {
			o.PlanApplications[0].ObjectEffects[0].Ref = &contracts.ObjectRef{Kind: "ResourceQuota", Name: "other"}
		}},
		{"unknown effect state", func(o *contracts.OutcomeRecord) { o.PlanApplications[0].ObjectEffects[0].State = "unknown" }},
		{"incomplete action reports", func(o *contracts.OutcomeRecord) { o.PlanApplications[0].ActionReports = nil }},
		{"failed action", func(o *contracts.OutcomeRecord) { o.PlanApplications[0].ActionReports[0].Status = "partial" }},
		{"missing applied action", func(o *contracts.OutcomeRecord) { o.PlanApplications[0].ActionsApplied = nil }},
		{"write refused", func(o *contracts.OutcomeRecord) { o.PlanApplications[0].Applied = false }},
		{"execution failure", func(o *contracts.OutcomeRecord) { o.PlanApplications[0].Error = "command exited 1" }},
		{"dry run", func(o *contracts.OutcomeRecord) { o.PlanApplications[0].DryRun = true }},
		{"rollback", func(o *contracts.OutcomeRecord) { o.PlanApplications[0].RolledBack = true }},
		{"pending rollout", func(o *contracts.OutcomeRecord) {
			o.PlanApplications[0].RolloutPending = &contracts.RolloutPending{Reason: "blocked"}
		}},
		{"missing final application", func(o *contracts.OutcomeRecord) { o.PlanApplications = nil }},
		{"ambiguous application", func(o *contracts.OutcomeRecord) {
			o.PlanApplications = append(o.PlanApplications, proto.Clone(o.PlanApplications[0]).(*contracts.PlanApplication))
		}},
		{"later hand repair", func(o *contracts.OutcomeRecord) {
			a := proto.Clone(o.RepairActions[0]).(*contracts.RepairActionReceipt)
			a.ActionId = "hand"
			a.Kind = "exec"
			a.StartedAt = timestamppb.New(time.Unix(101, 0))
			a.CompletedAt = a.StartedAt
			o.RepairActions = append(o.RepairActions, a)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := confirmedLifecycleOutcome("SyntheticPolicy")
			test.change(o)
			ds := ReflectionRepairDiagnostics(repairTree(t, "peer"), o, &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "covered"}, nil)
			if !slices.ContainsFunc(ds, func(d Diagnostic) bool { return d.Rule == RuleReflectionStalePlaybook }) {
				t.Fatalf("missing stale refusal: %v", ds)
			}
		})
	}
}
