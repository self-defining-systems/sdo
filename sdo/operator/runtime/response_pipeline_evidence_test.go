package runtime

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type pipelineIdentitylessDetector struct{ pipelineDetector }

func (d pipelineIdentitylessDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := d.pipelineDetector.Detect(ctx, view)
	for i := range findings {
		findings[i].DetectorID = ""
	}
	return findings, err
}

type pipelineLaggingHealthDetector struct{ remaining int }

func (d *pipelineLaggingHealthDetector) Spec() sdk.DetectorSpec {
	spec := (pipelineDetector{}).Spec()
	spec.ID = "traffic-lag"
	spec.Playbooks = nil
	return spec
}

func (d *pipelineLaggingHealthDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := (pipelineDetector{}).Detect(ctx, view)
	if err != nil || len(findings) != 0 {
		return nil, err
	}
	if d.remaining == 0 {
		return nil, nil
	}
	if d.remaining > 0 {
		d.remaining--
	}
	return []sdk.Finding{{RuleID: "lagging-sample", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Summary: "traffic sample is still clearing", Evidence: "previous traffic window still contains errors"}}, nil
}

func TestDeterministicResponseStageCannotClearWithoutIndependentProof(t *testing.T) {
	finding := sdk.Finding{DetectorID: "incident", Fingerprint: "covered", Status: sdk.FindingActive}
	makeAttempt := func() *contracts.StageMitigation {
		return &contracts.StageMitigation{PlaybookId: "repair", ProgramRevision: "revision", ApplyReceipt: &contracts.PlanApplication{Applied: true, VerificationCompleted: true, VerificationPassed: true}, PostApplyDiagnose: &contracts.ProgramRun{ProgramId: "repair", ProgramRevision: "revision", Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}}}
	}
	cases := map[string]func(*contracts.StageMitigation, *ToolResponse){
		"not-applied":     func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.Applied = false },
		"dry-run":         func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.DryRun = true },
		"rolled-back":     func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.RolledBack = true },
		"unverified":      func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.VerificationPassed = false },
		"not-observed":    func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.VerificationCompleted = false },
		"rollback-error":  func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.RollbackError = "unconfirmed" },
		"wrong-program":   func(a *contracts.StageMitigation, _ *ToolResponse) { a.PostApplyDiagnose.ProgramId = "peer" },
		"wrong-revision":  func(a *contracts.StageMitigation, _ *ToolResponse) { a.PostApplyDiagnose.ProgramRevision = "peer" },
		"diagnosis-error": func(a *contracts.StageMitigation, _ *ToolResponse) { a.PostApplyDiagnose.Error = "unavailable" },
		"pending-rollout": func(a *contracts.StageMitigation, _ *ToolResponse) {
			a.ApplyReceipt.RolloutPending = &contracts.RolloutPending{}
		},
		"apply-error":       func(a *contracts.StageMitigation, _ *ToolResponse) { a.ApplyReceipt.Error = "unconfirmed" },
		"observation-error": func(a *contracts.StageMitigation, _ *ToolResponse) { a.Error = "unavailable" },
		"still-match": func(a *contracts.StageMitigation, _ *ToolResponse) {
			a.PostApplyDiagnose.Verdict.Result = contracts.Verdict_RESULT_MATCH
		},
		"unknown": func(a *contracts.StageMitigation, _ *ToolResponse) {
			a.PostApplyDiagnose.Verdict.Result = contracts.Verdict_RESULT_UNKNOWN
		},
		"fresh-firing": func(_ *contracts.StageMitigation, r *ToolResponse) { r.Findings = []sdk.Finding{finding} },
	}
	state := &IncidentState{Findings: []sdk.Finding{finding}}
	clearVerifiedStageTargets(state, makeAttempt(), []string{"covered"}, ToolResponse{})
	if len(state.Findings) != 0 {
		t.Fatal("positive control did not clear a verified finding")
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			attempt, response := makeAttempt(), ToolResponse{}
			change(attempt, &response)
			state := &IncidentState{Findings: []sdk.Finding{finding}}
			clearVerifiedStageTargets(state, attempt, []string{"covered"}, response)
			if len(state.Findings) != 1 {
				t.Fatalf("%s erased an unverified finding", name)
			}
		})
	}
}

type redundantPipelinePortProgram struct{ pipelinePortProgram }

func (redundantPipelinePortProgram) Spec() program.ProgramSpec {
	spec := (pipelinePortProgram{}).Spec()
	spec.ID = "zzz-redundant-port"
	return spec
}

