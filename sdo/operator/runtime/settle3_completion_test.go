//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// The actual saved-plan producer reports successful observations without
// mutation receipts. Its consumer must still establish durable recovery proof.
func TestSettlementConsumesSharedCompletionForObservedNoops(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "restored"}[restart], func(t *testing.T) {
			ctx := context.Background()
			ref := databaseAction().Target.Pod
			object := planObject(ref, map[string]any{})
			object.SetUID("synthetic-pod")
			object.SetResourceVersion("1")
			client := &externalTestClient{lifecycleTestClient: &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}}
			p := settle3ObservedProgram{client: client, ref: ref}
			catalog, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
			if err != nil {
				t.Fatal(err)
			}
			request := IncidentRequest{IncidentID: "synthetic-noop", Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions", ResponseDeadline: time.Now().Add(time.Minute)}
			executor := PlanExecutor{Client: client, Rollouts: generalNoRollouts{}, Verifier: generalHealthy{}}
			makeLoop := func() *ResponderLoop {
				loop := NewResponderLoop(request, catalog, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: ref.Namespace}}, nil, executor, 5)
				loop.ReadView = func(sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{} }
				return loop
			}
			loop := makeLoop()
			var saved []byte
			loop.saveToolState = func(_ context.Context, b []byte) error { saved = append([]byte(nil), b...); return nil }
			planned := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: p.Spec().ID, Bindings: program.Bindings{Objects: program.ObjectBindings{"target": ref}}})
			if planned.Error != "" {
				t.Fatal(planned.Error)
			}
			applied := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
			if applied.Error != "" {
				t.Fatal(applied.Error)
			}
			evidence := loop.ApplicationEvidence()
			if len(evidence) != 1 || len(evidence[0].Result.ActionsApplied) != 1 || len(evidence[0].Result.ActionReports) != 3 {
				t.Fatalf("wrong producer evidence: %+v", evidence)
			}
			if restart {
				restored := makeLoop()
				if err := restored.restoreToolSnapshot(saved); err != nil {
					t.Fatal(err)
				}
				loop = restored
			}
			result, stages := loop.SettleRecoveryApplications(ctx, IncidentResult{PlanApplications: evidence}, nil)
			app := result.PlanApplications[0]
			if app.Result.FinalVerificationBasis != "recovery-settled" || app.Result.SettlementRejectionReason != contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_UNSPECIFIED {
				t.Fatalf("observations lost completion: %+v stages=%v", app, stages)
			}
			wire, err := recordedPlanContract(app)
			if err != nil {
				t.Fatal(err)
			}
			outcome := &contracts.OutcomeRecord{SourceCommit: request.SourceCommit, PlanApplications: []*contracts.PlanApplication{wire}, PriorStages: stages}
			for _, stage := range stages {
				for _, attempt := range stage.Mitigations {
					outcome.ProgramRuns = append(outcome.ProgramRuns, attempt.PostApplyDiagnose)
				}
			}
			if stats := ProgramTrustStats([]*contracts.OutcomeRecord{outcome})[p.Spec().ID]; stats.VerifiedSuccesses != 1 || stats.Failures != 0 {
				t.Fatalf("consumer lost completed plan: %+v", stats)
			}
		})
	}
}

type settle3ObservedProgram struct {
	client *externalTestClient
	ref    sdk.ObjectRef
}

func (p settle3ObservedProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "synthetic-observed", Params: []program.ParamSpec{{Name: "target", Kind: "Pod", Required: true}}}
}
func (p settle3ObservedProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	value, _, _ := unstructured.NestedString(p.client.objects[p.ref].Object, "metadata", "labels", "ready")
	if value == "yes" {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match}, nil
}
func (p settle3ObservedProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{
		program.PatchObject{Ref: p.ref, Type: "merge", Patch: json.RawMessage(`{"metadata":{"labels":{"ready":"yes"}}}`)},
		program.EnsureExec{Observe: program.Command{Pod: p.ref, Container: "db", Args: []string{"observe"}}, Ensure: program.Command{Pod: p.ref, Container: "db", Args: []string{"ensure"}}},
		program.WaitFor{Ref: p.ref, Path: "/metadata/labels/ready", Equals: "yes"},
	}}, nil
}

func TestSettlementGuardsCarryTypedVetoWithoutErasingPositiveFacts(t *testing.T) {
	cases := []struct {
		name   string
		reason contracts.SettlementRejectionReason
		change func(*ResponderLoop, *IncidentState)
	}{
		{"dependencies", contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_DEPENDENCY_UNAVAILABLE, func(l *ResponderLoop, _ *IncidentState) { l.Programs = nil }},
		{"snapshot", contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_SNAPSHOT_UNAVAILABLE, func(l *ResponderLoop, _ *IncidentState) { l.Provider = failingSnapshotProvider{} }},
		{"revision", contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_REVISION_MISMATCH, func(_ *ResponderLoop, s *IncidentState) {
			for _, stage := range s.PriorStages {
				for _, attempt := range stage.Mitigations {
					attempt.ProgramRevision = "synthetic-stale"
				}
			}
		}},
		{"partial", contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_INCOMPLETE_EXECUTION, func(l *ResponderLoop, _ *IncidentState) {
			for i := range l.applicationsLog {
				l.applicationsLog[i].Result.ActionReports[0].Status = "partial"
			}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			loop, state := pipelineTestLoop(t, false)
			if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
				t.Fatal(err)
			}
			before, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			test.change(loop, state)
			result, stages := loop.SettleRecoveryApplications(context.Background(), before, state.PriorStages)
			for i, app := range result.PlanApplications {
				if !before.PlanApplications[i].Result.VerificationPassed || !app.Result.VerificationPassed {
					t.Fatal("guard erased proven scoped verification")
				}
				if app.Result.SettlementRejectionReason != test.reason {
					t.Fatalf("reason=%v want=%v", app.Result.SettlementRejectionReason, test.reason)
				}
				for _, stage := range stages {
					for _, attempt := range stage.Mitigations {
						if attempt.GetApplyReceipt().GetApplicationId() == app.ID && attempt.GetApplyReceipt().GetSettlementRejectionReason() != test.reason {
							t.Fatal("receipt lost veto")
						}
					}
				}
				encoded, err := recordedPlanContract(app)
				if err != nil {
					t.Fatal(err)
				}
				restored := planApplicationState(encoded)
				if restored.Result.SettlementRejectionReason != test.reason || !restored.Result.VerificationPassed {
					t.Fatal("round trip lost veto or earlier facts")
				}
			}
			for _, receipt := range result.RepairActions {
				if receipt.Success {
					t.Fatal("vetoed observation earned attribution")
				}
			}
		})
	}
}

func TestSettlementMissingControllerEvidenceCannotCreditReturnedApplication(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
		t.Fatal(err)
	}
	before, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	loop.applicationsLog = nil
	loop.archivedApplications = nil
	loop.applicationBindings = nil
	result, stages := loop.SettleRecoveryApplications(context.Background(), before, state.PriorStages)
	for _, app := range result.PlanApplications {
		if !app.Result.VerificationPassed || app.Result.SettlementRejectionReason != contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_BINDING_INCOMPLETE {
			t.Fatalf("returned claim became proof or lost facts: %+v", app)
		}
	}
	for _, stage := range stages {
		for _, attempt := range stage.Mitigations {
			if attempt.GetApplyReceipt().GetSettlementRejectionReason() != contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_BINDING_INCOMPLETE {
				t.Fatal("missing evidence veto absent from stage receipt")
			}
		}
	}
	for _, receipt := range result.RepairActions {
		if receipt.Success {
			t.Fatal("returned application granted attribution without controller facts")
		}
	}
}

// A typed veto is authoritative even if an older projection still carries
// positive coverage/eligibility. Consumers cannot revive attribution from it.
func TestSettlementVetoIsAuthoritativeAtEveryCreditConsumer(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
		t.Fatal(err)
	}
	before, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	result, stages := loop.SettleRecoveryApplications(context.Background(), before, state.PriorStages)
	closure := IncidentClosure{Request: state.Request, Result: &result, VerifiedAt: time.Now(), FinalDetectorStates: []DetectorEvaluation{{DetectorID: loop.Detectors[0].Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: time.Now()}}, Frozen: true}
	closure.Request.PriorStages = stages
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	if !coveredRepeatCandidate(derived.Record) {
		t.Fatal("producer did not create covered success")
	}
	fixed := map[string]program.Result{}
	for _, run := range derived.Record.ProgramRuns {
		fixed[matchedViewKey(run.GetProgramId(), programBindingsState(run.GetBindings()))] = program.NoMatch
	}
	derived.Record.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Causes: classifyCauseWork(derived.Record, fixed)}

	for value, name := range contracts.SettlementRejectionReason_name {
		reason := contracts.SettlementRejectionReason(value)
		if reason == contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_UNSPECIFIED {
			continue
		}
		t.Run(name, func(t *testing.T) {
			outcome := proto.Clone(derived.Record).(*contracts.OutcomeRecord)
			app := outcome.PlanApplications[0]
			app.SettlementRejectionReason = reason
			if finalApplicationVerified(app) || coveredRepeatCandidate(outcome) {
				t.Fatal("historical observations bypassed settlement veto")
			}
			for _, id := range app.ProgramIds {
				if stats := ProgramTrustStats([]*contracts.OutcomeRecord{outcome})[id]; stats.VerifiedSuccesses != 0 {
					t.Fatalf("stale covered eligibility revived rejected application: %+v", stats)
				}
			}
		})
	}
}
