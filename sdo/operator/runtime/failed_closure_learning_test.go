package runtime

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Three synthetic faults: two independently clear; one health blocker remains.
// No responder verify command or installed playbook supplies the repair proof.
func failedThreeFaultClosure(t *testing.T) IncidentClosure {
	t.Helper()
	return failedThreeFaultClosureWithClearing(t, 2)
}

func failedThreeFaultClosureWithClearing(t *testing.T, clearing int) IncidentClosure {
	t.Helper()
	var detectors []sdk.Detector
	for _, name := range []string{"first", "second", "blocker"} {
		finding := stateFinding(name)
		finding.Severity = sdk.SeverityCritical
		finding.PrimaryResource = sdk.ObjectRef{Kind: "Service", Namespace: "demo", Name: name}
		d := controllerDetector(name, time.Second, finding, finding, sdk.Finding{}, sdk.Finding{}, sdk.Finding{})
		if name == "blocker" {
			d.samples = [][]sdk.Finding{{finding}}
		}
		d.spec.Class, d.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
		d.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: clearing}
		d.spec.OriginatingCommit = "objective"
		d.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
		detectors = append(detectors, d)
	}
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	config := testControllerConfig()
	config.VerificationTimeout = 2 * time.Second
	c, err := NewController(config, detectors, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan IncidentClosure, 1)
	c.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }
	for i := 0; i < 2; i++ {
		if err := c.Step(context.Background(), time.Unix(int64(i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	executePendingEffect(t, c)
	awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, c)
	if err := c.Step(context.Background(), time.Unix(2, 0), nil); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	for _, finding := range c.currentIncidentRequest.Findings {
		ref := objectRefContract(finding.PrimaryResource)
		c.currentIncidentResult.ConfirmedRootCauses = append(c.currentIncidentResult.ConfirmedRootCauses, ConfirmedRootCause{Summary: finding.PrimaryResource.Name, Resources: []sdk.ObjectRef{finding.PrimaryResource}})
		action := &contracts.RepairActionReceipt{ActionId: "sdo-act-" + ref.Name, Target: "Service/demo/" + ref.Name, Summary: "synthetic repair", Details: "patched and read back", Kind: "kubectl-patch", Resources: []*contracts.ObjectRef{ref}, Success: true, StartedAt: timestamppb.New(time.Unix(1, 0)), CompletedAt: timestamppb.New(time.Unix(2, 0))}
		if ref.Name == "second" {
			action.Kind = "kubectl-exec"
		}
		c.currentIncidentResult.RepairActions = append(c.currentIncidentResult.RepairActions, incidentResultState(&contracts.IncidentResult{RepairActions: []*contracts.RepairActionReceipt{action}}).RepairActions[0])
	}
	c.mu.Unlock()
	for i := 3; i <= 4; i++ {
		if err := c.Step(context.Background(), time.Unix(int64(i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case closure := <-closed:
		return closure
	default:
		t.Fatal("failed incident never closed")
		return IncidentClosure{}
	}
}

func TestFailedClosureRetainsTwoConfirmedRepairs(t *testing.T) {
	closure := failedThreeFaultClosure(t)
	if len(closure.FinalDetectorStates) != 0 || closure.HealthClearedAt != nil {
		t.Fatal("failed closure claimed global health")
	}
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	view := partialReflectionOutcome(derived.Record)
	if view.Classification != "failed" || len(view.ConfirmedRootCauses) != 2 || len(view.RepairActions) != 2 {
		t.Fatalf("lost independently confirmed repairs: %v", view)
	}
	for _, cause := range view.ConfirmedRootCauses {
		if cause.Summary == "blocker" {
			t.Fatal("still firing cause admitted")
		}
	}
}

func TestFailedClosureReflectionReceivesExactlyVerifiedCauses(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), ResponderBackend: "test", ResponderModel: "test", ForceReflection: true, Deriver: ProtoOutcomeDeriver{}}
	closure := failedThreeFaultClosure(t)
	workspace, err := native.PrepareIncident(ctx, closure.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	closure.Request.RepositoryWorktree = workspace.Worktree
	closure.Request.RepositoryBaseCommit = workspace.BaseCommit
	// The standalone controller fixture uses symbolic source IDs. Crossing the
	// real broker boundary requires the accepted repository's immutable commit.
	closure.Request.SourceCommit = workspace.BaseCommit
	closure.Request.RepairPolicy = "recorded-actions"
	calls := 0
	native.Reflector = RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.OpenReader(root), Timeout: time.Minute, SessionMode: "fresh", Executor: reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		calls++
		request := new(contracts.ReflectionRequest)
		if err := task.Request.UnmarshalTo(request); err != nil {
			t.Fatal(err)
		}
		if request.Outcome.Classification != "failed" || len(request.Outcome.ConfirmedRootCauses) != 2 || len(request.Outcome.RepairActions) != 2 || len(request.Closure.Result.ConfirmedRootCauses) != 2 || len(request.CauseWork) != 2 || len(validate.VerifiedReflectionCauses(request.Outcome)) != 2 {
			t.Fatalf("incorrect filtered reflection input: %v", request)
		}
		if len(request.UnassignedWork) != 1 {
			t.Fatalf("missing unresolved-cause question: %v", request)
		}
		// Prose cannot cover recorded hand repairs, so the shared check rejects
		// this answer. Its diagnostics show which repairs carry authority.
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "filtered learning view retains two independently verified causes"})
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
	})}
	if _, err := closeAndReflect(t, ctx, native, closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger(closure.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if calls == 0 || calls != len(ledger.ReflectionRejections) || !ledger.ReflectionRejected || ledger.OutcomeDocument.Classification != "failed" || len(ledger.OutcomeDocument.ConfirmedRootCauses) != 3 {
		t.Fatalf("durable outcome altered or rejection not recorded: calls=%d %v", calls, ledger)
	}
	for _, rejection := range ledger.ReflectionRejections {
		var named []string
		for _, d := range rejection.Diagnostics {
			if d.Rule != validate.RuleReflectionProgramRequired {
				t.Fatalf("unexpected rejection: %v", d)
			}
			named = append(named, d.Message)
		}
		text := strings.Join(named, "\n")
		if len(named) != 2 || !strings.Contains(text, `"sdo-act-first"`) || !strings.Contains(text, `"sdo-act-second"`) || strings.Contains(text, "blocker") {
			t.Fatalf("only independently verified repairs require coverage: %v", named)
		}
	}
}

func TestFailedClosureLearningRejectsUnconfirmedOrContradictoryProof(t *testing.T) {
	closure := failedThreeFaultClosure(t)
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*contracts.OutcomeRecord){
		"pending": func(o *contracts.OutcomeRecord) {
			for _, e := range o.DetectorHistory {
				e.Status = "pending"
			}
		},
		"error": func(o *contracts.OutcomeRecord) {
			for _, e := range o.DetectorHistory {
				e.Status = "error"
			}
		},
		"stale": func(o *contracts.OutcomeRecord) {
			for _, e := range o.DetectorHistory {
				e.EvaluatedAt = timestamppb.New(time.Unix(1, 0))
			}
		},
		"still present": func(o *contracts.OutcomeRecord) {
			for _, e := range o.DetectorHistory {
				e.Status = "firing"
				e.Fingerprints = []string{o.Findings[0].Fingerprint, o.Findings[1].Fingerprint, o.Findings[2].Fingerprint}
			}
		},
		"untrusted receipt": func(o *contracts.OutcomeRecord) {
			for _, a := range o.RepairActions {
				a.ActionId = "worker-claim"
			}
		},
		"late mutation": func(o *contracts.OutcomeRecord) {
			for _, a := range o.RepairActions {
				a.CompletedAt = timestamppb.New(time.Unix(5, 0))
			}
		},
		"no post-response observation": func(o *contracts.OutcomeRecord) { o.DetectorHistory = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			o := proto.Clone(derived.Record).(*contracts.OutcomeRecord)
			mutate(o)
			if got := partialRepairCauses(o); len(got) != 0 {
				t.Fatalf("unverified repairs admitted: %v", got)
			}
		})
	}
}

func TestFailedClosureLearningWaitsForClearingPersistence(t *testing.T) {
	closure := failedThreeFaultClosureWithClearing(t, 4)
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	if got := partialRepairCauses(derived.Record); len(got) != 0 {
		t.Fatalf("raw clear before confirmation admitted: %v", got)
	}
}

func TestFailedClosureLearningSeparatesFindingsFromSameDetector(t *testing.T) {
	closure := failedThreeFaultClosure(t)
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	o := derived.Record
	for _, finding := range o.Findings {
		finding.DetectorId = "shared"
	}
	o.DetectorHistory = []*contracts.DetectorEvaluation{{DetectorId: "shared", Status: "firing", Fingerprints: []string{"blocker"}, EvaluatedAt: timestamppb.New(time.Unix(4, 0))}}
	view := partialReflectionOutcome(o)
	if len(view.ConfirmedRootCauses) != 2 || len(view.RepairActions) != 2 {
		t.Fatalf("peer finding hid confirmed repairs: %v", view)
	}
}

func TestFailedClosureLatestObservationOverridesEarlierVerify(t *testing.T) {
	o := coldPartialRepairOutcome(t)
	o.DetectorHistory = []*contracts.DetectorEvaluation{{DetectorId: "health-objective", Status: "firing", Fingerprints: []string{o.Findings[0].Fingerprint, o.Findings[1].Fingerprint}, EvaluatedAt: timestamppb.New(time.Unix(121, 0))}}
	if got := partialRepairCauses(o); len(got) != 0 {
		t.Fatalf("stale verification overrode unresolved closure finding: %v", got)
	}
}

func TestFailedClosureLearningIncludesControllerProgramReceipt(t *testing.T) {
	closure := failedThreeFaultClosure(t)
	projected, err := incidentClosureContract(closure)
	if err != nil {
		t.Fatal(err)
	}
	restored := incidentClosureState(projected)
	if len(restored.PartialDetectorStates) != 3 {
		t.Fatalf("partial evidence lost across durable closure: %v", projected)
	}
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), restored, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range derived.Record.RepairActions {
		action.Kind = "program-plan"
		action.ActionId = "sdo-program-" + action.ActionId
	}
	if got := partialRepairCauses(derived.Record); len(got) != 2 {
		t.Fatalf("controller-applied verified repairs lost: %v", got)
	}
}

func TestFailedClosureLearningRetainsRepairWhenWorkerFailsAfterHealthClears(t *testing.T) {
	closure := failedThreeFaultClosure(t)
	closure.Result.Status = IncidentFailed
	closure.PartialDetectorStates = nil
	for _, finding := range closure.Request.Findings {
		closure.FinalDetectorStates = append(closure.FinalDetectorStates, DetectorEvaluation{DetectorID: finding.DetectorID, Status: DetectorEvaluationClear, EvaluatedAt: time.Unix(3, 0)})
	}
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	view := partialReflectionOutcome(derived.Record)
	if view.Classification != "failed" || len(view.ConfirmedRootCauses) != 3 || len(view.RepairActions) != 3 {
		t.Fatalf("failed worker hid final independent clearance: %v", view)
	}
}

func TestFailedClosurePartialEvidenceSnapshotIsIsolated(t *testing.T) {
	closure := failedThreeFaultClosure(t)
	controller := &Controller{pendingClosure: &closure}
	first, ok := controller.PendingIncidentClosure()
	if !ok {
		t.Fatal("closure not pending")
	}
	first.PartialDetectorStates[0].Status = DetectorEvaluationClear
	first.PartialDetectorStates[0].Fingerprints[0] = "caller-mutated"
	second, ok := controller.PendingIncidentClosure()
	if !ok {
		t.Fatal("closure no longer pending")
	}
	if second.PartialDetectorStates[0].Status != DetectorEvaluationFiring || second.PartialDetectorStates[0].Fingerprints[0] != "blocker" {
		t.Fatal("caller changed controller-owned partial proof through an exported snapshot")
	}
}
