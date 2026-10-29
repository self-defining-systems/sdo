package runtime

import (
	"context"
	"path/filepath"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/memory"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// The scheduler produces the proof and the real closure/tool consumers must
// agree before and after it. Detector clearing overlaps the stability window.
func TestRecoveryProofSharesClearingWindowAndToolVerdict(t *testing.T) {
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	c.tracker.SetPolicy("health", 2, 10, 0)
	for i := 2; i < 17; i++ {
		step(i)
		got := c.cachedResponderVerification(context.Background(), c.currentIncidentRequest.IncidentID, 0)
		if got.Healthy {
			t.Fatalf("tool granted recovery before 15 seconds at %d", i)
		}
	}
	step(17)
	if c.IncidentOpen() {
		t.Fatal("detector clearing added a second stability window")
	}
	got := c.cachedResponderVerification(context.Background(), c.pendingClosure.Request.IncidentID, 0)
	if !got.Healthy || got.IncidentVerified == nil {
		t.Fatalf("closure proof not shared with tool: %+v", got)
	}
}

func TestRecoveryProofReleasesOutcomeWithoutNarrationGrace(t *testing.T) {
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	c.broker = &recordingIncidentBroker{}
	for i := 2; i <= 17; i++ {
		step(i)
	}
	if c.IncidentOpen() {
		t.Fatal("missing recovery proof")
	}
	step(18)
	if _, ok := c.PendingClosureEffect(); !ok {
		t.Fatal("outcome still waits for final narration")
	}
	if !c.pendingClosure.VerifiedAt.Equal(time.Unix(17, 0)) {
		t.Fatal("proof changed during handoff")
	}
}

// Real executor preparation must carry the previous acceptance baseline into
// the source revision it applies, without accepting unknown object instances.
func TestRecoveryProofSourceRepairRebindsAcceptance(t *testing.T) {
	for _, kind := range []string{"same-source", "new-source", "missing-record", "reused-uid"} {
		t.Run(kind, func(t *testing.T) {
			source := addedFieldsSource()
			live := addedFieldsLive()
			live.SetUID("original")
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			if kind == "new-source" {
				_ = unstructured.SetNestedField(source.Object, "new", "spec", "newField")
			}
			if kind == "missing-record" {
				a := live.GetAnnotations()
				delete(a, acceptedPathsAnnotation)
				live.SetAnnotations(a)
			}
			if kind == "reused-uid" {
				live.SetUID("replacement")
			}
			executor := PlanExecutor{Source: planFakeSource{source: source}}
			repaired, err := executor.prepareAction(context.Background(), IncidentRequest{Namespace: addedFieldsRef().Namespace, SourceCommit: "accepted"}, program.ReconcileFromSource{Ref: addedFieldsRef()}, live)
			if err != nil {
				t.Fatal(err)
			}
			_, err = addedSourceFields(repaired, source)
			wantKnown := kind == "same-source" || kind == "new-source"
			if (err == nil) != wantKnown {
				t.Fatalf("acceptance known=%v want %v (%v)", err == nil, wantKnown, err)
			}
		})
	}
}

type recoveryHandoffDispatcher struct{ requests chan IncidentRequest }

func (d recoveryHandoffDispatcher) PrepareDispatch(_ context.Context, r IncidentRequest) (IncidentRequest, error) {
	r.PreparedAgentTask = "synthetic-residual-task"
	return r, nil
}
func (d recoveryHandoffDispatcher) Dispatch(_ context.Context, r IncidentRequest) (IncidentResult, error) {
	d.requests <- r
	result := completedResult(r.IncidentID)
	result.ResponderSessionID = "residual-session"
	return result, nil
}

// Real executor -> real automatic stage -> controller closure -> real pipeline
// handoff. A program's NoMatch is not clearance of its unrelated health peer.
func TestRecoveryProofStageOnlyResidualReachesResponder(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, ObservationCeiling: time.Millisecond}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage}); err != nil {
		t.Fatal(err)
	}
	if len(state.Findings) == 0 || len(state.PriorStages[0].Mitigations) == 0 {
		t.Fatal("fixture did not produce partial repair")
	}
	now := time.Now().UTC()
	produced, err := responsePipelineResult(state, now.Add(-time.Second), now)
	if err != nil {
		t.Fatal(err)
	}
	d := recoveryHandoffDispatcher{requests: make(chan IncidentRequest, 1)}
	cfg := testControllerConfig()
	cfg.Namespace = "app"
	c, err := NewController(cfg, loop.Detectors, loop.Provider, d, now)
	if err != nil {
		t.Fatal(err)
	}
	c.currentIncidentRequest = cloneIncidentRequest(&state.Request)
	c.currentIncidentRequest.PriorStages = cloneStageReports(state.PriorStages)
	c.incidentOpen = true
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.incidentDispatchedAt = now.Add(-time.Second)
	c.handleDispatchCompletion(dispatchCompletion{incidentID: state.Request.IncidentID, result: produced, priorStages: state.PriorStages}, now)
	if err := c.Step(context.Background(), now.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	effect, ok := c.PendingDispatchEffect()
	if !ok {
		t.Fatal("active health after automatic repair did not hand off to responder")
	}
	if len(effect.Request.PriorStages) == 0 {
		t.Fatal("handoff lost stage receipts")
	}
	if _, _, err := c.runResponsePipeline(context.Background(), effect.Request); err != nil {
		t.Fatal(err)
	}
	got := awaitRequest(t, d.requests)
	if len(got.Findings) == 0 || len(got.PriorStages[0].Mitigations) == 0 {
		t.Fatal("responder lost residuals or repair receipts")
	}
	if _, ok := c.PendingIncidentClosure(); ok {
		t.Fatal("active health became closure")
	}
}

// Scheduler -> shared proof -> closure effect -> native broker -> accepted
// outcome. The responder never returns a final report in this pipeline.
func TestRecoveryProofPublishesNativeOutcomeWithoutFinalReport(t *testing.T) {
	ctx := context.Background()
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	root := nativeRepository(t)
	nativeMemoryFixture(t, root)
	repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), simMemoryStructureValidator)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(18, 0).UTC()
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "scripted", ResponderModel: "none", Now: func() time.Time { return now }}
	workspace, err := native.PrepareIncident(ctx, c.currentIncidentRequest.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	c.currentIncidentRequest.RepairPolicy = "recorded-actions"
	c.currentIncidentRequest.RepositoryWorktree = workspace.Worktree
	c.currentIncidentRequest.RepositoryBaseCommit = workspace.BaseCommit
	c.currentIncidentRequest.SourceCommit = workspace.BaseCommit
	c.currentIncidentRequest.DeployedCommit = workspace.BaseCommit
	c.broker = native
	c.now = func() time.Time { return now }
	for i := 2; i <= 17; i++ {
		now = time.Unix(int64(i), 0).UTC()
		step(i)
	}
	var effect ClosureEffect
	var ok bool
	for i := 18; i <= 48; i++ {
		now = time.Unix(int64(i), 0).UTC()
		step(i)
		if effect, ok = c.PendingClosureEffect(); ok {
			break
		}
	}
	if !ok {
		t.Fatal("narration held native outcome handoff")
	}
	if err := c.ExecuteClosureEffect(ctx, effect); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, c.closureResults)
	completion := <-c.closureResults
	if completion.err != nil {
		t.Fatal(completion.err)
	}
	outcome, err := memory.NewJSONStore(root).Get(ctx, effect.Closure.Request.IncidentID)
	if err != nil || outcome.Record.GetClassification() != "cleared_without_action" {
		t.Fatalf("native outcome classification=%s error=%v", outcome.Record.GetClassification(), err)
	}
	ledger, err := native.readLedger(effect.Closure.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	written := ledger.ClosurePhaseTimestamps["outcome_write"].AsTime()
	clear := effect.Closure.HealthClearedAt
	if clear != nil {
		t.Logf("hermetic pipeline clear-to-verified=%s verified-to-outcome=%s clear-to-outcome=%s", effect.Closure.VerifiedAt.Sub(*clear), written.Sub(effect.Closure.VerifiedAt), written.Sub(*clear))
	}
	if clear == nil || written.Sub(*clear) > 16*time.Second {
		t.Fatalf("clear-to-outcome exceeded shared 15s proof + handoff: clear=%v written=%v", clear, written)
	}
}
