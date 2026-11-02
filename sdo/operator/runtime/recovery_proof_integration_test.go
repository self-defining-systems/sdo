//go:build integration

package runtime

import (
	"context"
	"path/filepath"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/memory"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// Real executor -> real automatic stage -> controller closure -> real pipeline
// handoff. A program's NoMatch is not clearance of its unrelated health peer.
func TestRecoveryProofStageOnlyResidualReachesResponder(t *testing.T) {
	for _, phase := range []string{"stage-end", "verification-timeout"} {
		t.Run(phase, func(t *testing.T) {
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
			if phase == "verification-timeout" {
				c.verificationDeadline = now
			}
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

		})
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

