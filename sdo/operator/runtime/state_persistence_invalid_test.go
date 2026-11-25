package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/runtime/lifecyclecore"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type exitingScriptClient struct {
	TypedPlanClient
	exit int32
}

func (exitingScriptClient) ScriptReady() error { return nil }

func (c exitingScriptClient) RunScript(_ context.Context, request ScriptExecutionRequest) (ScriptExecutionResult, error) {
	return ScriptExecutionResult{ApplicationID: request.ApplicationID, JobName: "synthetic-job", JobUID: "synthetic-uid", ScriptDigest: program.ScriptDigest(request.Script), ExitCode: &c.exit}, nil
}

func failedScriptWorld(t *testing.T) (*Controller, *settlingWorld) {
	t.Helper()
	exit := int32(3)
	c, world, _ := automaticRecoveryWorld(t, settlingCase{scriptExit: &exit, testMitigate: func() (program.Plan, error) {
		return program.Plan{Actions: []program.Action{program.RunScript{Script: "exit 3", Reason: "repair synthetic fault"}}}, nil
	}}, settings.AfterPlaybookAgent)
	c.config.ExternalIncidentsOnly = false
	c.stateStore = &memoryStateStore{}
	return c, world
}

func persist(t *testing.T, c *Controller) *contracts.ControllerState {
	t.Helper()
	if err := c.PersistState(t.Context()); err != nil {
		t.Fatal(err)
	}
	state := c.stateStore.(*memoryStateStore).state
	if err := wire.Validate(state); err != nil {
		t.Fatal(err)
	}
	return state
}

func requireOpen(t *testing.T, c *Controller) {
	t.Helper()
	if !c.IncidentOpen() || c.pendingClosure != nil {
		t.Fatalf("incident open=%v closure=%+v", c.IncidentOpen(), c.pendingClosure)
	}
}

func requireHandoff(t *testing.T, c *Controller, world *settlingWorld) {
	t.Helper()
	for range 10 {
		if c.dispatchState == contracts.DispatchState_DISPATCH_STATE_PENDING {
			break
		}
		autoStep(t, c, world.clock, 15*time.Second)
		persist(t, c)
		requireOpen(t, c)
	}
	if c.dispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING || c.responderDone || !autoReport(c).GetHandoffSelected() {
		t.Fatalf("agent not dispatched: state=%s done=%v report=%v", c.dispatchState, c.responderDone, autoReport(c))
	}
	if c.responseEntry(*c.currentIncidentRequest) != dispatchStagedHandoff {
		t.Fatal("dispatch would run the automatic stage again")
	}
	persist(t, c)
}

func TestFailedScriptKeepsIncidentOpenAndDispatchesAgent(t *testing.T) {
	c, world := failedScriptWorld(t)
	if len(c.currentIncidentResult.RepairActions) == 0 {
		t.Fatal("the failed script has no receipt")
	}
	c.OnError = func(err error) { t.Errorf("a failed script made invalid state: %v", err) }
	persist(t, c)
	requireOpen(t, c)
	autoStep(t, c, world.clock, time.Second)
	if c.dispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING {
		t.Fatalf("one evaluation after the script failed the agent is not dispatched: %s", c.dispatchState)
	}
	requireHandoff(t, c, world)
}

// An invalid record of an applied write is reduced, never dropped.
func TestInvalidRecordOfAWriteIsKept(t *testing.T) {
	c, _ := failedScriptWorld(t)
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	c.currentIncidentResult.RepairActions = append(c.currentIncidentResult.RepairActions, RepairActionReceipt{ActionID: "synthetic-write", Success: true, Resources: []sdk.ObjectRef{ref}})
	report := autoReport(c).GetMitigations()[0].GetApplyReceipt().GetActionReports()[0]
	report.Status = "bogus"
	autoReport(c).GetMitigations()[0].KeptReason = "bogus"
	persist(t, c)
	requireOpen(t, c)
	got := c.currentIncidentResult.RepairActions
	if last := got[len(got)-1]; last.ActionID != "synthetic-write" || last.Success || last.FailureReason != "outcome_unknown" || len(last.Resources) != 1 {
		t.Fatalf("write receipt lost: %+v", got)
	}
	if !playbookWrote(autoReport(c)) {
		t.Fatalf("stage write lost: %v", autoReport(c))
	}
}

func invalidReceipt() RepairActionReceipt {
	return RepairActionReceipt{ActionID: "synthetic-unknown", Kind: "unknown"}
}

func TestInvalidReceiptKeepsIncidentOpenAndDispatchesAgent(t *testing.T) {
	c, world := failedScriptWorld(t)
	valid := len(c.currentIncidentResult.RepairActions)
	c.currentIncidentResult.RepairActions = append(c.currentIncidentResult.RepairActions, invalidReceipt())
	var reported []string
	c.OnError = func(err error) { reported = append(reported, err.Error()) }
	persist(t, c)
	requireOpen(t, c)
	if got := c.currentIncidentResult.RepairActions; len(got) != valid || got[0].Kind != "run-script" {
		t.Fatalf("valid receipts were not kept: %+v", got)
	}
	for _, text := range append(reported, autoReport(c).GetError()) {
		if !strings.Contains(text, "repair_actions[") {
			t.Fatalf("drop not named: %q", text)
		}
	}
	if len(reported) != 1 {
		t.Fatalf("drop reported %d times: %v", len(reported), reported)
	}
	requireHandoff(t, c, world)
}

// A result with no valid core is lost whole; the agent is dispatched at once.
func TestUnstorableStageResultDispatchesAgent(t *testing.T) {
	for name, spoil := range map[string]func(*Controller){
		"invalid status":     func(c *Controller) { c.currentIncidentResult.Status = "bogus" },
		"unencodable result": func(c *Controller) { c.currentIncidentResult.ProposedArtifactIDs = []string{"../outside"} },
	} {
		t.Run(name, func(t *testing.T) {
			c, world := failedScriptWorld(t)
			spoil(c)
			persist(t, c)
			requireOpen(t, c)
			if c.dispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING {
				t.Fatalf("dispatch = %s", c.dispatchState)
			}
			requireHandoff(t, c, world)
		})
	}
}

// The agent runs once per incident: losing its result neither starts it again
// nor closes the incident.
func TestInvalidAgentResultIsNotDispatchedAgain(t *testing.T) {
	c, _ := failedScriptWorld(t)
	autoReport(c).HandoffSelected = true
	c.currentIncidentResult.ResolutionPath = "responder"
	c.currentIncidentResult.ResponderSessionID = "synthetic-session"
	c.currentIncidentResult.Status = "bogus"
	persist(t, c)
	requireOpen(t, c)
	if !c.responderDone || c.dispatchState != contracts.DispatchState_DISPATCH_STATE_COMPLETED || c.currentIncidentResult.ResponderSessionID != "synthetic-session" {
		t.Fatalf("done=%v dispatch=%s result=%+v", c.responderDone, c.dispatchState, c.currentIncidentResult)
	}
}

func TestInvalidStateDoesNotRestartRunningAgent(t *testing.T) {
	c, _ := failedScriptWorld(t)
	c.currentIncidentResult.RepairActions = append(c.currentIncidentResult.RepairActions, invalidReceipt())
	autoReport(c).HandoffSelected = true
	c.responderDone, c.dispatchState = false, contracts.DispatchState_DISPATCH_STATE_RUNNING
	persist(t, c)
	requireOpen(t, c)
	if c.dispatchState != contracts.DispatchState_DISPATCH_STATE_RUNNING {
		t.Fatalf("running agent became %s", c.dispatchState)
	}
}

func TestRestartWithInvalidSavedStateKeepsIncidentAndDispatchesAgent(t *testing.T) {
	c, world := failedScriptWorld(t)
	state := proto.Clone(persist(t, c)).(*contracts.ControllerState)
	state.IncidentResult.RepairActions[0].Target, state.IncidentResult.RepairActions[0].Details = "", ""
	payload, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "state", Namespace: "app", Annotations: map[string]string{stateRevisionAnnotation: "1"}},
		Data:       map[string]string{stateDataKey: string(payload)},
	})
	restarted, err := NewController(c.config, c.responderLoop.Detectors[:1], world, c.dispatcher, world.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	restarted.now, restarted.codePath, restarted.responderLoop = world.clock.Now, c.codePath, c.responderLoop
	var reported []string
	restarted.OnError = func(err error) { reported = append(reported, err.Error()) }
	if err := restarted.AttachStateStore(t.Context(), lifecycleio.NewConfigMapStateStore(client, "app", "state")); err != nil {
		t.Fatal(err)
	}
	requireOpen(t, restarted)
	if restarted.durable != nil || len(reported) != 1 || !strings.Contains(reported[0], "repair_actions[0]") {
		t.Fatalf("durable=%v reported=%v", restarted.durable != nil, reported)
	}
	if err := restarted.PersistState(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := lifecycleio.NewConfigMapStateStore(client, "app", "state").Load(t.Context()); err != nil {
		t.Fatalf("repaired state was not saved: %v", err)
	}
	restarted.stateStore, restarted.stateRevision = &memoryStateStore{}, ""
	requireHandoff(t, restarted, world)
}

// A closure that already began keeps closing: normally when it is valid, with
// the bad receipt dropped when one is not, and as failed when its core is lost.
func TestInvalidStateInABegunClosure(t *testing.T) {
	for name, test := range map[string]struct {
		spoil  func(*IncidentClosure)
		failed bool
	}{
		"valid":           {func(*IncidentClosure) {}, false},
		"invalid receipt": {func(c *IncidentClosure) { c.Result.RepairActions = append(c.Result.RepairActions, invalidReceipt()) }, false},
		"invalid core":    {func(c *IncidentClosure) { c.Result.Status = "bogus" }, true},
	} {
		t.Run(name, func(t *testing.T) {
			c, world := failedScriptWorld(t)
			now := world.clock.Now()
			c.pendingClosure = &IncidentClosure{Request: *cloneIncidentRequest(c.currentIncidentRequest), Result: cloneIncidentResult(c.currentIncidentResult),
				DetectedAt: now, DispatchedAt: now, ResponderCompletedAt: now, VerifiedAt: now, HealthClearedAt: &now}
			c.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
			c.clearIncidentLocked()
			test.spoil(c.pendingClosure)
			state := persist(t, c)
			if state.GetIncidentOpen() || state.GetPendingClosure() == nil || state.GetClosureState() != contracts.ClosureState_CLOSURE_STATE_PENDING {
				t.Fatalf("closure lost: %v", state)
			}
			if got := state.GetPendingClosure().GetStatePersistenceFailure() != nil; got != test.failed {
				t.Fatalf("failed closure = %v, want %v", got, test.failed)
			}
			if !test.failed && len(state.GetPendingClosure().GetResult().GetRepairActions()) == 0 {
				t.Fatal("valid receipts lost")
			}
		})
	}
}

func TestPersistenceInvalidNeverBeginsAClosure(t *testing.T) {
	state := &contracts.ControllerState{IncidentOpen: true, IncidentRequest: &contracts.IncidentRequest{IncidentId: "synthetic"}}
	next, _ := lifecyclecore.Step(state, lifecyclecore.Event{Kind: lifecyclecore.PersistenceInvalid, Closure: &contracts.IncidentClosure{
		Request: &contracts.IncidentRequest{IncidentId: "synthetic"}, StatePersistenceFailure: &contracts.StatePersistenceFailure{}}})
	if next.GetPendingClosure() != nil || !next.GetIncidentOpen() {
		t.Fatalf("open incident was closed: %v", next)
	}
}

// Every receipt the runtime builds must be storable, whatever happened to its
// application.
func TestEveryProgramReceiptIsValid(t *testing.T) {
	at := time.Unix(1_000, 0).UTC()
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	script := program.RunScript{Script: "true", Reason: "repair"}
	reasons := wire.StringIn(&contracts.ActionReport{}, "failure_reason")
	exit := int32(3)
	count := 0
	for _, action := range []program.Action{script, program.RestartRollout{Ref: ref, Reason: "repair"}, program.ReconcileFromSource{Ref: ref}, program.DeletePod{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-1"}}} {
		for _, status := range wire.StringIn(&contracts.ActionReport{}, "status") {
			for _, reason := range reasons {
				for _, ran := range []bool{false, true} {
					for _, change := range []func(*RecordedPlanApplication){
						func(*RecordedPlanApplication) {},
						func(a *RecordedPlanApplication) { a.Pending, a.CompletedAt = true, time.Time{} },
						func(a *RecordedPlanApplication) { a.RolledBack = true },
						func(a *RecordedPlanApplication) { a.RollbackError = "refused" },
						func(a *RecordedPlanApplication) { a.Error = "lost" },
						func(a *RecordedPlanApplication) { a.CompletedAt = at.Add(-time.Second) },
					} {
						report := PlanActionReport{Status: status, FailureReason: reason}
						if ran {
							report.ExitCode = &exit
							report.Script = &ScriptExecutionResult{ScriptDigest: program.ScriptDigest(script.Script), Scope: []sdk.ObjectRef{ref}}
						}
						application := RecordedPlanApplication{ID: "synthetic", ProgramIDs: []string{"fix"}, StartedAt: at, CompletedAt: at,
							Result: PlanApplicationResult{Applied: true, Plan: program.Plan{Actions: []program.Action{action}}, ActionsApplied: []program.Action{action}, ActionReports: []PlanActionReport{report}}}
						change(&application)
						receipts := ProgramRepairReceipts([]RecordedPlanApplication{application, application}, nil)
						count += len(receipts)
						result := completedResult("synthetic")
						result.RepairActions = receipts
						message, err := incidentResultContract(result)
						if err != nil {
							t.Fatal(err)
						}
						if err := wire.Validate(message); err != nil {
							t.Fatalf("%T status=%s reason=%q ran=%v: %v", action, status, reason, ran, err)
						}
					}
				}
			}
		}
	}
	if count == 0 {
		t.Fatal("no receipt was built")
	}
	loop := &ResponderLoop{Request: IncidentRequest{IncidentID: "synthetic"}, sessionID: "synthetic", Now: func() time.Time { return at }}
	for _, reason := range []string{"incident_verified", "execution_failed"} {
		if err := wire.Validate(loop.rejectedReceipt("apply", ToolResponse{FailureReason: reason, Error: reason}, at)); err != nil {
			t.Fatal(err)
		}
	}
}
