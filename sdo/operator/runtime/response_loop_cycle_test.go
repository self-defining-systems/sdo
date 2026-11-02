package runtime

import (
	"context"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// A response loop is one act→verify cycle (docs/design/playbook-programs.md):
// every write between two verifications shares one loop, and a call that
// writes nothing spends none. run17/run18 lost 13 incidents because each
// mitigate call, including planning failures, was counted as a loop.

func loopCycleMitigate(t *testing.T, loop *ResponderLoop, index int) ToolResponse {
	t.Helper()
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	if menu.Menu == nil {
		t.Fatalf("menu unavailable: %s", menu.Error)
	}
	return loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: plainProgramOption(t, menu.Menu, index%2), RoundLimit: 1}})
}

// SetField without its required path and value fails in planning, before any write.
func loopCycleUnwritable(loop *ResponderLoop) ToolResponse {
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	return loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "SetField", RoundLimit: 1}, Parameters: map[string]string{"ref": "Service/app/backend"}})
}

func loopCycleVerify(loop *ResponderLoop) ToolResponse {
	return loop.Handle(context.Background(), ToolRequest{Command: "verify"})
}

func TestResponseLoopIsOneActVerifyCycle(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.MenuConfig = productionResponderMenu()
	loop.Executor.Verifier = planFakeVerifier{true}
	for i := 0; i < 7; i++ {
		if response := loopCycleMitigate(t, loop, i); response.Report == nil || response.Error != "" {
			t.Fatalf("write %d in the first act phase refused: %s", i+1, response.Error)
		}
	}
	if loop.LoopsUsed() != 1 {
		t.Fatalf("seven writes before one verification used %d loops, want 1", loop.LoopsUsed())
	}
	for cycle := 2; cycle <= 5; cycle++ {
		loopCycleVerify(loop)
		for i := 0; i < 2; i++ {
			if response := loopCycleMitigate(t, loop, i); response.Error != "" {
				t.Fatalf("loop %d write refused: %s", cycle, response.Error)
			}
		}
		if loop.LoopsUsed() != cycle {
			t.Fatalf("after %d act→verify cycles LoopsUsed=%d", cycle, loop.LoopsUsed())
		}
	}
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	if menu.Menu.RemainingBatches != 1 {
		t.Fatalf("the open fifth loop still accepts writes: remaining=%d", menu.Menu.RemainingBatches)
	}
	loopCycleVerify(loop)
	if response := loopCycleMitigate(t, loop, 0); response.Error != "response loop budget exhausted" || loop.LoopsUsed() != 5 {
		t.Fatalf("a sixth act→verify cycle ran: %+v loops=%d", response.Error, loop.LoopsUsed())
	}
}

func TestResponseLoopPlanningFailureSpendsNoLoop(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.MenuConfig = productionResponderMenu()
	before := len(cluster.patches)
	for i := 0; i < 6; i++ {
		response := loopCycleUnwritable(loop)
		if response.Error == "" && (response.Report == nil || response.Report.Status == contracts.MitigationReport_STATUS_SUCCESS) {
			t.Fatalf("incomplete SetField planned: %+v", response)
		}
		loopCycleVerify(loop)
	}
	if loop.LoopsUsed() != 0 || len(cluster.patches) != before {
		t.Fatalf("planning failures spent %d loops (writes %d)", loop.LoopsUsed(), len(cluster.patches)-before)
	}
}

func TestResponseLoopOpenCycleSurvivesRestart(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.MenuConfig = productionResponderMenu()
	loop.Executor.Verifier = planFakeVerifier{true}
	var saved []byte
	loop.saveToolState = func(_ context.Context, payload []byte) error { saved = append([]byte(nil), payload...); return nil }
	loopCycleMitigate(t, loop, 0)
	restored := NewResponderLoop(loop.Request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, 5)
	restored.MenuConfig = loop.MenuConfig
	restored.ReadView = loop.ReadView
	if err := restored.restoreToolSnapshot(saved); err != nil {
		t.Fatal(err)
	}
	loopCycleMitigate(t, restored, 1)
	if restored.LoopsUsed() != 1 {
		t.Fatalf("restart opened a second loop: %d", restored.LoopsUsed())
	}
	loopCycleVerify(restored)
	loopCycleMitigate(t, restored, 0)
	if restored.LoopsUsed() != 2 {
		t.Fatalf("verification after restart did not close the loop: %d", restored.LoopsUsed())
	}
}

// Class guard: for any interleaving of writes, unwritable calls and
// verifications, LoopsUsed equals the number of verification-separated phases
// that wrote, capped at the budget, and writes are refused only past it.
func TestResponseLoopAccountingSweep(t *testing.T) {
	random := rand.New(rand.NewSource(1))
	for trial := 0; trial < 24; trial++ {
		loop, _ := menuB3Loop(t)
		loop.MenuConfig = productionResponderMenu()
		loop.Executor.Verifier = planFakeVerifier{true}
		want, open := 0, false
		var trace []string
		for step := 0; step < 14; step++ {
			switch random.Intn(3) {
			case 0:
				trace = append(trace, "verify")
				loopCycleVerify(loop)
				open = false
			case 1:
				trace = append(trace, "unwritable")
				loopCycleUnwritable(loop)
			default:
				trace = append(trace, "write")
				response := loopCycleMitigate(t, loop, step)
				allowed := open || want < 5
				if allowed != (response.Error != "response loop budget exhausted") {
					t.Fatalf("trial %d %v: allowed=%v error=%q", trial, trace, allowed, response.Error)
				}
				if allowed && !open {
					want, open = want+1, true
				}
			}
			if loop.LoopsUsed() != want {
				t.Fatalf("trial %d %v: LoopsUsed=%d want %d", trial, trace, loop.LoopsUsed(), want)
			}
		}
	}
}

// Producer/consumer: real mitigate calls in one agent turn feed the real
// dispatcher's continuation decision. One act phase of five writes leaves four
// loops, so unresolved findings earn a continuation, not a failed incident.
func TestResponseLoopActPhaseDoesNotEndIncident(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.MenuConfig = productionResponderMenu()
	loop.Executor.Verifier = planFakeVerifier{true}
	request := goldenRequest(t)
	request.ResponseDeadline = time.Now().Add(time.Minute)
	request.RepositoryWorktree = filepath.Join(t.TempDir(), "proposal")
	request.MaxResponseLoops = 5
	loop.Request.IncidentID = request.IncidentID
	unresolved := []sdk.Finding{{DetectorID: "health-objective", RuleID: "deployment-unavailable", Status: sdk.FindingActive, Fingerprint: "health-objective/deployment-unavailable/app/frontend", PrimaryResource: menuDeployment, Summary: "rollout still converging"}}
	verifications := 0
	loop.VerifyResult = func(context.Context, IncidentResult) ToolResponse {
		verifications++
		if verifications == 1 {
			return ToolResponse{Summary: "rollout still converging", Findings: unresolved}
		}
		return ToolResponse{Summary: "healthy", Healthy: true}
	}
	client := fake.NewSimpleClientset()
	d := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(request.RepositoryWorktree), ServiceAccount: "responder", PollInterval: time.Millisecond, LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type reply struct {
		result IncidentResult
		err    error
	}
	done := make(chan reply, 1)
	go func() {
		result, err := d.Dispatch(ctx, request)
		done <- reply{result, err}
	}()
	first := awaitJob(t, client, IncidentJobName(request.IncidentID))
	for i := 0; i < 5; i++ {
		if response := loopCycleMitigate(t, loop, i); response.Error != "" {
			t.Fatalf("act-phase write %d refused: %s", i+1, response.Error)
		}
	}
	result := completedResult(request.IncidentID)
	result.RepairActions = loop.MenuRepairActions()
	publishContinuationResult(t, client, first, result)
	second := awaitJob(t, client, IncidentJobName(request.IncidentID+"-continue-1"))
	publishContinuationResult(t, client, second, result)
	outcome := <-done
	if outcome.err != nil || outcome.result.Status != IncidentCompleted || outcome.result.ResponseLoopExhausted {
		t.Fatalf("one act phase ended the incident: %+v err=%v", outcome.result, outcome.err)
	}
}
