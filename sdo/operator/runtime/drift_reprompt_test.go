package runtime

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

func TestHealthyDriftRepromptOnceAcrossRestart(t *testing.T) {
	for _, mode := range []string{"remaining", "repaired", "no-drift", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client := fake.NewSimpleClientset()
			request := goldenRequest(t)
			request.RepositoryWorktree = filepath.Join(t.TempDir(), "proposal")
			request.ResponseDeadline = time.Now().Add(time.Minute)
			request.MaxResponseLoops = 1 // the drift turn is outside the mitigation/response budget
			var healthy atomic.Bool
			healthy.Store(true)
			drift := []sdk.Finding{{DetectorID: "drift.source", RuleID: "different", PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "demo", Name: "api"}, Summary: "target differs"}}
			verification := ToolResponse{Command: "verify", Healthy: true, Summary: "controller independently verified healthy", Drift: drift}
			factory := func(r IncidentRequest) *ResponderLoop {
				loop := continuationLoop(r, &healthy, 1)
				loop.VerifyResult = func(context.Context, IncidentResult) ToolResponse { return verification }
				return loop
			}
			dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(request.RepositoryWorktree), ServiceAccount: "responder", PollInterval: time.Millisecond, LoopFactory: factory}
			// The initial response still returns to the controller for independent closure.
			results := make(chan IncidentResult, 1)
			errs := make(chan error, 1)
			go func() { r, e := dispatcher.Dispatch(ctx, request); results <- r; errs <- e }()
			first := awaitJob(t, client, IncidentJobName(request.IncidentID))
			forged := completedResult(request.IncidentID)
			forged.DriftReprompt = &contracts.DriftReprompt{FindingsBefore: 999}
			publishContinuationResult(t, client, first, forged)
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
			if initial := <-results; initial.DriftReprompt != nil {
				t.Fatal("worker authored controller drift evidence")
			}
			now := time.Now()
			if mode == "no-drift" {
				verification.Drift = nil
			}
			if mode == "deadline" {
				now = request.ResponseDeadline
			}
			resumed, err := dispatcher.ContinueAfterVerification(ctx, request, verification, now)
			want := mode == "remaining" || mode == "repaired"
			if err != nil || resumed != want {
				t.Fatalf("resume=%v want=%v err=%v", resumed, want, err)
			}
			if !want {
				return
			}
			// Crash after persisting the continuation but before dispatch: the exact
			// same turn is rejoined, rather than spending a second drift prompt.
			resumed, err = dispatcher.ContinueAfterVerification(ctx, request, verification, time.Now())
			if err != nil || !resumed {
				t.Fatalf("pending transition was not rejoined: %v %v", resumed, err)
			}
			go func() { r, e := dispatcher.Dispatch(ctx, request); results <- r; errs <- e }()
			second := awaitJob(t, client, IncidentJobName(request.IncidentID+"-continue-1"))
			var feedback responderFeedback
			for _, env := range second.Spec.Template.Spec.Containers[0].Env {
				if env.Name == "SDO_RESPONSE_CONTINUATION" {
					if err := json.Unmarshal([]byte(env.Value), &feedback); err != nil {
						t.Fatal(err)
					}
				}
			}
			if !feedback.Healthy || len(feedback.SourceDifferences) != 1 {
				t.Fatalf("drift feedback lost: %+v", feedback)
			}
			if mode == "repaired" {
				verification.Drift = nil
			}
			publishContinuationResult(t, client, second, completedResult(request.IncidentID))
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
			final := <-results
			after := int32(1)
			if mode == "repaired" {
				after = 0
			}
			if final.DriftReprompt == nil || final.DriftReprompt.FindingsBefore != 1 || final.DriftReprompt.FindingsAfter == nil || *final.DriftReprompt.FindingsAfter != after {
				t.Fatalf("lost drift prompt counts: %+v", final.DriftReprompt)
			}
			if final.Status != IncidentCompleted || final.ResponseLoopExhausted {
				t.Fatalf("drift gated success: %+v", final)
			}
			// A new loop is constructed on every call; all decisions must be durable.
			resumed, err = dispatcher.ContinueAfterVerification(ctx, request, ToolResponse{Healthy: true, Drift: drift}, time.Now())
			if err != nil || resumed {
				t.Fatalf("restart repeated drift prompt: %v %v", resumed, err)
			}
			jobs, err := client.BatchV1().Jobs("demo").List(ctx, metav1.ListOptions{})
			if err != nil || len(jobs.Items) != 2 {
				t.Fatalf("jobs=%v err=%v", jobs, err)
			}
		})
	}
}

// Fresh closure drift is produced by the real controller/source comparison and
// consumed by the real durable continuation dispatcher, through a worker Job.
func TestControllerHealthyDriftRepromptClosesAfterReturn(t *testing.T) {
	for _, mode := range []string{"remaining", "repaired", "deadline", "dispatch-error", "reprompt-failure"} {
		t.Run(mode, func(t *testing.T) {
			f := newClosureDriftFixture(t, true, true, "app")
			f.step(t, 1) // consume the initial worker result, start the stability window
			request := *cloneIncidentRequest(f.c.currentIncidentRequest)
			request.ResponseDeadline = time.Now().Add(time.Minute)
			if mode == "deadline" {
				request.ResponseDeadline = f.start
			}
			f.c.currentIncidentRequest = &request
			if mode == "dispatch-error" {
				f.c.dispatchError = "worker failed"
			}
			var healthy atomic.Bool
			healthy.Store(true)
			client := fake.NewSimpleClientset()
			dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: t.TempDir(), ServiceAccount: "responder", PollInterval: time.Millisecond}
			request.RepositoryWorktree = filepath.Join(dispatcher.RepositoryMountPath, "proposal")
			f.c.currentIncidentRequest = &request
			dispatcher.LoopFactory = func(r IncidentRequest) *ResponderLoop {
				loop := continuationLoop(r, &healthy, 1)
				loop.VerifyResult = func(context.Context, IncidentResult) ToolResponse {
					snapshot, _ := f.c.provider.Snapshot(context.Background())
					source := f.c.verifyClosureSource(context.Background(), f.start.Add(30*time.Second), snapshot, nil)
					return ToolResponse{Command: "verify", Healthy: true, Summary: "controller independently verified healthy", Drift: source.Drift}
				}
				f.c.mu.Lock()
				f.c.responderLoop = loop
				f.c.mu.Unlock()
				return loop
			}
			loop := dispatcher.LoopFactory(request)
			if err := dispatcher.restoreToolState(context.Background(), request, loop); err != nil {
				t.Fatal(err)
			}
			initial, err := incidentResultContract(*f.c.currentIncidentResult)
			if err != nil {
				t.Fatal(err)
			}
			if err := loop.saveContinuation(context.Background(), responderContinuation{Attempts: 1, Done: true, Result: initial}); err != nil {
				t.Fatal(err)
			}
			f.c.dispatcher = dispatcher
			if mode == "repaired" {
				f.restoreSource(t)
			}
			f.step(t, 20)
			if mode != "remaining" && mode != "reprompt-failure" {
				if len(f.closed) != 1 {
					t.Fatalf("unchanged closure timing for %s: closures=%d", mode, len(f.closed))
				}
				return
			}
			if !f.c.IncidentOpen() || f.c.responderDone {
				t.Fatal("healthy drift did not resume responder")
			}
			if mode == "reprompt-failure" {
				f.c.handleDispatchCompletion(dispatchCompletion{incidentID: request.IncidentID, err: &ResponderJobFailedError{JobName: IncidentJobName(request.IncidentID + "-continue-1")}}, f.start.Add(21*time.Second))
				f.settle(t, 22)
				if len(f.closed) != 1 {
					t.Fatal("failed drift turn held closure")
				}
				derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), f.closed[0], "repair", "codex", "fixture")
				if err != nil {
					t.Fatal(err)
				}
				prompt := derived.Record.GetDriftReprompt()
				if prompt == nil || prompt.FindingsBefore != 1 || prompt.FindingsAfter != nil {
					t.Fatalf("failed turn lost scheduled evidence: %+v", prompt)
				}
				return
			}
			executePendingEffect(t, f.c)
			job := awaitJob(t, client, IncidentJobName(request.IncidentID+"-continue-1"))
			// Healthy observations must not cancel this extra turn before its return.
			f.step(t, 21)
			f.step(t, 40)
			if len(f.closed) != 0 {
				t.Fatal("early health closure cancelled drift turn")
			}
			publishContinuationResult(t, client, job, completedResult(request.IncidentID))
			awaitDispatchCompletionQueued(t, f.c)
			f.settle(t, 41)
			if len(f.closed) != 1 || closureOutcome(t, f.closed[0]) != "success" {
				t.Fatalf("drift gated final closure: %+v", f.closed)
			}
			derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), f.closed[0], "repair", "codex", "fixture")
			if err != nil {
				t.Fatal(err)
			}
			prompt := derived.Record.GetDriftReprompt()
			if prompt == nil || prompt.FindingsBefore != 1 || prompt.FindingsAfter == nil || *prompt.FindingsAfter != 1 {
				t.Fatalf("outcome lost drift counts: %+v", prompt)
			}
		})
	}
}

func TestDriftOnlyIncidentGetsOneExtraHealthyTurn(t *testing.T) {
	c, d, step := driftOnlyController(t)
	// Supply the owned-field evidence admitted with a drift-only opening finding.
	// The view performs the same real closure-source comparison as mixed incidents.
	event := c.currentIncidentRequest.DriftEvents[0]
	event.Fields = []sdk.DriftField{{Path: "spec.ports[0].targetPort", Old: "8087", New: "9999", Source: "8087"}}
	c.currentIncidentRequest.DriftEvents[0] = event
	for i := range c.currentIncidentRequest.Findings {
		if strings.HasPrefix(c.currentIncidentRequest.Findings[i].DetectorID, "drift.") {
			c.currentIncidentRequest.Findings[i].Metadata = map[string]any{"drift_event": event}
		}
	}
	c.drift.View = func(sdk.DetectionContext) program.ReadView {
		return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}, diff: program.Diff{Changed: true, Text: "accepted: {} observed: {\"spec\":{\"ports\":[{\"targetPort\":9999}]}}"}}
	}
	for i := 2; i < 10; i++ {
		step(i)
	}
	if !c.IncidentOpen() {
		t.Fatal("drift-only closure skipped initial turn")
	}
	close(d.release)
	awaitDispatchCompletionQueued(t, c)
	step(10)
	// A real durable continuation is seeded with the returned response, as the
	// Kubernetes dispatcher does before publishing completion to the controller.
	request := *cloneIncidentRequest(c.currentIncidentRequest)
	client := fake.NewSimpleClientset()
	var healthy atomic.Bool
	healthy.Store(true)
	loop := continuationLoop(request, &healthy, 1)
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "demo", LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }}
	if err := dispatcher.restoreToolState(context.Background(), request, loop); err != nil {
		t.Fatal(err)
	}
	result, err := incidentResultContract(*c.currentIncidentResult)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.saveContinuation(context.Background(), responderContinuation{Attempts: 1, Done: true, Result: result}); err != nil {
		t.Fatal(err)
	}
	c.dispatcher = dispatcher
	c.responderLoop = loop
	closed := 0
	c.OnIncidentClosed = func(IncidentClosure) { closed++ }
	step(30)
	if c.responderDone || closed != 0 || !loop.driftRepromptPending() {
		t.Fatal("drift-only incident skipped extra turn")
	}
	step(40)
	if closed != 0 {
		t.Fatal("drift-only extra turn closed early")
	}
	// Complete that same turn, then restart the closure decision with drift left.
	state := loop.continuation
	state.Done = true
	if err := loop.saveContinuation(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	c.responderDone = true
	c.responderCompletedAt = time.Unix(40, 0)
	step(60)
	if closed != 1 {
		t.Fatalf("remaining drift gated drift-only closure: %d", closed)
	}
}
