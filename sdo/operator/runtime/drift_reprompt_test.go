package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/proto"
	"path/filepath"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

func TestHealthyDriftRepromptOnceAcrossRestart(t *testing.T) {
	for _, mode := range []string{"remaining", "repaired", "no-drift", "deadline", "verification-error"} {
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
			want := mode == "remaining" || mode == "repaired" || mode == "verification-error"
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
			if mode == "verification-error" {
				verification = ToolResponse{Command: "verify", Error: "snapshot unavailable", Summary: "verification unavailable"}
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
			if mode == "verification-error" {
				if final.DriftReprompt == nil || final.DriftReprompt.FindingsAfter != nil {
					t.Fatalf("failed verification invented after count: %+v", final.DriftReprompt)
				}
				return
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
	for _, mode := range []string{"remaining", "repaired", "deadline", "dispatch-error", "reprompt-failure", "no-progress", "authorization-denied", "failed-result", "cancelled-result"} {
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
			switch mode {
			case "no-progress":
				f.c.currentIncidentResult.Error = "no_progress"
			case "authorization-denied":
				f.c.currentIncidentResult.Error = "authorization_denied_no_progress"
			case "failed-result":
				f.c.currentIncidentResult.Status = IncidentFailed
			case "cancelled-result":
				f.c.currentIncidentResult.Status = IncidentCancelled
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
			before, err := f.client.CoreV1().Services("app").Get(context.Background(), "reservation", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			f.restoreSource(t)
			f.step(t, 22)
			cached := f.c.cachedResponderVerification(context.Background(), request.IncidentID, 0)
			if len(cached.Drift) != 0 {
				t.Fatal("active drift turn froze the verification report after repair")
			}
			if _, err := f.client.CoreV1().Services("app").Update(context.Background(), before, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			f.step(t, 23)
			cached = f.c.cachedResponderVerification(context.Background(), request.IncidentID, 0)
			if len(cached.Drift) != 1 {
				t.Fatal("active drift turn hid recurring source differences")
			}
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

// Observation must keep progressing while a responder owns its tool mutex.
// Both early closure and result collection read optional continuation metadata.
func TestControllerDriftRepromptObservationNeverWaitsForTools(t *testing.T) {
	for _, mode := range []string{"running", "completion"} {
		t.Run(mode, func(t *testing.T) {
			f := newClosureDriftFixture(t, true, true, "app")
			f.step(t, 1)
			var healthy atomic.Bool
			healthy.Store(true)
			loop := continuationLoop(*f.c.currentIncidentRequest, &healthy, 1)
			f.c.responderLoop = loop
			if mode == "running" {
				f.c.responderDone = false
				f.c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
			}
			completion := dispatchCompletion{incidentID: f.c.currentIncidentRequest.IncidentID, result: *cloneIncidentResult(f.c.currentIncidentResult)}
			loop.mu.Lock()
			done := make(chan error, 1)
			go func() {
				if mode == "completion" {
					f.c.handleDispatchCompletion(completion, f.start.Add(20*time.Second))
					done <- nil
					return
				}
				done <- f.c.Step(context.Background(), f.start.Add(20*time.Second), nil)
			}()
			select {
			case err := <-done:
				loop.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				loop.mu.Unlock()
				<-done
				t.Fatal("controller observation waited for a responder tool mutex")
			}
		})
	}
}

// Terminal stops outrank even a pending persisted drift transition after restart.
func TestDriftRepromptDoesNotResumeTerminalState(t *testing.T) {
	for _, pending := range []bool{false, true} {
		for _, reason := range []string{"no_progress", "authorization_denied_no_progress", "worker failed"} {
			t.Run(fmt.Sprintf("%s/pending=%v", reason, pending), func(t *testing.T) {
				request := goldenRequest(t)
				request.RepositoryWorktree = filepath.Join(t.TempDir(), "proposal")
				request.ResponseDeadline = time.Now().Add(time.Minute)
				var healthy atomic.Bool
				healthy.Store(true)
				dispatcher := KubernetesJobDispatcher{Client: fake.NewSimpleClientset(), Namespace: "demo", LoopFactory: func(r IncidentRequest) *ResponderLoop { return continuationLoop(r, &healthy, 1) }}
				loop := dispatcher.LoopFactory(request)
				if err := dispatcher.restoreToolState(context.Background(), request, loop); err != nil {
					t.Fatal(err)
				}
				result, err := incidentResultContract(completedResult(request.IncidentID))
				if err != nil {
					t.Fatal(err)
				}
				result.Error = proto.String(reason)
				if pending {
					result.DriftReprompt = &contracts.DriftReprompt{FindingsBefore: 1}
				}
				state := responderContinuation{Attempts: 1, Done: !pending, Result: result}
				if err := loop.saveContinuation(context.Background(), state); err != nil {
					t.Fatal(err)
				}
				resumed, err := dispatcher.ContinueAfterVerification(context.Background(), request, ToolResponse{Healthy: true, Drift: []sdk.Finding{{DetectorID: "drift.source"}}}, time.Now())
				if err != nil || resumed {
					t.Fatalf("terminal state resumed: %v %v", resumed, err)
				}
				restored := dispatcher.LoopFactory(request)
				if err := dispatcher.restoreToolState(context.Background(), request, restored); err != nil {
					t.Fatal(err)
				}
				if restored.continuation.Done != state.Done || !proto.Equal(restored.continuation.Result, state.Result) {
					t.Fatal("terminal continuation mutated")
				}
			})
		}
	}
}

// Real inventory findings feed the persisted continuation admission boundary.
// Known failed attempts are terminal; unknown effects or another actionable
// difference must retain the opportunity to make progress.
func TestDriftRepromptInventoryProgress(t *testing.T) {
	for _, mode := range []string{"authorization_denied", "execution_failed", "unknown", "unattempted", "unfinished", "success", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			request := goldenRequest(t)
			request.RepositoryWorktree = filepath.Join(t.TempDir(), "proposal")
			request.ResponseDeadline = time.Now().Add(time.Minute)
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "app", Name: "cleanup"}
			drift := NewSourceInventory("app", request.SourceCommit, nil).Findings([]*unstructured.Unstructured{urObject(ref)}, time.Now())
			if len(drift) != 1 {
				t.Fatalf("inventory fixture: %+v", drift)
			}
			result := completedResult(request.IncidentID)
			reason := mode
			if mode == "mixed" {
				reason = "authorization_denied"
				drift = append(drift, sdk.Finding{DetectorID: "drift.source", PrimaryResource: sdk.ObjectRef{Kind: "Service", Name: "api"}})
			}
			stamp := time.Now()
			receipt := RepairActionReceipt{ActionID: "remove-cleanup", Resources: []sdk.ObjectRef{ref}, FailureReason: reason, StartedAt: stamp, CompletedAt: stamp, Success: mode == "success"}
			if mode == "unfinished" {
				receipt.CompletedAt = time.Time{}
			}
			if mode != "unattempted" {
				result.RepairActions = []RepairActionReceipt{receipt}
			}
			var healthy atomic.Bool
			healthy.Store(true)
			dispatcher := KubernetesJobDispatcher{Client: fake.NewSimpleClientset(), Namespace: "demo", LoopFactory: func(r IncidentRequest) *ResponderLoop { return continuationLoop(r, &healthy, 1) }}
			loop := dispatcher.LoopFactory(request)
			if err := dispatcher.restoreToolState(context.Background(), request, loop); err != nil {
				t.Fatal(err)
			}
			encoded, err := incidentResultContract(result)
			if err != nil {
				t.Fatal(err)
			}
			if err := loop.saveContinuation(context.Background(), responderContinuation{Attempts: 1, Done: true, Result: encoded}); err != nil {
				t.Fatal(err)
			}
			resumed, err := dispatcher.ContinueAfterVerification(context.Background(), request, ToolResponse{Healthy: true, Drift: drift}, time.Now())
			want := mode != "authorization_denied" && mode != "execution_failed"
			if err != nil || resumed != want {
				t.Fatalf("resume=%v want=%v err=%v", resumed, want, err)
			}
		})
	}
}
