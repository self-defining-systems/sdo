package runtime

import (
	"context"
	"encoding/json"
	"errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"path/filepath"
	"sdo.dev/operator/runtime/lifecyclecore"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

func scriptedUsageCheckpoint(t *testing.T, root, id, task, turn string, input int, complete bool) {
	t.Helper()
	sub, err := memory.IncidentSessionSubPath("", id)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, sub, "usage")
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"version": 1, "incident_id": id, "task_id": task, "turn_id": turn, "phase": "responder", "complete": complete, "usage": map[string]int{"input_tokens": input, "output_tokens": input / 10}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, turn+".json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestResponderUsageSurvivesReportGraceAndRestart(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "first-turn", true: "continuation-restart"}[restart], func(t *testing.T) {
			c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
			c.broker = &recordingIncidentBroker{}
			id := c.currentIncidentRequest.IncidentID
			root := t.TempDir()
			for i := 2; i <= 17; i++ {
				step(i)
			}
			d := KubernetesJobDispatcher{Client: fake.NewSimpleClientset(), Namespace: "control", RepositoryMountPath: root}
			c.dispatcher = d
			scriptedUsageCheckpoint(t, root, id, id, "first", 120, false)
			if restart {
				scriptedUsageCheckpoint(t, root, id, id, "first", 200, true)
				scriptedUsageCheckpoint(t, root, id, id+"-continue-1", "second", 300, false)
				restored, err := NewController(testControllerConfig(), c.detectorList(), c.provider, d, time.Unix(17, 0))
				if err != nil {
					t.Fatal(err)
				}
				restored.broker = &recordingIncidentBroker{}
				if err := restored.RestoreState(exportState(t, c)); err != nil {
					t.Fatal(err)
				}
				c = restored
			}
			c.finishEarlyReport(context.Background(), time.Unix(47, 0))
			closure, ok := c.PendingIncidentClosure()
			want := int64(120)
			if restart {
				want = 500
			}
			if !ok || closure.Result == nil || closure.Result.Usage.InputTokens != want {
				t.Fatalf("retirement lost or duplicated observed usage: %+v", closure.Result)
			}
			if !closure.Result.Usage.Partial || closure.Result.Usage.TotalCostUsd != nil {
				t.Fatal("partial or unknown cost was fabricated")
			}
			derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "scripted", "fixture")
			if err != nil {
				t.Fatal(err)
			}
			if derived.Record.GetUsage().GetInputTokens() != want || !derived.Record.GetUsage().GetPartial() {
				t.Fatal("outcome lost usage")
			}
			c.finishEarlyReport(context.Background(), time.Unix(48, 0))
			closure, _ = c.PendingIncidentClosure()
			if closure.Result.Usage.InputTokens != want {
				t.Fatal("repeated retirement counted checkpoints twice")
			}
		})
	}
}

// A completed result can be replayed after controller restart. Checkpoints
// replace both the per-turn report and durable continuation aggregate.
func TestResponderUsageNormalCompletionAcrossContinuationAndRestart(t *testing.T) {
	client := fake.NewSimpleClientset()
	root := t.TempDir()
	request := goldenRequest(t)
	request.RepositoryWorktree = filepath.Join(root, "proposal")
	request.ResponseDeadline = time.Now().Add(time.Minute)
	request.MaxResponseLoops = 3
	var healthy atomic.Bool
	d := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: root, ServiceAccount: "responder", PollInterval: time.Millisecond, RequireMemorySnapshot: true, Catalog: fixedMemoryCatalog{request.RepositoryBaseCommit}, LoopFactory: func(r IncidentRequest) *ResponderLoop { return continuationLoop(r, &healthy, 3) }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := d.Dispatch(ctx, request); done <- err }()
	first := awaitJob(t, client, IncidentJobName(request.IncidentID))
	if !containsEnvironment(first.Spec.Template.Spec.Containers[0].Env, "SDO_RESPONDER_USAGE_DIR", "/sdo/session/usage") {
		t.Fatal("production worker has no durable usage destination")
	}
	result := completedResult(request.IncidentID)
	result.Status = IncidentFailed
	result.Usage.InputTokens = 100
	result.Usage.OutputTokens = 10
	scriptedUsageCheckpoint(t, root, request.IncidentID, request.IncidentID, "first", 100, true)
	publishContinuationResult(t, client, first, result)
	second := awaitJob(t, client, IncidentJobName(request.IncidentID+"-continue-1"))
	scriptedUsageCheckpoint(t, root, request.IncidentID, request.IncidentID+"-continue-1", "interrupted", 70, false)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	healthy.Store(true)
	// A replacement process resumes the same continuation with a fresh turn
	// identity and turn-local delta; it must retain the old interrupted turn.
	scriptedUsageCheckpoint(t, root, request.IncidentID, request.IncidentID+"-continue-1", "resumed", 130, true)
	result.Status = IncidentCompleted
	result.Usage.InputTokens = 130
	result.Usage.OutputTokens = 13
	publishContinuationResult(t, client, second, result)
	ctx2, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	for replay := 0; replay < 2; replay++ {
		final, err := d.Dispatch(ctx2, request)
		if err != nil {
			t.Fatal(err)
		}
		if final.Usage.InputTokens != 300 || final.Usage.OutputTokens != 30 || !final.Usage.Partial {
			t.Fatalf("restart/replay usage: %v", &final.Usage)
		}
	}
	jobs, err := client.BatchV1().Jobs("demo").List(ctx2, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 2 {
		t.Fatalf("replay allocated another worker: %v %v", len(jobs.Items), err)
	}
}

func TestResponderUsageNormalCompletionUnchanged(t *testing.T) {
	client := fake.NewSimpleClientset()
	root := t.TempDir()
	request := goldenRequest(t)
	request.RepositoryWorktree = filepath.Join(root, "proposal")
	d := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: root, ServiceAccount: "responder", PollInterval: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan IncidentResult, 1)
	errs := make(chan error, 1)
	go func() { result, err := d.Dispatch(ctx, request); done <- result; errs <- err }()
	job := awaitJob(t, client, IncidentJobName(request.IncidentID))
	result := completedResult(request.IncidentID)
	result.Usage.InputTokens = 100
	result.Usage.OutputTokens = 10
	scriptedUsageCheckpoint(t, root, request.IncidentID, request.IncidentID, "completed", 100, true)
	publishContinuationResult(t, client, job, result)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	final := <-done
	if final.Usage.InputTokens != 100 || final.Usage.OutputTokens != 10 || final.Usage.Partial {
		t.Fatalf("completion accounting changed: %v", &final.Usage)
	}
}

func TestResponderUsageFailedWorkerWithoutSession(t *testing.T) {
	c, _, _ := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	request := *c.currentIncidentRequest
	root := t.TempDir()
	c.dispatcher = KubernetesJobDispatcher{RepositoryMountPath: root}
	scriptedUsageCheckpoint(t, root, request.IncidentID, request.IncidentID, "failed", 90, false)
	closure := IncidentClosure{Request: request, DispatchError: "worker failed before result", DetectedAt: time.Unix(1, 0), DispatchedAt: time.Unix(1, 0), ResponderCompletedAt: time.Unix(2, 0), VerifiedAt: time.Unix(2, 0)}
	projected, err := incidentClosureContract(closure)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	_, err = c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Begin, At: time.Unix(2, 0), Closure: projected})
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	frozen, ok := c.PendingIncidentClosure()
	if !ok || frozen.Result == nil || frozen.Result.ResponderSessionID != "" || frozen.Result.Usage.InputTokens != 90 || !frozen.Result.Usage.Partial {
		t.Fatalf("failed worker accounting: %+v", frozen.Result)
	}
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), frozen, "", "scripted", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	if derived.Record.GetUsage().GetInputTokens() != 90 || !derived.Record.GetUsage().GetPartial() {
		t.Fatal("failed outcome dropped usage")
	}
}
