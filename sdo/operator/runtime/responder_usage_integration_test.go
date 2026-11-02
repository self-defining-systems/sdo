//go:build integration

package runtime

import (
	"context"
	"errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

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
