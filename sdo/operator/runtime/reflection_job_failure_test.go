package runtime

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/worker_registry"
)

func TestReflectionFailedJobStopsClosureRetriesWithEvidence(t *testing.T) {
	testReflectionTerminalJob(t, false)
}

func TestReflectionVanishedJobStopsClosureRetriesWithEvidence(t *testing.T) {
	testReflectionTerminalJob(t, true)
}

func testReflectionTerminalJob(t *testing.T, vanished bool) {
	t.Helper()
	logEvidence := []string{"OOMKilled", "exit_code=137"}
	diagnostics := []string{"BackoffLimitExceeded", "failed-worker", "OOMKilled", "137", "memory limit exceeded"}
	if vanished {
		diagnostics = []string{"worker Job vanished"}
		logEvidence = diagnostics
	}
	root := nativeRepository(t)
	repo, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	native := &NativeIncidentBroker{Repository: repo, LedgerRoot: t.TempDir(), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test"}
	workspace, err := native.PrepareIncident(context.Background(), "failed-reflection")
	if err != nil {
		t.Fatal(err)
	}
	closure := socialRepeatClosure(workspace)
	client := fake.NewSimpleClientset()
	registry := testIncidentRegistry(t, "reflector:test")
	agent, err := registry.AgentFor(&contracts.ReflectionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	executor := worker_registry.KubernetesExecutor{Client: client, Namespace: "demo", Registry: registry, RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(filepath.Dir(workspace.Worktree)), PollInterval: time.Millisecond}
	executor.OnPhase = func(ctx context.Context, phase string) error {
		if phase != "reflection_job_created" {
			return nil
		}
		jobs, err := client.BatchV1().Jobs("demo").List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}
		if vanished {
			return client.BatchV1().Jobs("demo").Delete(ctx, jobs.Items[0].Name, metav1.DeleteOptions{})
		}
		job := jobs.Items[0].DeepCopy()
		job.UID = "failed-job"
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "worker failed"}}
		if _, err = client.BatchV1().Jobs("demo").UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
			return err
		}
		_, err = client.CoreV1().Pods("demo").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "failed-worker", OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: job.Name, UID: job.UID}}}, Status: corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137, Message: "memory limit exceeded"}}}}}}, metav1.CreateOptions{})
		return err
	}
	native.Reflector = RegisteredReflectionWorker{AgentID: agent, TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(root), Timeout: time.Minute, TaskStore: &lifecycleio.TaskStore{Root: t.TempDir()}, Executor: executor, SessionExecutor: func(string) (ReflectionTaskExecutor, error) { return executor, nil }}
	now := time.Now()
	controller := newClosureRetryController(t, ClosureRetryPolicy{MaxAttempts: 8}, native, &now)
	controller.pendingClosure = &closure
	var logged []string
	controller.OnError = func(err error) { logged = append(logged, err.Error()) }
	attemptClosure(t, controller)
	failure, ok := controller.ClosureFailure()
	if !ok || !failure.Permanent || failure.Attempts != 1 || failure.Reason != contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_BROKER_ERROR {
		t.Fatalf("terminal Job consumed closure retry budget: %+v", failure)
	}
	for _, want := range diagnostics {
		if !strings.Contains(failure.LastError, want) {
			t.Errorf("closure diagnostic missing %q: %+v", want, failure)
		}
	}
	if len(logged) != 2 || !strings.Contains(logged[0], logEvidence[0]) || !strings.Contains(logged[1], logEvidence[len(logEvidence)-1]) {
		t.Fatalf("controller log lost terminal evidence: %v", logged)
	}
	now = now.Add(time.Hour)
	if _, ok := controller.PendingClosureEffect(); ok {
		t.Fatal("terminal Job re-observed on retry")
	}
	state := exportState(t, controller)
	if state.GetClosureState() == contracts.ClosureState_CLOSURE_STATE_PENDING {
		t.Fatal("terminal worker left closure pending")
	}
	restored := newClosureRetryController(t, ClosureRetryPolicy{}, native, &now)
	if err := restored.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	if _, ok := restored.PendingClosureEffect(); ok {
		t.Fatal("restart retried terminal worker")
	}
	if len(state.FailedClosures) != 1 || !strings.Contains(state.FailedClosures[0].GetFailure().GetLastError(), diagnostics[0]) {
		t.Fatalf("archived diagnostics lost: %v", state.FailedClosures)
	}
	creates := 0
	for _, a := range client.Actions() {
		if a.GetVerb() == "create" && a.GetResource().Resource == "jobs" {
			creates++
		}
	}
	if creates != 1 {
		t.Fatalf("created %d Jobs", creates)
	}
}
