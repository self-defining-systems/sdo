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
	if failure, failed := controller.ClosureFailure(); failed {
		t.Fatalf("a failed attempt's closure waited for its reflection: %+v", failure)
	}
	ack, ok := controller.PendingClosureAcknowledgmentEffect()
	if !ok {
		t.Fatal("failed attempt's closure was not acknowledgeable on its outcome commit")
	}
	if err := controller.ExecuteClosureAcknowledgmentEffect(context.Background(), ack); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, controller.acknowledgmentResults)
	controller.processBrokerCompletions()
	// The queued reflection meets the terminal Job once; the next pass records
	// it as abandoned with the Job's evidence instead of creating another Job.
	for range 2 {
		controller.driveQueuedReflections(context.Background(), now)
		awaitBrokerResult(t, controller.reflectionResults)
		controller.processBrokerCompletions()
	}
	if len(logged) != 1 || !strings.Contains(logged[0], "process queued reflection") {
		t.Fatalf("queued reflection failure not reported once: %v", logged)
	}
	for _, want := range logEvidence {
		if !strings.Contains(logged[0], want) {
			t.Errorf("controller log lost terminal evidence %q: %v", want, logged)
		}
	}
	if ids := controller.QueuedReflectionIncidentIDs(); len(ids) != 0 {
		t.Fatalf("terminal reflection stayed queued: %v", ids)
	}
	ledger, err := native.readLedger(workspace.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if !ledger.ReflectionProcessed || !strings.HasPrefix(ledger.ReflectionSkippedReason, "queued_reflection_abandoned") {
		t.Fatalf("abandoned reflection not recorded: %v", ledger.ReflectionSkippedReason)
	}
	for _, want := range diagnostics {
		if !strings.Contains(ledger.ReflectionSkippedReason, want) {
			t.Errorf("abandon reason missing %q: %s", want, ledger.ReflectionSkippedReason)
		}
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
