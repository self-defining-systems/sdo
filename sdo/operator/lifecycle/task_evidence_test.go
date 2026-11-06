package lifecycle_test

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/worker_registry"
)

// worker plays the agent pods of a fake cluster. Each Job gets the worker's view of its session mount, writes a
// Codex rollout under the CODEX_HOME the Job was launched with, and then completes with the fake agents'
// answer or fails, as the pod of a crashed agent would.
type worker struct {
	t      *testing.T
	client *fake.Clientset
	mount  string
	answer func(*contracts.AgentTask) (*contracts.AgentResult, error)
	fail   func(*contracts.AgentTask) bool
}

// install makes the fake cluster run each Job as it is created.
func (w *worker) install(ctx context.Context) {
	w.client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		w.play(ctx, action.(k8stesting.CreateAction).GetObject().(*batchv1.Job))
		return false, nil, nil
	})
}

func (w *worker) play(ctx context.Context, job *batchv1.Job) {
	container := job.Spec.Template.Spec.Containers[0]
	env := map[string]string{}
	for _, variable := range container.Env {
		env[variable.Name] = variable.Value
	}
	session := ""
	for _, volume := range container.VolumeMounts {
		if volume.MountPath == "/sdo/session" {
			session = filepath.Join(w.mount, volume.SubPath)
		}
	}
	request, err := w.client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), "sdo", env["SDO_REQUEST_CONFIGMAP"])
	if err != nil {
		w.t.Error(err)
		return
	}
	task := &contracts.AgentTask{}
	if err := wire.Unmarshal([]byte(request.(*corev1.ConfigMap).Data[worker_registry.TaskDataKey]), task); err != nil {
		w.t.Error(err)
		return
	}
	home := env["CODEX_HOME"]
	if session == "" || !strings.HasPrefix(home, "/sdo/session/") {
		w.t.Errorf("task %s launched without a persistent session home: session mount %q, CODEX_HOME %q", task.GetTaskId(), session, home)
		return
	}
	codex := filepath.Join(session, strings.TrimPrefix(home, "/sdo/session/"))
	rollout := filepath.Join(codex, "sessions", "2026", "11", "06", "rollout-"+task.GetTaskId()+".jsonl")
	if err := os.MkdirAll(filepath.Dir(rollout), 0700); err != nil {
		w.t.Error(err)
		return
	}
	if err := os.WriteFile(rollout, []byte(`{"type":"session_meta","task":"`+task.GetTaskId()+`"}`+"\n"), 0600); err != nil {
		w.t.Error(err)
		return
	}
	// The provider login the worker copies into its session home must not outlive the task.
	if err := os.WriteFile(filepath.Join(codex, "auth.json"), []byte("secret"), 0600); err != nil {
		w.t.Error(err)
		return
	}
	if w.fail(task) {
		job.Status.Failed = 1
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
		return
	}
	result, err := w.answer(task)
	if err != nil {
		w.t.Error(err)
		return
	}
	payload, err := wire.Canonical(result)
	if err != nil {
		w.t.Error(err)
		return
	}
	if err := w.client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("configmaps"), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: env["SDO_RESULT_CONFIGMAP"], Namespace: "sdo"}, Data: map[string]string{worker_registry.ResultDataKey: string(payload)}}, "sdo"); err != nil {
		w.t.Error(err)
		return
	}
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
}

// inMount runs tasks whose checkout lives under the repository mount, as the broker's checkouts do.
type inMount struct {
	kube  worker_registry.KubernetesExecutor
	mount string
}

func (m inMount) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	task = proto.Clone(task).(*contracts.AgentTask)
	task.Workspace.CheckoutPath = path.Join(m.mount, "checkouts", task.GetTaskId())
	return m.kube.Execute(ctx, task)
}

// Every lifecycle agent task persists its session and its request/result to the repository volume, keyed by
// task ID, so a failing judge that is retried leaves each attempt's rollout behind after its pod is gone.
func TestEveryJudgeTaskOfAFailingLifecycleKeepsItsRolloutRequestAndResult(t *testing.T) {
	h, _ := gateHarness(t, clear)
	mount := t.TempDir()
	client := fake.NewSimpleClientset()
	h.coordinator.Workers = inMount{mount: mount, kube: worker_registry.KubernetesExecutor{
		Client: client, Namespace: "sdo", Registry: h.coordinator.Registry, RepositoryPVC: "repo", RepositoryMountPath: mount,
		PollInterval: 5 * time.Millisecond, Environment: map[string]string{"CODEX_HOME": "/tmp/codex"},
	}}
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	judgeRuns := 0
	var mu sync.Mutex
	play := &worker{t: t, client: client, mount: mount,
		answer: func(task *contracts.AgentTask) (*contracts.AgentResult, error) { return h.workers.Execute(ctx, task) },
		fail: func(task *contracts.AgentTask) bool {
			if !task.GetRequest().MessageIs(&contracts.HealthJudgeRequest{}) {
				return false
			}
			mu.Lock()
			defer mu.Unlock()
			judgeRuns++
			return judgeRuns <= 2
		},
	}
	play.install(ctx)
	if _, err := h.coordinator.Run(ctx, config()); err != nil {
		t.Fatal(err)
	}
	stop()

	taskIDs := map[string]string{} // task ID -> task kind
	entries, err := os.ReadDir(filepath.Join(mount, filepath.FromSlash(worker_registry.TaskEvidenceRoot)))
	if err != nil {
		t.Fatalf("no task evidence was persisted: %v", err)
	}
	judges := 0
	for _, entry := range entries {
		directory := filepath.Join(mount, filepath.FromSlash(worker_registry.TaskEvidenceRoot), entry.Name())
		request := &contracts.AgentTask{}
		raw, err := os.ReadFile(filepath.Join(directory, "request.json"))
		if err != nil {
			t.Fatalf("%s: %v", entry.Name(), err)
		}
		if err := wire.Unmarshal(raw, request); err != nil {
			t.Fatal(err)
		}
		id := request.GetTaskId()
		if entry.Name() != worker_registry.TaskEvidenceKey(id) {
			t.Errorf("directory %s is not the key of task %q", entry.Name(), id)
		}
		taskIDs[id] = request.GetTaskType()
		rollouts, _ := filepath.Glob(filepath.Join(directory, "codex", "sessions", "*", "*", "*", "rollout-*.jsonl"))
		if len(rollouts) != 1 {
			t.Errorf("task %s keeps %d rollouts, want 1", id, len(rollouts))
		}
		if _, err := os.Stat(filepath.Join(directory, "codex", "auth.json")); err == nil {
			t.Errorf("task %s left the provider login in its retained session", id)
		}
		if request.GetRequest().MessageIs(&contracts.HealthJudgeRequest{}) {
			judges++
			_, resultErr := os.Stat(filepath.Join(directory, "result.json"))
			_, errorErr := os.Stat(filepath.Join(directory, "error.json"))
			if (resultErr == nil) == (errorErr == nil) {
				t.Errorf("judge task %s must record exactly one of result.json and error.json (result: %v, error: %v)", id, resultErr, errorErr)
			}
		}
	}
	if judges != 3 {
		t.Fatalf("judge tasks with evidence = %d (%v), want the 2 failed attempts and the accepted one", judges, taskIDs)
	}
	if len(taskIDs) <= judges {
		t.Fatalf("the deployer task kept no evidence: %v", taskIDs)
	}
}
