package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	clocktesting "k8s.io/utils/clock/testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry"
	"sdo.dev/operator/worker_registry/registrytest"
)

// startWorld simulates the cluster, the worker processes and the clock: every
// poll advances one second, and each created Job is given to the next script.
type startWorld struct {
	t       *testing.T
	client  *fake.Clientset
	clock   *clocktesting.FakeClock
	mount   string
	scripts []func(*startWorld, *batchv1.Job)
	jobs    []string
	created []time.Time
	request IncidentRequest
	task    *contracts.AgentTask
	// onPoll runs at each look at a Job, before it is read.
	onPoll func(*startWorld, string)
}

func newStartWorld(t *testing.T, scripts ...func(*startWorld, *batchv1.Job)) *startWorld {
	t.Helper()
	w := &startWorld{t: t, client: fake.NewSimpleClientset(), clock: clocktesting.NewFakeClock(time.Unix(1000, 0).UTC()), mount: t.TempDir(), scripts: scripts}
	registry := registrytest.BuiltIn(t)
	agentID, err := registry.AgentFor(&contracts.IncidentRequest{})
	if err != nil {
		t.Fatal(err)
	}
	opened := newPreparationHarness(t, func(int) error { return nil })
	opened.controller.mu.Lock()
	w.request = *cloneIncidentRequest(opened.controller.currentIncidentRequest)
	opened.controller.mu.Unlock()
	w.request.IncidentID, w.request.RepositoryWorktree = "inc-1", w.mount+"/proposals/inc-1"
	w.request.RepositoryBaseCommit = strings.Repeat("a", 40)
	w.request.ResponseDeadline = time.Date(2999, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := os.MkdirAll(w.request.RepositoryWorktree, 0o755); err != nil {
		t.Fatal(err)
	}
	task, payload, err := encodeIncidentAgentTask(w.request, registry, agentID)
	if err != nil {
		t.Fatal(err)
	}
	w.task, w.request.PreparedAgentTask = task, string(payload)
	w.client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, kruntime.Object, error) {
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job)
		w.jobs, w.created = append(w.jobs, job.Name), append(w.created, w.clock.Now())
		if running := w.livePods(); len(running) != 0 {
			t.Errorf("Job %s created while %v still runs", job.Name, running)
		}
		if len(w.jobs) > len(w.scripts) {
			t.Errorf("unexpected worker %d: %s", len(w.jobs), job.Name)
			return false, nil, nil
		}
		w.scripts[len(w.jobs)-1](w, job)
		return false, nil, nil
	})
	w.client.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, kruntime.Object, error) {
		w.clock.Step(time.Second)
		if w.onPoll != nil {
			w.onPoll(w, action.(k8stesting.GetAction).GetName())
		}
		return false, nil, nil
	})
	return w
}

func (w *startWorld) dispatcher() KubernetesJobDispatcher {
	registry := registrytest.BuiltIn(w.t)
	return KubernetesJobDispatcher{
		Client: w.client, Namespace: "sdo", Registry: registry, AgentID: w.task.GetAgentId(), RepositoryPVC: "repo",
		RepositoryMountPath: w.mount, RequireMemorySnapshot: true, PollInterval: time.Second, Clock: w.clock,
	}
}

func (w *startWorld) livePods() []string {
	list, err := w.client.Tracker().List(corev1.SchemeGroupVersion.WithResource("pods"), corev1.SchemeGroupVersion.WithKind("Pod"), "sdo")
	if err != nil {
		w.t.Fatal(err)
	}
	var live []string
	for _, pod := range list.(*corev1.PodList).Items {
		if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
			live = append(live, pod.Name)
		}
	}
	return live
}

func (w *startWorld) pod(job *batchv1.Job, phase corev1.PodPhase, state corev1.ContainerState) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-pod", Namespace: "sdo", Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name, "sdo.dev/incident": job.Name}},
		Status:     corev1.PodStatus{Phase: phase, ContainerStatuses: []corev1.ContainerStatus{{Name: "agent", State: state}}},
	}
	if err := w.client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), pod, "sdo"); err != nil {
		w.t.Error(err)
	}
}

func (w *startWorld) session() string {
	sub, err := memory.IncidentSessionSubPath("", w.request.IncidentID)
	if err != nil {
		w.t.Fatal(err)
	}
	return filepath.Join(w.mount, sub)
}

func (w *startWorld) write(name, content string) {
	path := filepath.Join(w.session(), name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		w.t.Error(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		w.t.Error(err)
	}
}

func (w *startWorld) usage(turn, counters string) {
	w.write("usage/"+turn+".json", fmt.Sprintf(`{"version":1,"incident_id":"inc-1","task_id":"inc-1","phase":"respond","turn_id":%q,"complete":false,"usage":%s}`, turn, counters))
}

func (w *startWorld) result(job string) {
	w.t.Helper()
	packed, err := anypb.New(mustResult(w.t, completedResult("inc-1")))
	if err != nil {
		w.t.Fatal(err)
	}
	payload, err := wire.Canonical(&contracts.AgentResult{SchemaVersion: worker_registry.SchemaVersion, TaskId: "inc-1", Status: "completed", ProposalId: w.task.GetWorkspace().GetProposalId(), Response: packed})
	if err != nil {
		w.t.Fatal(err)
	}
	data := map[string]string{worker_registry.ResultDataKey: string(payload)}
	if err := w.client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("configmaps"), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: job + "-result", Namespace: "sdo"}, Data: data}, "sdo"); err != nil {
		w.t.Error(err)
	}
}

func mustResult(t *testing.T, result IncidentResult) *contracts.IncidentResult {
	t.Helper()
	typed, err := incidentResultToProto(result)
	if err != nil {
		t.Fatal(err)
	}
	return typed
}

func workerCrashes(w *startWorld, job *batchv1.Job) {
	w.pod(job, corev1.PodFailed, corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1, Message: "TypeError: synthetic crash"}})
}

func workerAnswers(w *startWorld, job *batchv1.Job) {
	w.usage("turn-ok", `{"input_tokens":10}`)
	w.pod(job, corev1.PodSucceeded, corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}})
	w.result(job.Name)
}

func workerHangs(w *startWorld, job *batchv1.Job) {
	w.pod(job, corev1.PodRunning, corev1.ContainerState{Running: &corev1.ContainerStateRunning{}})
	w.write("startup-workers/"+job.Name+"/startup-worker.json", `{"worker_ready":"2026-01-01T00:00:00Z"}`)
}

func (w *startWorld) failures() []string {
	files, _ := filepath.Glob(filepath.Join(w.session(), "startup/attempts/*/startup-failure.json"))
	var out []string
	for _, name := range files {
		data, err := os.ReadFile(name)
		if err != nil {
			w.t.Fatal(err)
		}
		out = append(out, string(data))
	}
	return out
}

func (w *startWorld) dispatch() (IncidentResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return w.dispatcher().Dispatch(ctx, w.request)
}

func TestAWorkerThatDiesBeforeItsFirstTurnIsReplacedAtOnce(t *testing.T) {
	w := newStartWorld(t, workerCrashes, workerAnswers)
	claims := 0
	result, err := w.dispatcher().DispatchWithClaim(context.Background(), w.request, func() { claims++ })
	if err != nil || result.Status != IncidentCompleted {
		t.Fatalf("Dispatch = %v, %v; want the replacement's result", result, err)
	}
	if len(w.jobs) != 2 || w.jobs[0] == w.jobs[1] || claims != 1 {
		t.Fatalf("workers %v, claims %d; want two workers of one dispatch", w.jobs, claims)
	}
	if took := w.created[1].Sub(w.created[0]); took > worker_registry.WorkerStartStall {
		t.Fatalf("replaced after %s", took)
	}
	if recorded := w.failures(); len(recorded) != 1 || !strings.Contains(recorded[0], "exit_code=1") || !strings.Contains(recorded[0], "synthetic crash") {
		t.Fatalf("recorded failures %v; want the exit code and the failure line", recorded)
	}
}

func TestAWorkerThatHangsAtStartIsFencedAndReplacedWithinTheBound(t *testing.T) {
	w := newStartWorld(t, workerHangs, workerAnswers)
	if result, err := w.dispatch(); err != nil || result.Status != IncidentCompleted {
		t.Fatalf("Dispatch = %v, %v", result, err)
	}
	// The fake world forbids creating a Job while another pod runs.
	if took := w.created[1].Sub(w.created[0]); took < worker_registry.WorkerStartStall || took > worker_registry.WorkerStartStall+5*time.Second {
		t.Fatalf("replaced after %s; want about %s", took, worker_registry.WorkerStartStall)
	}
}

func TestAnImageProblemReplacesTheWorkerAtOnce(t *testing.T) {
	w := newStartWorld(t, func(w *startWorld, job *batchv1.Job) {
		w.pod(job, corev1.PodPending, corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "not found"}})
	}, workerAnswers)
	if _, err := w.dispatch(); err != nil {
		t.Fatal(err)
	}
	if recorded := w.failures(); len(recorded) != 1 || !strings.Contains(recorded[0], "ImagePullBackOff") {
		t.Fatalf("recorded failures %v", recorded)
	}
}

func TestTwoDeadWorkersEndTheDispatch(t *testing.T) {
	w := newStartWorld(t, workerCrashes, workerCrashes)
	_, err := w.dispatch()
	var never *WorkerNeverStartedError
	if !errors.As(err, &never) || never.Attempts != 2 || !strings.Contains(never.Reason, "synthetic crash") {
		t.Fatalf("Dispatch = %v; want WorkerNeverStartedError after two workers", err)
	}
	// A restart before the escalation is saved must not start a third worker.
	if _, err := w.dispatch(); !errors.As(err, &never) || len(w.jobs) != 2 {
		t.Fatalf("rejoin = %v, workers %v", err, w.jobs)
	}
}

func TestAWorkerThatDiedAfterATurnIsNeverReplaced(t *testing.T) {
	w := newStartWorld(t, func(w *startWorld, job *batchv1.Job) {
		w.usage("turn-1", `{"input_tokens":10}`)
		job.Status.Failed = 1
		workerCrashes(w, job)
	})
	_, err := w.dispatch()
	var failed *ResponderJobFailedError
	if !errors.As(err, &failed) || workerNeverStarted(err) || len(w.jobs) != 1 || len(w.failures()) != 0 {
		t.Fatalf("Dispatch = %v, workers %v", err, w.jobs)
	}
}

func TestATurnHandedToTheProviderIsNeverAStall(t *testing.T) {
	w := newStartWorld(t, func(w *startWorld, job *batchv1.Job) { workerHangs(w, job); w.usage("turn-1", `{}`) })
	polls := 0
	w.onPoll = func(w *startWorld, name string) {
		// The model thinks for far longer than the stall bound, then replies.
		if polls++; polls == 100 {
			w.usage("turn-1", `{"input_tokens":10}`)
			if err := w.client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), "sdo", name+"-pod"); err != nil {
				t.Error(err)
			}
			w.pod(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name}}, corev1.PodSucceeded, corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}})
			w.result(name)
		}
	}
	if _, err := w.dispatch(); err != nil || len(w.jobs) != 1 {
		t.Fatalf("Dispatch = %v, workers %v", err, w.jobs)
	}
}

// The first worker takes its turn between the stall verdict and the fence: no
// second agent starts.
func TestALateFirstTurnStartsNoSecondAgent(t *testing.T) {
	w := newStartWorld(t, workerHangs)
	w.client.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, kruntime.Object, error) {
		w.usage("late", `{"input_tokens":1}`)
		return false, nil, nil
	})
	_, err := w.dispatch()
	var failed *ResponderJobFailedError
	if !errors.As(err, &failed) || len(w.jobs) != 1 || len(w.failures()) != 0 || len(w.livePods()) != 0 {
		t.Fatalf("Dispatch = %v, workers %v, live %v", err, w.jobs, w.livePods())
	}
}

// A late result of the first worker is the incident's result.
func TestALateResultIsHarvestedInsteadOfReplaced(t *testing.T) {
	w := newStartWorld(t, workerHangs)
	w.client.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, kruntime.Object, error) {
		w.usage("late", `{"input_tokens":1}`)
		w.result(action.(k8stesting.DeleteAction).GetName())
		return false, nil, nil
	})
	if result, err := w.dispatch(); err != nil || result.Status != IncidentCompleted || len(w.jobs) != 1 {
		t.Fatalf("Dispatch = %v, %v, workers %v", result, err, w.jobs)
	}
}

// The controller restarts at each step of a replacement: the incident still
// gets exactly one fresh worker.
func TestARestartDuringAReplacementStartsOneFreshWorker(t *testing.T) {
	for _, step := range []string{"after the fence", "after the record", "after the new worker"} {
		t.Run(step, func(t *testing.T) {
			second := workerAnswers
			if step == "after the new worker" {
				second = workerHangs
			}
			w := newStartWorld(t, workerCrashes, second)
			ctx, restart := context.WithCancel(context.Background())
			switch step {
			case "after the fence":
				w.client.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, kruntime.Object, error) { restart(); return false, nil, nil })
			case "after the record":
				w.client.PrependReactor("delete", "configmaps", func(k8stesting.Action) (bool, kruntime.Object, error) {
					if ctx.Err() == nil {
						restart()
						return true, nil, errors.New("synthetic restart")
					}
					return false, nil, nil
				})
			default:
				w.onPoll = func(w *startWorld, _ string) {
					if len(w.jobs) == 2 {
						restart()
					}
				}
			}
			if _, err := w.dispatcher().Dispatch(ctx, w.request); err == nil || workerNeverStarted(err) {
				t.Fatalf("interrupted Dispatch = %v", err)
			}
			w.onPoll = nil
			if step == "after the new worker" {
				name := w.jobs[1]
				_ = w.client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), "sdo", name+"-pod")
				workerAnswers(w, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name}})
			}
			if result, err := w.dispatch(); err != nil || result.Status != IncidentCompleted {
				t.Fatalf("rejoined Dispatch = %v, %v", result, err)
			}
			if len(w.jobs) != 2 {
				t.Fatalf("workers %v; want two", w.jobs)
			}
		})
	}
}

// A provider that opened a session and refused the turn gets no second worker,
// also after a restart.
func TestARefusedProviderSessionIsNotRetried(t *testing.T) {
	failures := []worker_registry.StartupFailure{{JobName: "one", Reason: "refused"}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "inc-1"}, nil, staticProvider{}, nil, PlanExecutor{}, 2)
	if startsExhausted(nil, loop) || startsExhausted(failures, loop) {
		t.Fatal("one failed start without a provider session allows a fresh worker")
	}
	loop.menuSession = &contracts.MitigationSession{ProviderSessionId: "session"}
	if !startsExhausted(failures, loop) {
		t.Fatal("a refused provider session must not be retried")
	}
}

// What the automatic stage applied before the agent's workers died stays in the
// result, whatever response.after_playbook says.
func TestNeverStartedWorkersKeepTheStagesApplications(t *testing.T) {
	for _, after := range []string{"agent", "escalate", "wait"} {
		h := newPreparationHarness(t, func(int) error { return nil })
		h.step()
		h.controller.mu.Lock()
		h.controller.config.AfterPlaybook = after
		id := h.controller.currentIncidentRequest.IncidentID
		h.controller.currentIncidentRequest.PriorStages = []*contracts.StageReport{{Stage: "deterministic"}}
		h.controller.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
		h.controller.mu.Unlock()
		applied := IncidentResult{IncidentID: id, PlanApplications: make([]RecordedPlanApplication, 1)}
		h.controller.handleDispatchCompletion(dispatchCompletion{incidentID: id, result: applied, err: &WorkerNeverStartedError{Attempts: 2, Reason: "synthetic"}}, h.now)
		h.controller.mu.Lock()
		result, escalated := h.controller.currentIncidentResult, h.controller.escalatedAt
		h.controller.mu.Unlock()
		if result == nil || len(result.PlanApplications) != 1 || escalated.IsZero() {
			t.Fatalf("%s: result %v, escalated %v", after, result, escalated)
		}
	}
}

func TestOnlyModelTurnCallsCountAsATurn(t *testing.T) {
	activity := &workerActivity{}
	done := activity.received("session-open")
	watch := activity.startWatch()
	if !watch.Waiting() || watch.Turns() != 0 {
		t.Fatal("an unanswered session-open waits for the controller and is no turn")
	}
	done()
	activity.received("session-checkpoint")()
	activity.received("detect")()
	if watch.Waiting() || watch.Turns() != 1 {
		t.Fatalf("turns = %d", watch.Turns())
	}
}

// Prepared, then both workers died: the incident escalates and stays open, and
// the reason is in the result, the lifecycle event and the saved state.
func TestWorkersThatNeverStartedEscalateTheIncident(t *testing.T) {
	h := newPreparationHarness(t, func(int) error { return nil })
	h.step()
	h.controller.mu.Lock()
	id := h.controller.currentIncidentRequest.IncidentID
	h.controller.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	h.controller.mu.Unlock()
	cause := fmt.Errorf("response stage agent: %w", &WorkerNeverStartedError{Attempts: 2, Reason: "exit_code=1 synthetic"})
	h.controller.handleDispatchCompletion(dispatchCompletion{incidentID: id, err: cause}, h.now)
	h.controller.mu.Lock()
	open, escalated, done, dispatchErr := h.controller.incidentOpen, h.controller.escalatedAt, h.controller.responderDone, h.controller.dispatchError
	result := h.controller.currentIncidentResult
	h.controller.mu.Unlock()
	if !open || escalated.IsZero() || !done || dispatchErr != "" {
		t.Fatalf("open %v, escalated %v, done %v, dispatch error %q", open, escalated, done, dispatchErr)
	}
	if result == nil || result.Status != IncidentFailed || !strings.Contains(result.Error, "exit_code=1 synthetic") {
		t.Fatalf("result = %+v", result)
	}
	if !h.lifecycle.has(PhaseDispatchFailed) || h.lifecycle.has(PhaseResponderCompleted) {
		t.Fatal("want responder_dispatch_failed and no responder_completed")
	}
	if err := h.controller.PersistState(context.Background()); err != nil {
		t.Fatal(err)
	}
}
