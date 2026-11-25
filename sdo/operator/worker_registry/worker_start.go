package worker_registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry/internal/workerio"
)

// A responder worker that ends before its first model turn did nothing: no
// model output, no tool call, no result. Such a worker is fenced (its Job and
// pods are gone), recorded, and replaced by a fresh one for the same frozen
// task. A worker that took a turn is never replaced.

const (
	// WorkerStartStall is how long a claimed or running responder worker may
	// make no progress before its first turn is handed to the provider.
	WorkerStartStall = 20 * time.Second
	// workerPlacementBound is how long a cold worker may take to report its
	// first progress: scheduling, an image pull and the init containers are not
	// a hang of the worker. Failures Kubernetes reports end it at once.
	workerPlacementBound = 2 * time.Minute
	// MaxWorkerStarts is how many workers one incident task may get in all.
	MaxWorkerStarts = 2
	// workerFenceTimeout bounds the wait for a replaced worker's pods to end.
	workerFenceTimeout = 15 * time.Second
	startFailureFile   = "startup-failure.json"
	replacementSuffix  = "-s"
)

// StartupFailure records one worker that ended before its first model turn.
type StartupFailure struct {
	JobName       string    `json:"job_name"`
	RequestSHA256 string    `json:"request_sha256"`
	Reason        string    `json:"reason"`
	RecordedAt    time.Time `json:"recorded_at"`
}

// WorkerStalledError reports a responder worker that stopped making progress,
// or cannot run, before its first turn.
type WorkerStalledError struct{ JobName, Reason string }

func (e *WorkerStalledError) Error() string {
	return fmt.Sprintf("agent Job %q did not start: %s", e.JobName, e.Reason)
}

// WorkerStartWatch turns on supervision of a responder worker's start.
type WorkerStartWatch struct {
	// Turns counts the tool calls only a model turn makes.
	Turns func() int
	// Activity counts the worker's calls to the controller's tool server.
	Activity func() int
	// Waiting reports a call the controller has not answered yet, so the worker
	// waits for the controller and is not stalled.
	Waiting func() bool
}

// startWatcher follows one worker through its start, on the executor's clock.
type startWatcher struct {
	executor   KubernetesExecutor
	task       *contracts.AgentTask
	jobName    string
	clock      clock.PassiveClock
	progressAt time.Time
	seen       string
}

func (e KubernetesExecutor) newStartWatcher(task *contracts.AgentTask, jobName string, c clock.PassiveClock) *startWatcher {
	if e.StartWatch == nil || !task.GetRequest().MessageIs(&contracts.IncidentRequest{}) {
		return nil
	}
	return &startWatcher{executor: e, task: task, jobName: jobName, clock: c, progressAt: c.Now()}
}

// check returns why the worker cannot start, or empty. Callers have just read
// that no result exists.
func (w *startWatcher) check(pods []corev1.Pod) string {
	handed, answered, err := w.executor.turnEvidence(w.task)
	if answered || err != nil || w.executor.StartWatch.Turns() > 0 {
		return ""
	}
	for _, pod := range pods {
		if reason := podCannotStart(pod); reason != "" {
			return reason
		}
	}
	if handed {
		return "" // The provider owns the turn; the response timeout bounds it.
	}
	ready := w.executor.WarmBinding != nil || w.executor.workerReady(w.task, w.jobName)
	activity := w.executor.StartWatch.Activity()
	now := w.clock.Now()
	if state := fmt.Sprint(ready, activity); state != w.seen || w.executor.StartWatch.Waiting() {
		w.seen, w.progressAt = state, now
		return ""
	}
	bound := workerPlacementBound
	if ready || activity > 0 {
		bound = WorkerStartStall
	}
	if now.Sub(w.progressAt) < bound {
		return ""
	}
	return fmt.Sprintf("no progress for %s before its first turn", bound)
}

// podCannotStart names a container failure Kubernetes already reported. An
// agent container that ended without a result never runs again (the Job does
// not retry), so its exit is final seconds before the Job condition.
func podCannotStart(pod corev1.Pod) string {
	sidecars := map[string]bool{}
	for _, init := range pod.Spec.InitContainers {
		sidecars[init.Name] = init.RestartPolicy != nil && *init.RestartPolicy == corev1.ContainerRestartPolicyAlways
	}
	for _, status := range append(append([]corev1.ContainerStatus(nil), pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...) {
		if ended := status.State.Terminated; ended != nil && (status.Name == "agent" || ended.ExitCode != 0 && !sidecars[status.Name]) {
			return fmt.Sprintf("container %q ended without a result: reason=%q exit_code=%d message=%q", status.Name, failureText(ended.Reason), ended.ExitCode, failureText(ended.Message))
		}
		if waiting := status.State.Waiting; waiting != nil {
			switch waiting.Reason {
			case "ImagePullBackOff", "ErrImageNeverPull", "InvalidImageName", "CreateContainerConfigError", "CreateContainerError", "RunContainerError", "CrashLoopBackOff":
				return fmt.Sprintf("container %q cannot run: %s: %s", status.Name, waiting.Reason, failureText(waiting.Message))
			}
		}
	}
	return ""
}

// sessionDirectory is the incident session as the controller sees it.
func (e KubernetesExecutor) sessionDirectory(task *contracts.AgentTask) (string, error) {
	request := new(contracts.IncidentRequest)
	if err := task.GetRequest().UnmarshalTo(request); err != nil {
		return "", err
	}
	sub, err := memory.IncidentSessionSubPath(e.RepositoryPVCSubPath, request.GetIncidentId())
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(sessionPVCBase(e.RepositoryPVCSubPath), sub)
	if err != nil {
		return "", err
	}
	return filepath.Join(e.RepositoryMountPath, relative), nil
}

func (e KubernetesExecutor) workerReady(task *contracts.AgentTask, jobName string) bool {
	session, err := e.sessionDirectory(task)
	if err != nil {
		return false
	}
	data, err := os.ReadFile(filepath.Join(session, "startup-workers", jobName, "startup-worker.json"))
	var worker WorkerStartup
	return err == nil && json.Unmarshal(data, &worker) == nil && worker.WorkerReady != nil
}

// turnEvidence reads the worker's own usage checkpoints: it writes an empty one
// right before it hands its first turn to the provider (handed), and counters
// once the model answered (answered).
func (e KubernetesExecutor) turnEvidence(task *contracts.AgentTask) (handed, answered bool, err error) {
	session, err := e.sessionDirectory(task)
	if err != nil {
		return false, false, err
	}
	files, err := usageCheckpoints(session, task.GetTaskId())
	for _, usage := range files {
		handed = true
		answered = answered || usage
	}
	return handed, answered, err
}

// usageCheckpoints maps each of the task's checkpoint files to whether the
// model answered. A file that cannot be decoded counts as answered: uncertainty
// is never proof that nothing ran.
func usageCheckpoints(session, taskID string) (map[string]bool, error) {
	directory := filepath.Join(session, "usage")
	entries, err := os.ReadDir(directory)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	files := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		name := filepath.Join(directory, entry.Name())
		data, err := os.ReadFile(name)
		if err != nil {
			return nil, err
		}
		var checkpoint struct {
			TaskID string         `json:"task_id"`
			Usage  map[string]any `json:"usage"`
		}
		if json.Unmarshal(data, &checkpoint) != nil {
			files[name] = true
			continue
		}
		if checkpoint.TaskID != taskID {
			continue
		}
		files[name] = false
		for _, value := range checkpoint.Usage {
			if number, ok := value.(float64); !ok || number != 0 {
				files[name] = true
			}
		}
	}
	return files, nil
}

// startFailures lists the workers of this exact task that never took a turn,
// oldest first.
func (e KubernetesExecutor) startFailures(task *contracts.AgentTask) ([]StartupFailure, error) {
	if !task.GetRequest().MessageIs(&contracts.IncidentRequest{}) || e.RepositoryMountPath == "" {
		return nil, nil
	}
	session, err := e.sessionDirectory(task)
	if err != nil {
		return nil, err
	}
	files, err := filepath.Glob(filepath.Join(session, "startup/attempts/*", startFailureFile))
	if err != nil {
		return nil, err
	}
	var failures []StartupFailure
	for _, filename := range files {
		data, err := os.ReadFile(filename)
		if err != nil {
			return nil, err
		}
		var failure StartupFailure
		if err := json.Unmarshal(data, &failure); err != nil {
			return nil, fmt.Errorf("decode worker start failure: %w", err)
		}
		if failure.RequestSHA256 == taskRequestDigest(task) {
			failures = append(failures, failure)
		}
	}
	sort.Slice(failures, func(i, j int) bool { return failures[i].RecordedAt.Before(failures[j].RecordedAt) })
	return failures, nil
}

// StartFailures reports the recorded start failures of the task.
func (e KubernetesExecutor) StartFailures(task *contracts.AgentTask) ([]StartupFailure, error) {
	return e.startFailures(task)
}

func replacementJobName(taskID string, failures int) string {
	return fmt.Sprintf("%s%s%d", JobName(taskID), replacementSuffix, failures)
}

func isReplacementJobName(name, taskID string) bool {
	for attempt := 1; attempt < MaxWorkerStarts; attempt++ {
		if name == replacementJobName(taskID, attempt) {
			return true
		}
	}
	return false
}

// NeverStarted reports that the task's current worker took no model turn and
// published no result. An unreadable state is an error, never an answer.
func (e KubernetesExecutor) NeverStarted(ctx context.Context, task *contracts.AgentTask, jobName string) (bool, error) {
	if e.StartWatch != nil && e.StartWatch.Turns() > 0 {
		return false, nil
	}
	if _, answered, err := e.turnEvidence(task); err != nil || answered {
		return false, err
	}
	_, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Get(ctx, jobName+"-result", metav1.GetOptions{})
	if err == nil {
		return false, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("inspect worker result: %w", err)
	}
	return true, nil
}

// RetireNeverStarted fences a worker that never took a turn and records why.
// The Job and its pods are gone before the record exists, and the record exists
// before another worker may be named, so two workers never overlap. It returns
// false, and records nothing, when the worker turns out to have started.
func (e KubernetesExecutor) RetireNeverStarted(ctx context.Context, task *contracts.AgentTask, jobName, reason string) (bool, error) {
	if err := e.fenceWorker(ctx, jobName); err != nil {
		return false, err
	}
	// The worker may have taken its turn between the first look and the fence.
	if never, err := e.NeverStarted(ctx, task, jobName); err != nil || !never {
		return false, err
	}
	session, err := e.sessionDirectory(task)
	if err != nil {
		return false, err
	}
	// Its checkpoints hold no counters; the next worker's must not be mistaken for them.
	files, err := usageCheckpoints(session, task.GetTaskId())
	if err != nil {
		return false, err
	}
	for name, answered := range files {
		if !answered {
			if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
				return false, err
			}
		}
	}
	directory := filepath.Join(session, "startup/attempts", jobName)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return false, err
	}
	failure := StartupFailure{JobName: jobName, RequestSHA256: taskRequestDigest(task), Reason: boundedReason(reason), RecordedAt: workerio.Now().UTC()}
	if err := writeStartupOnce(filepath.Join(directory, startFailureFile), failure); err != nil {
		return false, fmt.Errorf("record worker start failure: %w", err)
	}
	return true, e.DropRetired(ctx, task)
}

// DropRetired removes the execution pointers of workers already recorded as
// never started. It also finishes a retirement a controller restart cut short.
func (e KubernetesExecutor) DropRetired(ctx context.Context, task *contracts.AgentTask) error {
	failures, err := e.startFailures(task)
	if err != nil || len(failures) == 0 {
		return err
	}
	retired := map[string]bool{}
	for _, failure := range failures {
		retired[failure.JobName] = true
	}
	binding, err := e.loadWarmBinding(task)
	if err != nil {
		return err
	}
	if binding != nil && retired[binding.JobName] {
		if err := os.Remove(e.warmBindingPath(task)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.RemoveAll(binding.Directory); err != nil {
			return err
		}
	}
	identity, err := e.loadIncidentExecution(ctx, task)
	if err != nil {
		return err
	}
	if identity != nil && retired[identity.JobName] {
		err := e.Client.CoreV1().ConfigMaps(e.Namespace).Delete(ctx, incidentExecutionName(identity.Digest), metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("drop retired execution identity: %w", err)
		}
	}
	return nil
}

// fenceWorker deletes the Job and its pods and returns once none runs.
func (e KubernetesExecutor) fenceWorker(ctx context.Context, jobName string) error {
	policy, zero := metav1.DeletePropagationForeground, int64(0)
	options := metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &policy}
	if err := e.Client.BatchV1().Jobs(e.Namespace).Delete(ctx, jobName, options); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("fence worker Job %s: %w", jobName, err)
	}
	pollClock := e.Clock
	if pollClock == nil {
		pollClock = clock.RealClock{}
	}
	deadline := pollClock.Now().Add(workerFenceTimeout)
	for {
		pods, err := e.Client.CoreV1().Pods(e.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + jobName})
		if err != nil {
			return fmt.Errorf("fence worker pods: %w", err)
		}
		for _, pod := range pods.Items {
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				continue
			}
			if err := e.Client.CoreV1().Pods(e.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("fence worker Pod %s: %w", pod.Name, err)
			}
		}
		quiet := e.verifyWorkerQuiescence(ctx, jobName)
		if quiet == nil {
			return nil
		}
		if !pollClock.Now().Before(deadline) {
			return quiet
		}
		wait := pollClock.NewTimer(workerExitPollInterval)
		select {
		case <-ctx.Done():
			wait.Stop()
			return ctx.Err()
		case <-wait.C():
		}
	}
}

func boundedReason(reason string) string {
	const maxRunes = 1024
	text := []rune(strings.ToValidUTF8(reason, "�"))
	if len(text) > maxRunes {
		return string(text[:maxRunes]) + "…"
	}
	return string(text)
}
