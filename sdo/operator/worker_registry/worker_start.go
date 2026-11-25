package worker_registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry/internal/workerio"
)

// A responder worker that ended without a result, a usage file, a tool call or
// a provider session did nothing. Once its pods have ended it is recorded and
// one fresh worker runs the same frozen task. Any other worker is never replaced.

// MaxWorkerStarts is how many workers one incident task may get in all.
const MaxWorkerStarts = 2

const startFailureFile = "startup-failure.json"

// StartupFailure records one worker that ended before its first model turn.
type StartupFailure struct {
	JobName       string    `json:"job_name"`
	RequestSHA256 string    `json:"request_sha256"`
	Reason        string    `json:"reason"`
	RecordedAt    time.Time `json:"recorded_at"`
}

// workerEnded reports that the agent container exited and every pod of the Job
// is over: the Job controller reports the same, seconds later or, for a clean
// exit without a result, never as a failure.
func (e KubernetesExecutor) workerEnded(ctx context.Context, jobName string) bool {
	pods, err := e.Client.CoreV1().Pods(e.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + jobName})
	if err != nil {
		return false
	}
	exited := false
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return false
		}
		for _, status := range pod.Status.ContainerStatuses {
			exited = exited || status.Name == "agent" && status.State.Terminated != nil
		}
	}
	return exited
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
	return filepath.Join(e.RepositoryMountPath, relative), err
}

// StartFailures lists the workers of this exact task that did nothing.
func (e KubernetesExecutor) StartFailures(task *contracts.AgentTask) ([]StartupFailure, error) {
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
	return failures, nil
}

func replacementJobName(taskID string) string { return JobName(taskID) + "-s1" }

// RetireNeverStarted records a worker that did nothing, so a fresh one may run.
// It returns false when a result or a usage file shows that the worker started,
// and an error while any of its pods may still run. The pods are read first: a
// worker writes nothing after they ended.
func (e KubernetesExecutor) RetireNeverStarted(ctx context.Context, task *contracts.AgentTask, jobName, reason string) (bool, error) {
	if err := e.verifyWorkerQuiescence(ctx, jobName); err != nil {
		return false, err
	}
	_, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Get(ctx, jobName+"-result", metav1.GetOptions{})
	if err == nil {
		return false, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("inspect worker result: %w", err)
	}
	session, err := e.sessionDirectory(task)
	if err != nil {
		return false, err
	}
	// The worker writes a usage file right before it hands its first turn to the provider.
	usage, err := os.ReadDir(filepath.Join(session, "usage"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	for _, entry := range usage {
		if strings.HasSuffix(entry.Name(), ".json") {
			return false, nil
		}
	}
	directory := filepath.Join(session, "startup/attempts", jobName)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return false, err
	}
	if text := []rune(strings.ToValidUTF8(reason, "�")); len(text) > 1024 {
		reason = string(text[:1024]) + "…"
	}
	failure := StartupFailure{JobName: jobName, RequestSHA256: taskRequestDigest(task), Reason: reason, RecordedAt: workerio.Now().UTC()}
	if err := writeStartupOnce(filepath.Join(directory, startFailureFile), failure); err != nil {
		return false, fmt.Errorf("record worker start failure: %w", err)
	}
	return true, e.DropRetired(ctx, task)
}

// DropRetired removes the Job and execution pointers of recorded workers. It
// also finishes a retirement a controller restart cut short.
func (e KubernetesExecutor) DropRetired(ctx context.Context, task *contracts.AgentTask) error {
	failures, err := e.StartFailures(task)
	if err != nil {
		return err
	}
	policy := metav1.DeletePropagationBackground
	for _, failure := range failures {
		if err := e.Client.BatchV1().Jobs(e.Namespace).Delete(ctx, failure.JobName, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("drop retired worker Job: %w", err)
		}
		binding, err := e.loadWarmBinding(task)
		if err != nil {
			return err
		}
		if binding != nil && binding.JobName == failure.JobName {
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
		if identity != nil && identity.JobName == failure.JobName {
			err := e.Client.CoreV1().ConfigMaps(e.Namespace).Delete(ctx, incidentExecutionName(identity.Digest), metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("drop retired execution identity: %w", err)
			}
		}
	}
	return nil
}
