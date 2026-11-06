package deployment

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// progressEvery bounds how often an observer reports what it waits on, so a
// long wait is never silent and never floods the log.
const progressEvery = 30 * time.Second

// defaultJobBackoffLimit is the Kubernetes default when spec.backoffLimit is unset.
const defaultJobBackoffLimit = 6

// jobFailure ends the wait for a Job that cannot succeed. A Job that reports
// Failed (backoffLimit or activeDeadlineSeconds reached) ends it at once. A Job
// whose attempts keep failing is not yet failed (the Job controller retries):
// it ends at the rollout deadline (atDeadline) with the attempts and the last
// exit. A Job that never failed an attempt, or any other workload, returns nil
// and keeps its ordinary rollout rules.
func (b CommandBackend) jobFailure(resourceID string, workload []byte, limit, waited time.Duration, atDeadline bool) *RolloutTimeoutError {
	if !strings.HasPrefix(resourceID, "Job/") {
		return nil
	}
	var job batchv1.Job
	if json.Unmarshal(workload, &job) != nil {
		return nil
	}
	var failedCondition *batchv1.JobCondition
	for i, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			failedCondition = &job.Status.Conditions[i]
		}
	}
	if failedCondition == nil && !atDeadline {
		return nil
	}
	attempts := int(job.Status.Failed)
	last := ""
	var lastAt time.Time
	if pods, _, ok := b.readRollout(resourceID, workload); ok {
		var list struct {
			Items []podObject `json:"items"`
		}
		if json.Unmarshal(pods, &list) == nil {
			for _, pod := range list.Items {
				for _, s := range pod.Status.ContainerStatuses {
					if pod.Status.Phase != "Failed" {
						attempts += s.RestartCount
					}
					t := s.State.Terminated
					if t == nil || t.ExitCode == 0 {
						t = s.LastState.Terminated
					}
					if t != nil && t.ExitCode != 0 && !t.FinishedAt.Before(lastAt) {
						lastAt = t.FinishedAt
						last = fmt.Sprintf("last exit code %d (%s)", t.ExitCode, t.Reason)
						if t.Message != "" {
							last += ": " + clip(strings.TrimSpace(t.Message), 120)
						}
					}
				}
			}
		}
	}
	if failedCondition == nil && attempts == 0 {
		return nil
	}
	backoff := defaultJobBackoffLimit
	if job.Spec.BackoffLimit != nil {
		backoff = int(*job.Spec.BackoffLimit)
	}
	if last == "" {
		last = "no exit recorded"
	}
	detail := fmt.Sprintf("%s: %d failed attempts (backoffLimit %d), %s", resourceID, attempts, backoff, last)
	reason := RolloutReasonJobRetryDeadline
	if failedCondition != nil {
		reason = RolloutReasonJobFailed
		detail += fmt.Sprintf("; the Job reports Failed (%s: %s)", failedCondition.Reason, clip(failedCondition.Message, 160))
	} else {
		detail += fmt.Sprintf("; its attempts were still failing at the %s rollout deadline", limit.Round(time.Second))
	}
	return &RolloutTimeoutError{
		Resource: resourceID, Limit: limit, Waited: waited, Detail: detail, Stalled: true, Reason: reason,
		Advice: "the Job's command fails on every attempt: read its exit code and output and fix the command, environment or image; " +
			"a Job re-run against state a previous run left behind must be idempotent",
	}
}

// waitingLine says what the observer waits on and how far the workload has got.
func waitingLine(resourceID string, workload []byte, waited, limit time.Duration) string {
	head := fmt.Sprintf("deploy: waiting on %s for %s of %s", resourceID, waited.Round(time.Second), limit.Round(time.Second))
	var object struct {
		Spec struct {
			Replicas     *int32 `json:"replicas"`
			BackoffLimit *int32 `json:"backoffLimit"`
		} `json:"spec"`
		Status struct {
			Active, Failed, Succeeded, ReadyReplicas int32
		} `json:"status"`
	}
	if json.Unmarshal(workload, &object) != nil {
		return head
	}
	if strings.HasPrefix(resourceID, "Job/") {
		backoff := int32(defaultJobBackoffLimit)
		if object.Spec.BackoffLimit != nil {
			backoff = *object.Spec.BackoffLimit
		}
		return fmt.Sprintf("%s: active %d, %d failed attempts (backoffLimit %d), succeeded %d", head, object.Status.Active, object.Status.Failed, backoff, object.Status.Succeeded)
	}
	desired := int32(1)
	if object.Spec.Replicas != nil {
		desired = *object.Spec.Replicas
	}
	return fmt.Sprintf("%s: %d of %d replicas ready", head, object.Status.ReadyReplicas, desired)
}
