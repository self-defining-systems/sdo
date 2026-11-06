package deployment

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// jobObject is a synthetic stamped Job with the given status JSON.
func jobObject(status string) string {
	return `{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"seed","namespace":"demo","uid":"u","generation":1,
"annotations":{"sdo.dev/source-tree":"t"}},"spec":{"backoffLimit":6,"selector":{"matchLabels":{"controller-uid":"u"}},
"template":{"metadata":{"annotations":{"sdo.dev/source-tree":"t"}},"spec":{"restartPolicy":"Never","containers":[{"name":"seed","image":"x/seed@sha256:` + digestA + `"}]}}},"status":` + status + `}`
}

const failedJobPods = `{"items":[
{"metadata":{"name":"seed-a","creationTimestamp":"2026-11-05T00:00:00Z"},"status":{"phase":"Failed","containerStatuses":[{"name":"seed","state":{"terminated":{"exitCode":1,"reason":"Error","finishedAt":"2026-11-05T00:05:00Z"}}}]}},
{"metadata":{"name":"seed-b","creationTimestamp":"2026-11-05T00:05:00Z"},"status":{"phase":"Failed","containerStatuses":[{"name":"seed","state":{"terminated":{"exitCode":2,"reason":"Error","message":"bucket exists","finishedAt":"2026-11-05T00:10:00Z"}}}]}}]}`

// jobClock is an injected clock: every Wait advances it, so no test sleeps.
type jobClock struct {
	now   time.Time
	waits int
}

func (c *jobClock) Now() time.Time { return c.now }
func (c *jobClock) Wait(_ context.Context, d time.Duration) error {
	c.waits++
	c.now = c.now.Add(d)
	return nil
}

func failingJobBackend(clock *jobClock, job, pods string, limit time.Duration) CommandBackend {
	return CommandBackend{
		ObserveLimit: limit, Now: clock.Now, Wait: clock.Wait,
		Run: func(_ context.Context, _ string, args []string, _ []byte) ([]byte, error) {
			if len(args) > 1 && args[1] == "pods" {
				return []byte(pods), nil
			}
			return []byte(job), nil
		},
	}
}

func TestObserveEndsAtOnceWhenAJobReportsFailed(t *testing.T) {
	status := `{"failed":7,"conditions":[{"type":"Failed","status":"True","reason":"BackoffLimitExceeded","message":"Job has reached the specified backoff limit"}]}`
	clock := &jobClock{now: mustTime(testNow)}
	_, err := failingJobBackend(clock, jobObject(status), failedJobPods, 10*time.Minute).Observe(context.Background(), "Job/demo/seed")
	var failure *RolloutTimeoutError
	if !errors.As(err, &failure) || !failure.Stalled || failure.InfrastructureOutcome() {
		t.Fatalf("want a stalled typed rollout failure, got %v", err)
	}
	if failure.Reason != RolloutReasonJobFailed {
		t.Fatalf("reason = %q", failure.Reason)
	}
	for _, want := range []string{"Job/demo/seed", "7 failed attempts", "backoffLimit 6", "exit code 2", "bucket exists", "BackoffLimitExceeded"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q: %v", want, err)
		}
	}
	if clock.waits != 0 {
		t.Errorf("waited %d times for a Job that already failed", clock.waits)
	}
}

func TestObserveEndsAFailingJobAtTheDeadlineButNotBefore(t *testing.T) {
	// Attempts keep failing (Job still under its backoffLimit): retries are not yet
	// progressed until the deadline, then a typed failure.
	status := `{"active":1,"failed":2}`
	clock := &jobClock{now: mustTime(testNow)}
	start := clock.now
	_, err := failingJobBackend(clock, jobObject(status), failedJobPods, 10*time.Minute).Observe(context.Background(), "Job/demo/seed")
	var failure *RolloutTimeoutError
	if !errors.As(err, &failure) || !failure.Stalled || failure.Reason != RolloutReasonJobRetryDeadline {
		t.Fatalf("want job-retry-deadline-exceeded, got %v", err)
	}
	if elapsed := clock.now.Sub(start); elapsed < 10*time.Minute || elapsed > 10*time.Minute+time.Minute {
		t.Errorf("ended after %s, want the 10m deadline", elapsed)
	}
	for _, want := range []string{"Job/demo/seed", "2 failed attempts", "exit code 2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q: %v", want, err)
		}
	}
}

func TestObserveKeepsWaitingForAJobWithNoFailedAttempt(t *testing.T) {
	clock := &jobClock{now: mustTime(testNow)}
	pods := `{"items":[{"metadata":{"name":"seed-a","creationTimestamp":"2026-11-05T00:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"name":"seed","ready":true,"state":{"running":{"startedAt":"2026-11-05T00:00:00Z"}}}]}}]}`
	_, err := failingJobBackend(clock, jobObject(`{"active":1}`), pods, 10*time.Minute).Observe(context.Background(), "Job/demo/seed")
	var timeout *RolloutTimeoutError
	if errors.As(err, &timeout) && timeout.Reason == RolloutReasonJobRetryDeadline {
		t.Fatalf("a Job that never failed an attempt is not a failure: %v", err)
	}
}

func TestObserveReportsWhatItWaitsOnAtABoundedInterval(t *testing.T) {
	clock := &jobClock{now: mustTime(testNow)}
	var lines []string
	backend := failingJobBackend(clock, jobObject(`{"active":1,"failed":2}`), failedJobPods, 10*time.Minute)
	backend.Progress = func(line string) { lines = append(lines, line) }
	_, _ = backend.Observe(context.Background(), "Job/demo/seed")
	if len(lines) < 5 || len(lines) > 25 {
		t.Fatalf("%d progress lines over a 10 minute wait:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	for _, want := range []string{"Job/demo/seed", "2 failed", "active 1"} {
		if !strings.Contains(lines[len(lines)-1], want) {
			t.Errorf("last progress line lacks %q: %s", want, lines[len(lines)-1])
		}
	}
}

func TestObserveReportsProgressForADeploymentToo(t *testing.T) {
	clock := &jobClock{now: mustTime(testNow)}
	var lines []string
	backend := CommandBackend{ObserveLimit: time.Minute, Now: clock.Now, Wait: clock.Wait, Progress: func(l string) { lines = append(lines, l) },
		Run: func(_ context.Context, _ string, args []string, _ []byte) ([]byte, error) {
			if len(args) > 1 && args[1] == "pods" {
				return []byte(`{"items":[]}`), nil
			}
			return []byte(rollingDeployment), nil
		}}
	_, _ = backend.Observe(context.Background(), "Deployment/demo/web")
	if len(lines) == 0 || !strings.Contains(lines[0], "Deployment/demo/web") {
		t.Fatalf("progress lines = %v", lines)
	}
}
