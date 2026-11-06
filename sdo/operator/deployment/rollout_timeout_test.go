package deployment

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

const rollingDeployment = `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web","namespace":"demo","uid":"u","generation":1,
"annotations":{"sdo.dev/source-tree":"t"}},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"web"}},
"template":{"metadata":{"annotations":{"sdo.dev/source-tree":"t"}},"spec":{"initContainers":[{"name":"git","image":"x/git@sha256:` + digestA + `"}],
"containers":[{"name":"web","image":"x/web@sha256:` + digestA + `"}]}}},"status":{"observedGeneration":1,"updatedReplicas":1,"readyReplicas":0}}`

const digestA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func rolloutBackend(pods string, limit time.Duration) CommandBackend {
	return CommandBackend{
		ObserveLimit: limit,
		Run: func(_ context.Context, _ string, args []string, _ []byte) ([]byte, error) {
			if len(args) > 1 && args[1] == "pods" {
				return []byte(pods), nil
			}
			return []byte(rollingDeployment), nil
		},
		Wait: func(ctx context.Context, _ time.Duration) error { <-ctx.Done(); return ctx.Err() },
	}
}

func TestObserveTimeoutForSlowInitContainerIsTypedAndNotAPlanError(t *testing.T) {
	pods := `{"items":[{"metadata":{"name":"web-1"},"status":{"phase":"Pending","initContainerStatuses":[{"name":"git","ready":false,"state":{"running":{}}}],"containerStatuses":[{"name":"web","ready":false,"state":{"waiting":{"reason":"PodInitializing"}}}]}}]}`
	_, err := rolloutBackend(pods, 20*time.Millisecond).Observe(context.Background(), "Deployment/demo/web")
	var timeout *RolloutTimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("want RolloutTimeoutError, got %v", err)
	}
	if !timeout.InfrastructureOutcome() {
		t.Fatal("a still-starting rollout is a time limit, not a plan rejection")
	}
	for _, want := range []string{"still rolling out", "phase=Pending", "init git running", "THE PLAN WAS FINE"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("feedback lacks %q: %v", want, err)
		}
	}
}

func TestObserveTimeoutForFailingPodsIsNotBlamedOnTime(t *testing.T) {
	pods := `{"items":[{"metadata":{"name":"web-1"},"status":{"phase":"Pending","containerStatuses":[{"name":"web","ready":false,"state":{"waiting":{"reason":"ImagePullBackOff"}}}]}}]}`
	_, err := rolloutBackend(pods, 20*time.Millisecond).Observe(context.Background(), "Deployment/demo/web")
	var timeout *RolloutTimeoutError
	if !errors.As(err, &timeout) || timeout.InfrastructureOutcome() {
		t.Fatalf("failing pods must not be classed as a time limit: %v", err)
	}
	if !strings.Contains(err.Error(), "ImagePullBackOff") || strings.Contains(err.Error(), "THE PLAN WAS FINE") {
		t.Fatalf("unexpected feedback: %v", err)
	}
}

const (
	podAge   = `"creationTimestamp":"2026-11-05T00:00:00Z"`
	testNow  = "2026-11-05T00:10:00Z"
	justNow  = `"startedAt":"2026-11-05T00:09:30Z"`
	longAgo  = `"startedAt":"2026-11-05T00:01:00Z"`
	podOpen  = `{"items":[{"metadata":{"name":"web-1",` + podAge + `},"status":`
	podClose = `}]}`
)

func warning(reason, message string) string {
	return `{"reason":"` + reason + `","message":"` + message + `","involvedObject":{"kind":"Pod","name":"web-1"}}`
}

// pastDeadline is the observation state of a rollout that has run to its deadline with no Job running.
var pastDeadline = rolloutInputs{Waited: 10 * time.Minute, Deadline: 10 * time.Minute}

func TestAssessRolloutClassifiesOnPositiveEvidenceOnly(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, testNow)
	cases := []struct {
		name        string
		pods        string
		events      string
		progressing bool
		stuck       bool
		want        []string
	}{
		{"init container running", podOpen + `{"phase":"Pending","initContainerStatuses":[{"name":"git","state":{"running":{` + longAgo + `}}}],"containerStatuses":[{"name":"web","state":{"waiting":{"reason":"PodInitializing"}}}]}` + podClose, "", true, false, []string{"init git running"}},
		{"container creating without failure events", podOpen + `{"phase":"Pending","containerStatuses":[{"name":"web","state":{"waiting":{"reason":"ContainerCreating"}}}]}` + podClose, warning("Unhealthy", "unrelated") + "", true, false, nil},
		{"running just started", podOpen + `{"phase":"Running","containerStatuses":[{"name":"web","ready":false,"state":{"running":{` + justNow + `}}}]}` + podClose, "", true, false, []string{"web running (ready=false)"}},
		{"running never ready (bad probe)", podOpen + `{"phase":"Running","containerStatuses":[{"name":"web","ready":false,"state":{"running":{` + longAgo + `}}}]}` + podClose, warning("Unhealthy", "Readiness probe failed: connection refused"), false, false, []string{"readinessProbe", "Readiness probe failed"}},
		{"crash loop sampled while running", podOpen + `{"phase":"Running","containerStatuses":[{"name":"web","ready":false,"restartCount":4,"lastState":{"terminated":{"exitCode":1,"reason":"Error"}},"state":{"running":{` + justNow + `}}}]}` + podClose, "", false, true, []string{"restarts=4", "last exit 1 Error", "deadline exceeded while container web"}},
		{"one restart past the deadline", podOpen + `{"phase":"Running","containerStatuses":[{"name":"web","ready":false,"restartCount":1,"lastState":{"terminated":{"exitCode":1,"reason":"Error"}},"state":{"running":{` + justNow + `}}}]}` + podClose, "", false, true, []string{"restarts=1"}},
		{"crashloopbackoff", podOpen + `{"phase":"Running","containerStatuses":[{"name":"web","restartCount":2,"state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}` + podClose, "", false, true, []string{"CrashLoopBackOff"}},
		{"unschedulable pending", podOpen + `{"phase":"Pending","conditions":[{"type":"PodScheduled","status":"False","reason":"Unschedulable","message":"0/3 nodes are available: 3 Insufficient cpu."}]}` + podClose, warning("FailedScheduling", "0/3 nodes are available"), false, true, []string{"Unschedulable", "Insufficient cpu", "cannot be scheduled", "FailedScheduling"}},
		{"unbound pvc pending without conditions", podOpen + `{"phase":"Pending"}` + podClose, warning("FailedScheduling", "pod has unbound immediate PersistentVolumeClaims"), false, true, []string{"Pending with no container statuses", "unbound immediate", "PersistentVolumeClaim"}},
		{"missing configmap volume", podOpen + `{"phase":"Pending","containerStatuses":[{"name":"web","state":{"waiting":{"reason":"ContainerCreating"}}}]}` + podClose, warning("FailedMount", `configmap \"web-conf\" not found`), false, true, []string{"FailedMount", "web-conf", "cannot be mounted"}},
		{"image pull backoff", podOpen + `{"phase":"Pending","containerStatuses":[{"name":"web","state":{"waiting":{"reason":"ImagePullBackOff"}}}]}` + podClose, "", false, true, []string{"ImagePullBackOff", "cannot be pulled"}},
		{"main container exited nonzero", podOpen + `{"phase":"Failed","containerStatuses":[{"name":"web","state":{"terminated":{"exitCode":2,"reason":"Error"}}}]}` + podClose, "", false, true, []string{"exit 2"}},
		{"init container failing past the deadline", podOpen + `{"phase":"Pending","initContainerStatuses":[{"name":"wait-db","restartCount":5,"state":{"waiting":{"reason":"CrashLoopBackOff"}}}],"containerStatuses":[{"name":"web","state":{"waiting":{"reason":"PodInitializing"}}}]}` + podClose, "", false, true, []string{"init wait-db", "init container wait-db"}},
		{"zero pods and replicaset cannot create", `{"items":[]}`, `{"reason":"FailedCreate","message":"error looking up service account demo/web: not found","involvedObject":{"kind":"ReplicaSet","name":"web-5d9"}}`, false, true, []string{"no pods exist", "service account", "cannot create pods"}},
		{"zero pods no events", `{"items":[]}`, "", false, false, []string{"no pods exist"}},
		{"ready pod beside a progressing one", `{"items":[{"metadata":{"name":"web-0",` + podAge + `},"status":{"phase":"Running","containerStatuses":[{"name":"web","ready":true,"state":{"running":{` + longAgo + `}}}]}},{"metadata":{"name":"web-1",` + podAge + `},"status":{"phase":"Pending","containerStatuses":[{"name":"web","state":{"waiting":{"reason":"ContainerCreating"}}}]}}]}`, "", true, false, []string{"ready"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events := ""
			if tc.events != "" {
				events = tc.events
			}
			got := assessRollout("Deployment/demo/web", []byte(tc.pods), []byte(`{"items":[`+events+`]}`), now, pastDeadline)
			if got.Progressing != tc.progressing || got.Stuck != tc.stuck {
				t.Fatalf("progressing=%t stuck=%t, want %t/%t: %s", got.Progressing, got.Stuck, tc.progressing, tc.stuck, got.Detail)
			}
			text := got.Detail + " | " + got.Advice
			for _, want := range tc.want {
				if !strings.Contains(text, want) {
					t.Fatalf("evidence lacks %q: %s", want, text)
				}
			}
			if !tc.progressing && got.Advice == "" {
				t.Fatalf("a plan outcome must carry advice: %s", got.Detail)
			}
		})
	}
}

func TestObserveTimeoutForNotProgressingPodsIsAPlanOutcome(t *testing.T) {
	pods := podOpen + `{"phase":"Running","containerStatuses":[{"name":"web","ready":false,"state":{"running":{"startedAt":"2026-11-05T00:00:10Z"}}}]}` + podClose
	backend := rolloutBackend(pods, 20*time.Millisecond)
	backend.Now = func() time.Time { t, _ := time.Parse(time.RFC3339, testNow); return t }
	_, err := backend.Observe(context.Background(), "Deployment/demo/web")
	var timeout *RolloutTimeoutError
	if !errors.As(err, &timeout) || timeout.InfrastructureOutcome() {
		t.Fatalf("a running-but-never-ready pod is a plan outcome: %v", err)
	}
	for _, want := range []string{"not progressing", "plan problem", "readinessProbe"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("feedback lacks %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "THE PLAN WAS FINE") {
		t.Fatalf("must not forbid fixing the plan: %v", err)
	}
}

func TestObserveFailsFastWhenRolloutIsClearlyStuck(t *testing.T) {
	pods := podOpen + `{"phase":"Pending","containerStatuses":[{"name":"web","state":{"waiting":{"reason":"InvalidImageName"}}}]}` + podClose
	clock, _ := time.Parse(time.RFC3339, testNow)
	waits := 0
	backend := rolloutBackend(pods, 10*time.Minute)
	backend.Now = func() time.Time { return clock }
	backend.Wait = func(_ context.Context, d time.Duration) error {
		waits++
		clock = clock.Add(10 * time.Second)
		if waits > 100 {
			t.Fatal("kept waiting on an invalid image")
		}
		return nil
	}
	_, err := backend.Observe(context.Background(), "Deployment/demo/web")
	var timeout *RolloutTimeoutError
	if !errors.As(err, &timeout) || timeout.InfrastructureOutcome() || timeout.Waited >= 10*time.Minute {
		t.Fatalf("want early plan outcome, got %v", err)
	}
	if !strings.Contains(err.Error(), "InvalidImageName") || waits > 6 {
		t.Fatalf("waits=%d err=%v", waits, err)
	}
}

func TestObserveKeepsWaitingWhileInitContainerRuns(t *testing.T) {
	pods := podOpen + `{"phase":"Pending","initContainerStatuses":[{"name":"git","state":{"running":{"startedAt":"2026-11-05T00:09:00Z"}}}],"containerStatuses":[{"name":"web","state":{"waiting":{"reason":"PodInitializing"}}}]}` + podClose
	clock, _ := time.Parse(time.RFC3339, testNow)
	backend := rolloutBackend(pods, time.Minute)
	backend.Now = func() time.Time { return clock }
	backend.Wait = func(ctx context.Context, d time.Duration) error {
		clock = clock.Add(10 * time.Second)
		if clock.After(mustTime(testNow).Add(61 * time.Second)) {
			return context.DeadlineExceeded
		}
		return nil
	}
	_, err := backend.Observe(context.Background(), "Deployment/demo/web")
	if err == nil || strings.Contains(err.Error(), "not progressing") {
		t.Fatalf("a progressing rollout must not fail fast: %v", err)
	}
}

func mustTime(s string) time.Time { t, _ := time.Parse(time.RFC3339, s); return t }

const retryingInitPod = `{"phase":"Pending","initContainerStatuses":[{"name":"dependencies","restartCount":4,"state":{"waiting":{"reason":"CrashLoopBackOff"}},"lastState":{"terminated":{"exitCode":1,"reason":"Error","message":"waiting for synthetic-job"}}}],"containerStatuses":[{"name":"web","state":{"waiting":{"reason":"PodInitializing"}}}]}`

// A restarting init container is not yet progressed, not stuck, until the
// progress deadline, and never while a Job of the same or an earlier phase runs.
func TestInitContainerRetriesAreNotStuckBeforeTheDeadline(t *testing.T) {
	now := mustTime(testNow)
	pods := []byte(podOpen + retryingInitPod + podClose)
	early := rolloutInputs{Waited: 80 * time.Second, Deadline: 10 * time.Minute}
	got := assessRollout("Deployment/demo/web", pods, []byte(`{"items":[]}`), now, early)
	if got.Stuck || !got.Progressing {
		t.Fatalf("init retry before the deadline must not be stuck: %+v", got)
	}
	jobs := rolloutInputs{Waited: time.Hour, Deadline: 10 * time.Minute, RunningJobs: []string{"Job/demo/migrate"}}
	got = assessRollout("Deployment/demo/web", pods, []byte(`{"items":[]}`), now, jobs)
	if got.Stuck || !got.Progressing || !strings.Contains(got.Detail, "last exit 1") {
		t.Fatalf("init retry while a Job runs must not be stuck: %+v", got)
	}
	got = assessRollout("Deployment/demo/web", pods, []byte(`{"items":[]}`), now, pastDeadline)
	if !got.Stuck || got.Progressing || got.RetryKind != retryInit || got.reason() != RolloutReasonInitRetryDeadline {
		t.Fatalf("init retry past the deadline must be stuck with its own reason: %+v", got)
	}
	for _, want := range []string{"deadline exceeded while init container dependencies was retrying", "last exit 1 Error", "waiting for synthetic-job"} {
		if !strings.Contains(got.Detail+got.Advice, want) {
			t.Fatalf("evidence lacks %q: %s | %s", want, got.Detail, got.Advice)
		}
	}
}

func TestInitRetryGraceDoesNotHideFastFailures(t *testing.T) {
	now := mustTime(testNow)
	jobs := rolloutInputs{Waited: time.Second, Deadline: 10 * time.Minute, RunningJobs: []string{"Job/demo/migrate"}}
	for name, pods := range map[string]string{
		"image pull":    podOpen + `{"phase":"Pending","containerStatuses":[{"name":"web","state":{"waiting":{"reason":"ImagePullBackOff"}}}]}` + podClose,
		"invalid image": podOpen + `{"phase":"Pending","containerStatuses":[{"name":"web","state":{"waiting":{"reason":"InvalidImageName"}}}]}` + podClose,
		"unschedulable": podOpen + `{"phase":"Pending","conditions":[{"type":"PodScheduled","status":"False","reason":"Unschedulable"}]}` + podClose,
	} {
		got := assessRollout("Deployment/demo/web", []byte(pods), []byte(`{"items":[]}`), now, jobs)
		if !got.Stuck && name != "image pull" {
			t.Fatalf("%s must keep its fast fail: %+v", name, got)
		}
		if got.Progressing {
			t.Fatalf("%s must not be progressing: %+v", name, got)
		}
	}
}

func jobBackend(pods, job, deployment string, limit time.Duration) CommandBackend {
	return CommandBackend{
		ObserveLimit: limit,
		Run: func(_ context.Context, _ string, args []string, _ []byte) ([]byte, error) {
			switch {
			case len(args) > 1 && args[1] == "pods":
				return []byte(pods), nil
			case len(args) > 1 && args[1] == "job":
				return []byte(job), nil
			}
			return []byte(deployment), nil
		},
	}
}

func observeWithClock(b CommandBackend, ctx context.Context, step time.Duration, maxWaits int) (time.Duration, error) {
	clock := mustTime(testNow)
	waits := 0
	b.Now = func() time.Time { return clock }
	b.Wait = func(ctx context.Context, _ time.Duration) error {
		waits++
		clock = clock.Add(step)
		if waits > maxWaits {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	_, err := b.Observe(ctx, "Deployment/demo/web")
	return time.Duration(waits) * step, err
}

func TestObserveDoesNotFailInitRetriesWhileEarlierJobRuns(t *testing.T) {
	pods := podOpen + retryingInitPod + podClose
	runningJob := `{"metadata":{"name":"migrate"},"spec":{"completions":1},"status":{"active":1}}`
	ctx := withJobsInFlight(context.Background(), []string{"Job/demo/migrate"})
	_, err := observeWithClock(jobBackend(pods, runningJob, rollingDeployment, 20*time.Millisecond), ctx, 10*time.Second, 0)
	var timeout *RolloutTimeoutError
	if !errors.As(err, &timeout) || timeout.Stalled || timeout.Reason != RolloutReasonProgressing {
		t.Fatalf("a running Job makes init retries a time limit, never a stall: %v", err)
	}
	finished := `{"metadata":{"name":"migrate"},"spec":{"completions":1},"status":{"succeeded":1}}`
	_, err = observeWithClock(jobBackend(pods, finished, rollingDeployment, 20*time.Millisecond), ctx, 10*time.Second, 0)
	if !errors.As(err, &timeout) || !timeout.Stalled || timeout.Reason != RolloutReasonInitRetryDeadline {
		t.Fatalf("with no Job running the deadline ends the wait with the init reason: %v", err)
	}
	if !strings.Contains(err.Error(), "dependencies") || !strings.Contains(err.Error(), "waiting for synthetic-job") {
		t.Fatalf("error must name the container and its last message: %v", err)
	}
}

func TestObserveUsesProgressDeadlineForInitRetries(t *testing.T) {
	deployment := strings.Replace(rollingDeployment, `"spec":{"replicas":1,`, `"spec":{"progressDeadlineSeconds":120,"replicas":1,`, 1)
	pods := podOpen + retryingInitPod + podClose
	waited, err := observeWithClock(jobBackend(pods, "", deployment, 10*time.Minute), context.Background(), 10*time.Second, 100)
	var timeout *RolloutTimeoutError
	if !errors.As(err, &timeout) || timeout.Reason != RolloutReasonInitRetryDeadline {
		t.Fatalf("want init retry deadline, got %v", err)
	}
	if waited < 120*time.Second || waited > 150*time.Second {
		t.Fatalf("stalled after %s, want about the 120s progress deadline", waited)
	}
}

const retryingMainPod = `{"phase":"Running","initContainerStatuses":[{"name":"dependencies","ready":true,"state":{"terminated":{"exitCode":0,"reason":"Completed"}}}],"containerStatuses":[{"name":"app","restartCount":3,"state":{"waiting":{"reason":"CrashLoopBackOff"}},"lastState":{"terminated":{"exitCode":1,"reason":"Error","message":"database not migrated"}}}]}`

// A main container that crash-loops is not yet progressed, never stuck, while a
// Job of the same or an earlier phase runs, and until the progress deadline after.
func TestMainContainerRestartsAreNotStuckBeforeTheDeadline(t *testing.T) {
	now := mustTime(testNow)
	pods := []byte(podOpen + retryingMainPod + podClose)
	for name, in := range map[string]rolloutInputs{
		"before deadline": {Waited: 3 * time.Minute, Deadline: 10 * time.Minute},
		"job running":     {Waited: time.Hour, Deadline: 10 * time.Minute, RunningJobs: []string{"Job/demo/migrate"}},
	} {
		got := assessRollout("Deployment/demo/web", pods, []byte(`{"items":[]}`), now, in)
		if got.Stuck || !got.Progressing {
			t.Fatalf("%s: main container retry must not be stuck: %+v", name, got)
		}
	}
	got := assessRollout("Deployment/demo/web", pods, []byte(`{"items":[]}`), now, pastDeadline)
	if !got.Stuck || got.Progressing || got.reason() != RolloutReasonContainerRetryDeadline {
		t.Fatalf("past the deadline: want stuck with the container reason: %+v", got)
	}
	for _, want := range []string{"container app was retrying", "database not migrated"} {
		if !strings.Contains(got.Detail+got.Advice, want) {
			t.Fatalf("evidence lacks %q: %s | %s", want, got.Detail, got.Advice)
		}
	}
}

func TestMainContainerRetryGraceDoesNotHideFastFailures(t *testing.T) {
	jobs := rolloutInputs{Waited: time.Second, Deadline: 10 * time.Minute, RunningJobs: []string{"Job/demo/migrate"}}
	pods := podOpen + `{"phase":"Running","containerStatuses":[{"name":"app","restartCount":3,"state":{"waiting":{"reason":"CrashLoopBackOff"}}},{"name":"side","state":{"waiting":{"reason":"CreateContainerConfigError"}}}]}` + podClose
	got := assessRollout("Deployment/demo/web", []byte(pods), []byte(`{"items":[]}`), mustTime(testNow), jobs)
	if !got.Stuck || got.Progressing {
		t.Fatalf("a config error beside a retrying container must fail fast: %+v", got)
	}
}

func TestObserveDoesNotFailMainContainerRestartsWhileEarlierJobRuns(t *testing.T) {
	pods := podOpen + retryingMainPod + podClose
	runningJob := `{"metadata":{"name":"migrate"},"spec":{"completions":1},"status":{"active":1}}`
	ctx := withJobsInFlight(context.Background(), []string{"Job/demo/migrate"})
	_, err := observeWithClock(jobBackend(pods, runningJob, rollingDeployment, 20*time.Millisecond), ctx, 10*time.Second, 0)
	var timeout *RolloutTimeoutError
	if !errors.As(err, &timeout) || timeout.Stalled || timeout.Reason != RolloutReasonProgressing {
		t.Fatalf("a running Job makes main restarts a time limit: %v", err)
	}
	waited, err := observeWithClock(jobBackend(pods, "", rollingDeployment, 10*time.Minute), context.Background(), 10*time.Second, 100)
	if !errors.As(err, &timeout) || timeout.Reason != RolloutReasonContainerRetryDeadline {
		t.Fatalf("want container retry deadline, got %v", err)
	}
	if waited < 10*time.Minute {
		t.Fatalf("failed after %s, before the 10m deadline", waited)
	}
}
