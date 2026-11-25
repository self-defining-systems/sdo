package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/worker_registry"
)

// errRedispatch ends one pass of a dispatch that runs again at once: its worker
// never started and was replaced, or it is alive and the next pass rejoins it.
var errRedispatch = errors.New("run the responder dispatch again")

// WorkerNeverStartedError ends a dispatch whose workers all ended before their
// first model turn. The controller escalates the incident.
type WorkerNeverStartedError struct {
	Attempts int
	Reason   string
}

func (e *WorkerNeverStartedError) Error() string {
	return fmt.Sprintf("responder worker ended before its first turn (%d started): %s", e.Attempts, e.Reason)
}

func workerNeverStarted(err error) bool {
	var never *WorkerNeverStartedError
	return errors.As(err, &never)
}

func neverStarted(failures []worker_registry.StartupFailure) *WorkerNeverStartedError {
	last := failures[len(failures)-1]
	return &WorkerNeverStartedError{Attempts: len(failures), Reason: last.Reason}
}

// workerActivity counts what a dispatch's worker asked of the tool server.
type workerActivity struct {
	mu                    sync.Mutex
	calls, turns, pending int
}

// received records a call and returns the function that records its answer.
// The worker itself opens the session; the provider's first event checkpoints
// it; every other call comes from a model turn.
func (a *workerActivity) received(command string) func() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	a.pending++
	if command != "session-open" && command != "session-checkpoint" {
		a.turns++
	}
	return func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.calls++
		a.pending--
	}
}

func (a *workerActivity) turnCalls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.turns
}

func (a *workerActivity) startWatch() *worker_registry.WorkerStartWatch {
	return &worker_registry.WorkerStartWatch{
		Turns:    a.turnCalls,
		Activity: func() int { a.mu.Lock(); defer a.mu.Unlock(); return a.calls },
		Waiting:  func() bool { a.mu.Lock(); defer a.mu.Unlock(); return a.pending > 0 },
	}
}

// settleWorkerStart decides what a failed execution means. Nil leaves err to
// the rules for a worker that started. A worker that took no turn is fenced and
// recorded; the result is errRedispatch when a fresh worker may start, or
// a WorkerNeverStartedError when none may.
func (d KubernetesJobDispatcher) settleWorkerStart(ctx context.Context, executor worker_registry.KubernetesExecutor, task *contracts.AgentTask, loop *ResponderLoop, request IncidentRequest, err error) error {
	var failed *worker_registry.JobFailedError
	var vanished *worker_registry.JobVanishedError
	var stalled *worker_registry.WorkerStalledError
	job := ""
	switch {
	case ctx.Err() != nil || executor.StartWatch == nil:
		return nil
	case errors.As(err, &failed):
		job = failed.JobName
	case errors.As(err, &vanished):
		job = vanished.JobName
	case errors.As(err, &stalled):
		job = stalled.JobName
	default:
		return nil
	}
	// A response that ran out of time ends as a response timeout.
	if !request.ResponseDeadline.IsZero() && !lifecycleio.Now().Before(request.ResponseDeadline) {
		if stalled != nil {
			return &ResponderJobFailedError{JobName: job, Cause: err}
		}
		return nil
	}
	never, inspectErr := executor.NeverStarted(ctx, task, job)
	switch {
	case inspectErr != nil:
		return fmt.Errorf("inspect responder worker start: %w", errors.Join(inspectErr, err))
	case !never && stalled != nil:
		return errRedispatch // It started while it was judged: rejoin it.
	case !never:
		return nil
	}
	retired, retireErr := executor.RetireNeverStarted(ctx, task, job, err.Error())
	if retireErr != nil {
		return fmt.Errorf("retire responder worker: %w", errors.Join(retireErr, err))
	}
	if !retired {
		// It took its turn or published before the fence reached it. Its result
		// counts; without one it is a started worker that died.
		if _, recovered, recoverErr := executor.RecoverCompleted(ctx, task); recovered {
			return errRedispatch
		} else if recoverErr != nil && !errors.As(recoverErr, new(*worker_registry.JobVanishedError)) {
			return recoverErr
		}
		return &ResponderJobFailedError{JobName: job, Cause: err}
	}
	failures, readErr := executor.StartFailures(task)
	if readErr != nil {
		return fmt.Errorf("read responder start failures: %w", readErr)
	}
	slog.Error("responder_worker_never_started", "incident_id", request.IncidentID, "job", job, "attempt", len(failures), "max_attempts", worker_registry.MaxWorkerStarts, "reason", err.Error())
	if startsExhausted(failures, loop) {
		return neverStarted(failures)
	}
	return errRedispatch
}

// startsExhausted reports that no further worker may start. A provider that
// opened a session and refused the turn (a rejected credential) refuses it on
// any worker, and a session has one writer.
func startsExhausted(failures []worker_registry.StartupFailure, loop *ResponderLoop) bool {
	if len(failures) == 0 {
		return false
	}
	session := ""
	if loop != nil {
		loop.locked(func() { session = loop.menuSession.GetProviderSessionId() })
	}
	return len(failures) >= worker_registry.MaxWorkerStarts || session != ""
}
