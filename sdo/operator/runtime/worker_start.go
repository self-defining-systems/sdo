package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/worker_registry"
)

// errWorkerReplaced ends one pass of a dispatch whose worker did nothing and
// was recorded, so the dispatch runs a fresh worker at once.
var errWorkerReplaced = errors.New("responder worker never started; starting a fresh one")

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

// startsExhausted finishes an interrupted retirement and reports a task whose
// allowed workers all did nothing.
func startsExhausted(ctx context.Context, executor worker_registry.KubernetesExecutor, task *contracts.AgentTask) error {
	if err := executor.DropRetired(ctx, task); err != nil {
		return fmt.Errorf("drop retired responder worker: %w", err)
	}
	failures, err := executor.StartFailures(task)
	if err != nil {
		return fmt.Errorf("read responder start failures: %w", err)
	}
	if len(failures) < worker_registry.MaxWorkerStarts {
		return nil
	}
	last := failures[0]
	for _, failure := range failures[1:] {
		if failure.RecordedAt.After(last.RecordedAt) {
			last = failure
		}
	}
	return &WorkerNeverStartedError{Attempts: len(failures), Reason: last.Reason}
}

// settleWorkerStart returns errWorkerReplaced or a WorkerNeverStartedError for
// a worker that did nothing, and nil to leave err to the rules for a worker
// that started.
func (d KubernetesJobDispatcher) settleWorkerStart(ctx context.Context, executor worker_registry.KubernetesExecutor, task *contracts.AgentTask, loop *ResponderLoop, request IncidentRequest, calls *atomic.Int64, err error) error {
	var failed *worker_registry.JobFailedError
	var vanished *worker_registry.JobVanishedError
	job := ""
	switch {
	case errors.As(err, &failed):
		job = failed.JobName
	case errors.As(err, &vanished):
		job = vanished.JobName
	}
	// A response that ran out of time ends as a response timeout.
	outOfTime := !request.ResponseDeadline.IsZero() && !lifecycleio.Now().Before(request.ResponseDeadline)
	if job == "" || ctx.Err() != nil || executor.SessionPVCSubPath == "" || outOfTime || calls.Load() > 0 {
		return nil
	}
	session := ""
	if loop != nil {
		loop.locked(func() { session = loop.menuSession.GetProviderSessionId() })
	}
	if session != "" {
		return nil
	}
	retired, retireErr := executor.RetireNeverStarted(ctx, task, job, err.Error())
	if retireErr != nil {
		return fmt.Errorf("retire responder worker: %w", errors.Join(retireErr, err))
	}
	if !retired {
		return nil
	}
	slog.Error("responder_worker_never_started", "incident_id", request.IncidentID, "job", job, "reason", err.Error())
	if err := startsExhausted(ctx, executor, task); err != nil {
		return err
	}
	return errWorkerReplaced
}
