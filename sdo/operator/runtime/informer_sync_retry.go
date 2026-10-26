package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// DefaultTerminationLogPath is where Kubernetes reads a container's
// termination message from.
const DefaultTerminationLogPath = "/dev/termination-log"

// InformerSyncPolicy bounds how long the controller tries to sync its
// informers before it gives up with an InformerSyncError. A single slow sync
// (a busy API server, a rolling control plane) is retried in process; only a
// failure that persists ends the process, so one transient timeout does not
// spend one of the Job's few pod retries.
type InformerSyncPolicy struct {
	// Attempts is the number of tries; zero means 3.
	Attempts int
	// Timeout bounds each try; zero means one minute.
	Timeout time.Duration
	// Backoff is the wait before the second try and doubles after that; zero means 5 s.
	Backoff time.Duration
	// Budget bounds all tries and waits together; zero means 3 minutes.
	Budget time.Duration
}

// Validate rejects negative settings; the zero value selects the defaults.
func (p InformerSyncPolicy) Validate() error {
	if p.Attempts < 0 || p.Timeout < 0 || p.Backoff < 0 || p.Budget < 0 {
		return fmt.Errorf("informer sync policy must not be negative: %+v", p)
	}
	return nil
}

func (p InformerSyncPolicy) withDefaults() InformerSyncPolicy {
	if p.Attempts == 0 {
		p.Attempts = 3
	}
	if p.Timeout == 0 {
		p.Timeout = time.Minute
	}
	if p.Backoff == 0 {
		p.Backoff = 5 * time.Second
	}
	if p.Budget == 0 {
		p.Budget = 3 * time.Minute
	}
	return p
}

// Do runs op until it succeeds, the attempts or budget are spent, or ctx ends.
// timeout, when positive, replaces the per-try bound. onRetry is told about
// each failed try that will be followed by another. The last error is returned
// unchanged, so a typed InformerSyncError keeps naming its kinds.
func (p InformerSyncPolicy) Do(ctx context.Context, timeout time.Duration, op func(context.Context) error, onRetry func(attempt int, err error)) error {
	p = p.withDefaults()
	if timeout <= 0 {
		timeout = p.Timeout
	}
	budget, cancelBudget := context.WithTimeout(ctx, p.Budget)
	defer cancelBudget()
	backoff := p.Backoff
	var err error
	for attempt := 1; ; attempt++ {
		tryCtx, cancelTry := context.WithTimeout(budget, timeout)
		err = op(tryCtx)
		cancelTry()
		if err == nil {
			return nil
		}
		if attempt >= p.Attempts || budget.Err() != nil {
			return err
		}
		if onRetry != nil {
			onRetry(attempt, err)
		}
		select {
		case <-budget.Done():
			return err
		case <-time.After(backoff):
		}
		backoff *= 2
	}
}

// controllerErrorRecord is the typed record a failing controller leaves in
// its container termination message and on stdout.
func controllerErrorRecord(err error) (map[string]any, bool) {
	var syncErr *InformerSyncError
	if !errors.As(err, &syncErr) {
		return nil, false
	}
	return map[string]any{"controller_error": "informer_sync", "kinds": syncErr.Kinds, "error": err.Error()}, true
}

// reportControllerError records a typed terminal failure where the pod
// status, receipts and lane tooling can read it. It never replaces the error.
func reportControllerError(err error, stdout io.Writer, terminationLogPath string) {
	record, ok := controllerErrorRecord(err)
	if !ok {
		return
	}
	_ = json.NewEncoder(stdout).Encode(record)
	if terminationLogPath == "" {
		terminationLogPath = DefaultTerminationLogPath
	}
	if content, marshalErr := json.Marshal(record); marshalErr == nil {
		// Outside a container the default path may be absent or unwritable.
		_ = os.WriteFile(terminationLogPath, content, 0o644)
	}
}
