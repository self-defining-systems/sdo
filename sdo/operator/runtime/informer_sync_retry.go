package runtime

import (
	"context"
	"fmt"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"time"
)

// InformerSyncPolicy paces the controller's attempts to sync its informers. A
// sync that fails (a busy API server, a rolling control plane, list errors) is
// transient, so the controller never gives up on it: it retries with growing
// backoff until the informers sync or the run ends. A controller that exited
// here spent one of the Job's few pod retries on a failure that clears by
// itself, and repeated exits left the Job Failed.
type InformerSyncPolicy struct {
	// Timeout bounds each try; zero means one minute.
	Timeout time.Duration
	// Backoff is the wait before the second try and doubles after that; zero means 5 s.
	Backoff time.Duration
	// MaxBackoff caps the wait between tries; zero means 30 s.
	MaxBackoff time.Duration
}

// Validate rejects negative settings; the zero value selects the defaults.
func (p InformerSyncPolicy) Validate() error {
	if p.Timeout < 0 || p.Backoff < 0 || p.MaxBackoff < 0 {
		return fmt.Errorf("informer sync policy must not be negative: %+v", p)
	}
	return nil
}

func (p InformerSyncPolicy) withDefaults() InformerSyncPolicy {
	if p.Timeout == 0 {
		p.Timeout = time.Minute
	}
	if p.Backoff == 0 {
		p.Backoff = 5 * time.Second
	}
	if p.MaxBackoff == 0 {
		p.MaxBackoff = 30 * time.Second
	}
	return p
}

// Do runs op until it succeeds or ctx ends. timeout, when positive, replaces
// the per-try bound. onRetry is told about each failed try, which is always
// followed by another. When ctx ends the last error is returned unchanged, so a
// typed InformerSyncError keeps naming its kinds.
func (p InformerSyncPolicy) Do(ctx context.Context, timeout time.Duration, op func(context.Context) error, onRetry func(attempt int, err error)) error {
	p = p.withDefaults()
	if timeout <= 0 {
		timeout = p.Timeout
	}
	backoff := p.Backoff
	for attempt := 1; ; attempt++ {
		tryCtx, cancelTry := context.WithTimeout(ctx, timeout)
		err := op(tryCtx)
		cancelTry()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		if onRetry != nil {
			onRetry(attempt, err)
		}
		select {
		case <-ctx.Done():
			return err
		case <-lifecycleio.After(backoff):
		}
		backoff = min(backoff*2, p.MaxBackoff)
	}
}
