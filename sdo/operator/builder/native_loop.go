package builder

import (
	"context"
	"log/slog"
	"time"

	"sdo.dev/operator/builder/internal/buildio"
	"sdo.dev/operator/memory"
)

// NeedsRebuild reports whether err is the controller process asking its parent
// for a rebuild at the newly accepted revision (memory.HealthRefreshExitCode).
func NeedsRebuild(err error) bool {
	return buildio.ExitCode(err) == memory.HealthRefreshExitCode
}

// RestartPolicy paces passes that end in a rebuild request without the
// accepted revision having moved, so a controller that keeps asking cannot spin.
// A pass at a new revision starts immediately. Zero values select 1 s and 30 s.
type RestartPolicy struct {
	Backoff    time.Duration
	MaxBackoff time.Duration
	// Sleep waits for d or ctx; nil means a real timer.
	Sleep func(ctx context.Context, d time.Duration) error
}

func (p RestartPolicy) withDefaults() RestartPolicy {
	if p.Backoff <= 0 {
		p.Backoff = time.Second
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = 30 * time.Second
	}
	if p.Sleep == nil {
		p.Sleep = buildio.Sleep
	}
	return p
}

// Run is the controller parent loop: it runs RunOnce and, whenever the child
// asks for a rebuild, re-resolves the accepted HEAD and runs again inside the
// same process. A source adoption therefore never ends the pod, so the Job's
// backoff limit is not spent on it. Any other exit, including success, ends Run.
func Run(ctx context.Context, config RunConfig, policy RestartPolicy) error {
	return runLoop(ctx, func(ctx context.Context) (string, error) { return runPass(ctx, config) }, policy)
}

func runLoop(ctx context.Context, pass func(context.Context) (string, error), policy RestartPolicy) error {
	policy = policy.withDefaults()
	var last string
	wait := time.Duration(0)
	for first := true; ; first = false {
		revision, err := pass(ctx)
		if !NeedsRebuild(err) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !first && revision == last {
			if wait == 0 {
				wait = policy.Backoff
			} else if wait *= 2; wait > policy.MaxBackoff {
				wait = policy.MaxBackoff
			}
		} else {
			wait = 0
		}
		last = revision
		slog.Info("controller_rebuild_requested", "revision", revision, "backoff", wait)
		if wait > 0 {
			if err := policy.Sleep(ctx, wait); err != nil {
				return err
			}
		}
	}
}
