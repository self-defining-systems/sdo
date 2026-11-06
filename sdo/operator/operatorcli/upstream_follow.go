package operatorcli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/upstream"
)

// upstreamFollowing is the controller parent's upstream follower host: it
// reads the accepted sources and history, polls each followed upstream, runs
// the advance and records the follower's holds. It owns no decision; those
// are upstream.Follower's (when, what, coalescing, holds) and
// lifecycle.Advancer's (whether a revision is accepted).
//
// With no accepted .sdo/goal/sources.yaml it only re-reads that file each
// idle interval: no upstream, broker, ConfigMap or cluster call is made.
type upstreamFollowing struct {
	Follower *upstream.Follower
	// Sources reads the accepted sources.yaml (nil, nil when absent).
	Sources func(context.Context) ([]upstream.Source, error)
	// Accepted is the newest broker-written SDO-Upstream trailer on the
	// accepted history (found false when none).
	Accepted func(context.Context) (commit string, found bool, err error)
	Latest   func(context.Context, upstream.Source) (upstream.Revision, error)
	Advance  func(context.Context, upstream.Request) (lifecycle.AdvanceResult, error)
	Holds    holdsStore
	Now      func() time.Time
	// SleepUntil waits for the time or ctx.
	SleepUntil func(context.Context, time.Time) error
	// Idle is how long to wait before re-reading sources when nothing is
	// followed or a step failed; zero selects upstream.DefaultPollInterval.
	Idle time.Duration
	Log  *slog.Logger
}

// holdsStore persists the follower's holds (the sdo-upstream-holds ConfigMap).
type holdsStore interface {
	Load(context.Context) ([]upstream.Hold, error)
	Save(context.Context, []upstream.Hold) error
}

// Run follows until ctx ends or following is disabled for this installation
// (lifecycle.ErrNoAcceptedPlan). Failures of one poll are logged and retried;
// they never end the controller.
func (f upstreamFollowing) Run(ctx context.Context) error {
	if f.Follower == nil || f.Sources == nil || f.Accepted == nil || f.Latest == nil || f.Advance == nil || f.Holds == nil || f.Now == nil || f.SleepUntil == nil {
		return errors.New("upstream following needs a follower, sources, accepted history, watcher, advance, holds store, clock and timer")
	}
	idle := f.Idle
	if idle <= 0 {
		idle = upstream.DefaultPollInterval
	}
	log := f.Log
	if log == nil {
		log = slog.Default()
	}
	holdsLoaded := false
	for {
		next, err := f.step(ctx, &holdsLoaded, log)
		if errors.Is(err, lifecycle.ErrNoAcceptedPlan) {
			log.Warn("upstream_following_disabled", "reason", err.Error())
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			log.Warn("upstream_following_step_failed", "error", err.Error())
		}
		wake := f.Now().Add(idle)
		if err == nil && !next.IsZero() && next.Before(wake) {
			wake = next
		}
		if err := f.SleepUntil(ctx, wake); err != nil {
			return err
		}
	}
}

// step re-reads the accepted sources, polls every due source once and returns
// when the follower next wants to poll (zero when nothing is followed).
func (f upstreamFollowing) step(ctx context.Context, holdsLoaded *bool, log *slog.Logger) (time.Time, error) {
	sources, err := f.Sources(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("read accepted sources: %w", err)
	}
	if len(sources) == 0 && !*holdsLoaded {
		return time.Time{}, nil
	}
	if !*holdsLoaded {
		holds, err := f.Holds.Load(ctx)
		if err != nil {
			return time.Time{}, fmt.Errorf("load upstream holds: %w", err)
		}
		f.Follower.LoadHolds(holds)
		*holdsLoaded = true
	}
	changed, err := f.Follower.Sync(f.Now(), sources)
	if err != nil {
		return time.Time{}, fmt.Errorf("sync followed sources: %w", err)
	}
	if err := f.saveHolds(ctx, changed); err != nil {
		return time.Time{}, err
	}
	for _, source := range f.Follower.Due(f.Now()) {
		if err := f.poll(ctx, source, log); err != nil {
			return time.Time{}, err
		}
	}
	next, _ := f.Follower.NextPoll()
	return next, nil
}

func (f upstreamFollowing) poll(ctx context.Context, source upstream.Source, log *slog.Logger) error {
	accepted, _, err := f.Accepted(ctx)
	if err != nil {
		return fmt.Errorf("accepted upstream revision: %w", err)
	}
	latest, watchErr := f.Latest(ctx, source)
	decision := f.Follower.Observe(f.Now(), source.Path, accepted, latest, watchErr)
	if err := f.saveHolds(ctx, decision.HoldsChanged); err != nil {
		return err
	}
	attrs := []any{"path", source.Path, "kind", source.Kind, "decision", decision.Kind.String(), "accepted", accepted, "latest", latest.ID}
	if decision.Err != nil {
		attrs = append(attrs, "error", decision.Err.Error())
	}
	log.Info("upstream_poll", attrs...)
	if decision.Kind != upstream.Advance {
		return nil
	}
	result, err := f.Advance(ctx, decision.Request)
	if errors.Is(err, lifecycle.ErrNoAcceptedPlan) {
		return err
	}
	outcome := result.FollowerOutcome()
	// An error is transient for the follower: retry the same revision next
	// poll without a hold. After a deploy (publication refused because HEAD
	// moved, broker failure) the cluster runs the verified candidate while
	// accepted HEAD is older until that retry publishes it.
	if err != nil {
		outcome = upstream.Outcome{Kind: upstream.Retry}
	}
	attrs = []any{"path", source.Path, "revision", decision.Request.Revision.ID, "prev", decision.Request.Prev, "outcome", string(result.Outcome), "upstream", result.Upstream}
	switch {
	case err != nil:
		attrs = append(attrs, "error", err.Error())
	case result.Outcome == lifecycle.AdvanceHeld:
		attrs = append(attrs, "hold", outcome.Reason)
	case result.Outcome == lifecycle.AdvanceDeferred:
		attrs = append(attrs, "deferred", result.Detail)
	case result.Outcome == lifecycle.AdvanceAccepted:
		attrs = append(attrs, "accepted_commit", result.Receipt.Commit)
	}
	log.Info("upstream_advance", attrs...)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	changed, err := f.Follower.Finish(f.Now(), source.Path, outcome)
	if err != nil {
		return fmt.Errorf("finish advance: %w", err)
	}
	return f.saveHolds(ctx, changed)
}

func (f upstreamFollowing) saveHolds(ctx context.Context, changed bool) error {
	if !changed {
		return nil
	}
	if err := f.Holds.Save(ctx, f.Follower.Holds()); err != nil {
		return fmt.Errorf("save upstream holds: %w", err)
	}
	return nil
}
