package operatorcli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
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
	// Obligations persists work an accepted advance still owes (a failed
	// detection refresh) and Refresh retries it; both are required.
	Obligations obligationsStore
	Refresh     func(context.Context, upstream.Obligation) error
	Now         func() time.Time
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

// obligationsStore persists the follower's open obligations (the
// sdo-upstream-obligations ConfigMap).
type obligationsStore interface {
	// Load returns the valid obligations and, apart, the records it could not
	// decode: those are held individually and never stop following. Save
	// leaves quarantined records as they are.
	Load(context.Context) ([]upstream.Obligation, []upstream.Quarantined, error)
	Save(context.Context, []upstream.Obligation) error
}

// validate refuses a following built without a collaborator: each one's
// absence would silently disable a part of following.
func (f upstreamFollowing) validate() error {
	return lifecycle.RequireCollaborators("upstream following", map[string]bool{
		"follower": f.Follower != nil, "sources": f.Sources != nil, "accepted history": f.Accepted != nil, "watcher": f.Latest != nil,
		"advance": f.Advance != nil, "holds store": f.Holds != nil, "obligations store": f.Obligations != nil, "refresh": f.Refresh != nil,
		"clock": f.Now != nil, "timer": f.SleepUntil != nil,
	}, "follower", "sources", "accepted history", "watcher", "advance", "holds store", "obligations store", "refresh", "clock", "timer")
}

// Run follows until ctx ends or following is disabled for this installation
// (lifecycle.ErrNoAcceptedPlan). Failures of one poll are logged and retried;
// they never end the controller.
func (f upstreamFollowing) Run(ctx context.Context) error {
	if err := f.validate(); err != nil {
		return err
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
		owed, quarantined, err := f.Obligations.Load(ctx)
		if err != nil {
			return time.Time{}, fmt.Errorf("load upstream obligations: %w", err)
		}
		for _, q := range quarantined {
			log.Error("upstream_obligation_quarantined", "key", q.Key, "cause", upstream.QuarantinedCause, "owner", "operator", "reason", q.Reason)
		}
		f.Follower.LoadObligations(f.Now(), owed)
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
	if err := f.retryObligations(ctx, log); err != nil {
		return time.Time{}, err
	}
	next, found := f.Follower.NextPoll()
	if due, owed := f.Follower.NextObligation(); owed && (!found || due.Before(next)) {
		next = due
	}
	return next, nil
}

// retryObligations runs every open obligation whose backoff has elapsed. A
// failure keeps it open with a longer backoff; only success settles it.
func (f upstreamFollowing) retryObligations(ctx context.Context, log *slog.Logger) error {
	for _, o := range f.Follower.DueObligations(f.Now()) {
		err := f.Refresh(ctx, o)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		attrs := []any{"path", o.Path, "obligation", string(o.Kind), "accepted", o.Accepted, "attempts", o.Attempts + 1, "cause", o.Cause, "owner", o.Owner}
		if err != nil {
			hold := obligationHold(err)
			f.Follower.Retried(f.Now(), o, err.Error(), hold)
			log.Warn("upstream_obligation_retry_failed", append(attrs, "error", err.Error())...)
			if held, ok := f.heldObligation(o); ok {
				log.Error("upstream_obligation_held", "path", o.Path, "obligation", string(o.Kind), "accepted", o.Accepted, "attempts", held.Attempts, "cause", held.Held, "owner", held.HeldOwner, "reason", held.Reason)
			}
		} else {
			f.Follower.Settle(o)
			log.Info("upstream_obligation_settled", attrs...)
		}
		if err := f.saveObligations(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (f upstreamFollowing) heldObligation(o upstream.Obligation) (upstream.Obligation, bool) {
	for _, current := range f.Follower.Obligations() {
		if current.Path == o.Path && current.Kind == o.Kind {
			return current, current.Held != ""
		}
	}
	return upstream.Obligation{}, false
}

// obligationHold reads a refresh failure no retry can fix: a terminal
// operability stop (agent_not_converging, base_invalid, policy, ...) carries
// its cause and owner. Anything else is a transient failure, retried within
// the attempt budget.
func obligationHold(err error) *upstream.ObligationHold {
	var stop *lifecycle.OperabilityError
	if !errors.As(err, &stop) || stop.Decision.Kind != operability.Hold {
		return nil
	}
	hold := &upstream.ObligationHold{Owner: string(stop.Decision.Owner)}
	if len(stop.Decision.Checks) > 0 {
		hold.Cause = string(stop.Decision.Checks[0].Cause)
	}
	if hold.Cause == "" {
		hold.Cause = string(operability.CauseInternal)
	}
	return hold
}

func (f upstreamFollowing) saveObligations(ctx context.Context) error {
	if err := f.Obligations.Save(ctx, f.Follower.Obligations()); err != nil {
		return fmt.Errorf("save upstream obligations: %w", err)
	}
	return nil
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
	// An accepted advance that still owes work keeps that work open whatever
	// else happened (even an interrupted refresh), bound to the accepted
	// commit: the next poll sees the revision as accepted and clears the hold,
	// not the obligation.
	if result.Owed != nil {
		if _, oerr := f.Follower.Owe(f.Now(), upstream.Obligation{
			Path: source.Path, Kind: upstream.ObligationDesignerRefresh, Accepted: result.Owed.Accepted, Upstream: result.Owed.Upstream,
			Cause: string(result.Cause), Owner: string(result.Owner), Reason: result.Detail,
		}); oerr != nil {
			return fmt.Errorf("record owed designer refresh: %w", oerr)
		}
		if serr := f.saveObligations(ctx); serr != nil {
			return serr
		}
	}
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
