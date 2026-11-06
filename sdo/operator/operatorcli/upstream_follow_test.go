package operatorcli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/upstream"
)

// followWorld is a scripted upstream, accepted history and advance. Every
// effect is logged so tests assert what the host did, not how.
type followWorld struct {
	t        *testing.T
	now      time.Time
	sources  []upstream.Source
	accepted string
	latest   []string // upstream head per poll; the last repeats
	polls    int
	advance  func(upstream.Request) (lifecycle.AdvanceResult, error)
	advances []upstream.Request
	stored   []upstream.Hold
	owed     []upstream.Obligation
	// quarantined are the records the store could not decode.
	quarantined []upstream.Quarantined
	// refresh answers each retry of an open obligation; nil fails the test.
	refresh   func(upstream.Obligation) error
	refreshes []time.Time
	saves     int
	sleeps    int
	maxSleep  int
	cancel    context.CancelFunc
	touched   []string
}

func (w *followWorld) following() upstreamFollowing {
	follower, err := upstream.NewFollower(upstream.FollowerConfig{})
	if err != nil {
		w.t.Fatal(err)
	}
	return upstreamFollowing{
		Follower: follower,
		Sources:  func(context.Context) ([]upstream.Source, error) { return w.sources, nil },
		Accepted: func(context.Context) (string, bool, error) {
			w.touched = append(w.touched, "accepted")
			return w.accepted, w.accepted != "", nil
		},
		Latest: func(context.Context, upstream.Source) (upstream.Revision, error) {
			w.touched = append(w.touched, "latest")
			i := min(w.polls, len(w.latest)-1)
			w.polls++
			return upstream.Revision{ID: w.latest[i], Ref: "main"}, nil
		},
		Advance: func(_ context.Context, request upstream.Request) (lifecycle.AdvanceResult, error) {
			w.advances = append(w.advances, request)
			result, err := w.advance(request)
			if result.Outcome == lifecycle.AdvanceAccepted {
				w.accepted = request.Revision.ID
			}
			return result, err
		},
		Holds:       (*worldHolds)(w),
		Obligations: (*worldObligations)(w),
		Refresh: func(_ context.Context, o upstream.Obligation) error {
			w.refreshes = append(w.refreshes, w.now)
			return w.refresh(o)
		},
		Log: slog.New(slog.DiscardHandler),
		Now: func() time.Time { return w.now },
		SleepUntil: func(ctx context.Context, until time.Time) error {
			w.sleeps++
			if w.sleeps > w.maxSleep {
				w.cancel()
				return ctx.Err()
			}
			if until.After(w.now) {
				w.now = until
			}
			return nil
		},
	}
}

type worldObligations followWorld

func (o *worldObligations) Load(context.Context) ([]upstream.Obligation, []upstream.Quarantined, error) {
	return o.owed, o.quarantined, nil
}

func (o *worldObligations) Save(_ context.Context, owed []upstream.Obligation) error {
	o.owed = owed
	return nil
}

type worldHolds followWorld

func (h *worldHolds) Load(context.Context) ([]upstream.Hold, error) {
	h.touched = append(h.touched, "holds")
	return h.stored, nil
}

func (h *worldHolds) Save(_ context.Context, holds []upstream.Hold) error {
	h.stored, h.saves = holds, h.saves+1
	return nil
}

func (w *followWorld) run(maxSleep int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.cancel, w.maxSleep = cancel, maxSleep
	return w.following().Run(ctx)
}

const (
	revA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	revB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	revC = "cccccccccccccccccccccccccccccccccccccccc"
)

func followedSource(t *testing.T) upstream.Source {
	config, err := anypb.New(wrapperspb.String("synthetic"))
	if err != nil {
		t.Fatal(err)
	}
	return upstream.Source{Path: ".", Kind: "git", Config: config, PollInterval: 10 * time.Second}
}

func newWorld(t *testing.T) *followWorld {
	return &followWorld{t: t, now: time.Unix(1_700_000_000, 0), accepted: revA, latest: []string{revA}, advance: func(upstream.Request) (lifecycle.AdvanceResult, error) {
		t.Fatal("unexpected advance")
		return lifecycle.AdvanceResult{}, nil
	}}
}

func TestNoSourcesFileFollowsNothing(t *testing.T) {
	w := newWorld(t)
	if err := w.run(5); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	if len(w.touched) != 0 || w.saves != 0 {
		t.Fatalf("without sources the host touched %v and saved holds %d times", w.touched, w.saves)
	}
}

func TestNewRevisionAdvancesFromTheAcceptedTrailer(t *testing.T) {
	w := newWorld(t)
	w.sources = []upstream.Source{followedSource(t)}
	w.latest = []string{revB}
	w.advance = func(upstream.Request) (lifecycle.AdvanceResult, error) {
		return lifecycle.AdvanceResult{Outcome: lifecycle.AdvanceAccepted, Receipt: broker.Receipt{Commit: "accepted"}}, nil
	}
	_ = w.run(4)
	if len(w.advances) != 1 {
		t.Fatalf("advances = %+v, want one (the accepted revision is idle afterwards)", w.advances)
	}
	if got := w.advances[0]; got.Prev != revA || got.Revision.ID != revB || got.Source.Path != "." {
		t.Fatalf("request = %+v", got)
	}
	if w.saves != 0 {
		t.Fatalf("an accepted advance wrote holds %d times", w.saves)
	}
}

func TestHeldRevisionIsPersistedAndNotRetried(t *testing.T) {
	w := newWorld(t)
	w.sources = []upstream.Source{followedSource(t)}
	w.latest = []string{revB, revB, revB, revC}
	w.advance = func(request upstream.Request) (lifecycle.AdvanceResult, error) {
		if request.Revision.ID == revC {
			return lifecycle.AdvanceResult{Outcome: lifecycle.AdvanceAccepted}, nil
		}
		return lifecycle.AdvanceResult{Outcome: lifecycle.AdvanceHeld, Hold: lifecycle.HoldRollout, Detail: "workload toy not ready"}, nil
	}
	_ = w.run(6)
	if len(w.advances) != 2 || w.advances[0].Revision.ID != revB || w.advances[1].Revision.ID != revC {
		t.Fatalf("advances = %+v, want B once (held) then C", w.advances)
	}
	if w.saves < 2 {
		t.Fatalf("holds saved %d times, want the hold and its supersession", w.saves)
	}
	if len(w.stored) != 0 {
		t.Fatalf("holds after C was accepted = %+v", w.stored)
	}
}

func TestPersistedHoldSuppressesTheHeldRevisionAfterRestart(t *testing.T) {
	w := newWorld(t)
	source := followedSource(t)
	w.sources = []upstream.Source{source}
	w.latest = []string{revB}
	w.stored = []upstream.Hold{{Path: ".", Fingerprint: source.Fingerprint(), Revision: revB, Reason: "rollout: synthetic", At: w.now}}
	_ = w.run(3)
	if len(w.advances) != 0 {
		t.Fatalf("held revision advanced after restart: %+v", w.advances)
	}
	if len(w.stored) != 1 || !strings.HasPrefix(w.stored[0].Reason, "rollout") {
		t.Fatalf("hold lost: %+v", w.stored)
	}
}

func TestFailedOrDeferredAdvanceRetriesWithoutHold(t *testing.T) {
	for name, outcome := range map[string]func() (lifecycle.AdvanceResult, error){
		"error": func() (lifecycle.AdvanceResult, error) {
			return lifecycle.AdvanceResult{}, errors.New("broker HEAD moved")
		},
		"deferred": func() (lifecycle.AdvanceResult, error) {
			return lifecycle.AdvanceResult{Outcome: lifecycle.AdvanceDeferred, Detail: "incident open"}, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			w.sources = []upstream.Source{followedSource(t)}
			w.latest = []string{revB}
			w.advance = func(upstream.Request) (lifecycle.AdvanceResult, error) { return outcome() }
			start := w.now
			_ = w.run(3)
			if len(w.advances) < 2 || w.saves != 0 {
				t.Fatalf("advances = %d, holds saved = %d; want a retry each poll and no hold", len(w.advances), w.saves)
			}
			if w.now.Sub(start) < 10*time.Second {
				t.Fatalf("retried after %s, want at least one poll interval", w.now.Sub(start))
			}
		})
	}
}

// A shape-change advance is accepted but the designer's refresh fails: the
// follower's next poll sees upstream equal to the accepted revision (idle,
// the hold clears), yet the obligation stays open, is persisted with its cause
// and owner, is retried only after the backoff, and settles on success.
func TestFailedDesignerRefreshStaysOwedUntilItSucceeds(t *testing.T) {
	w := newWorld(t)
	w.sources = []upstream.Source{followedSource(t)}
	w.latest = []string{revB}
	w.advance = func(upstream.Request) (lifecycle.AdvanceResult, error) {
		w.accepted = revB
		return lifecycle.AdvanceResult{
			Outcome: lifecycle.AdvanceHeld, Hold: lifecycle.HoldDesigner, Detail: "refresh failed", Upstream: revB,
			Cause: "coverage", Owner: "designer", Receipt: broker.Receipt{Commit: "accepted1"},
			Owed: &lifecycle.ShapeChange{Upstream: revB, Accepted: "accepted1"},
		}, nil
	}
	start := w.now
	calls := 0
	w.refresh = func(o upstream.Obligation) error {
		calls++
		if o.Accepted != "accepted1" || o.Kind != upstream.ObligationDesignerRefresh || o.Owner != "designer" || o.Cause != "coverage" {
			t.Errorf("obligation = %+v", o)
		}
		if calls == 1 {
			return errors.New("judge unavailable")
		}
		return nil
	}
	_ = w.run(12)
	if len(w.advances) != 1 {
		t.Fatalf("advances = %d, want one", len(w.advances))
	}
	if calls != 2 {
		t.Fatalf("refresh attempts = %d, want a failure then a success", calls)
	}
	if first := w.refreshes[0].Sub(start); first < upstream.ObligationBackoffBase {
		t.Fatalf("first retry after %s, want at least the backoff %s", first, upstream.ObligationBackoffBase)
	}
	if gap := w.refreshes[1].Sub(w.refreshes[0]); gap < 2*upstream.ObligationBackoffBase {
		t.Fatalf("second retry after %s, want a doubled backoff", gap)
	}
	if len(w.owed) != 0 || len(w.stored) != 0 {
		t.Fatalf("after success obligations = %+v, holds = %+v; want none", w.owed, w.stored)
	}
}

// Each obligation retry is a full judge run: a persistent failure is retried
// only after its growing backoff, only MaxObligationAttempts times, and then
// the obligation is a typed hold (owner designer) that no later poll retries.
func TestObligationRetriesAreBoundedThenHeld(t *testing.T) {
	w := newWorld(t)
	w.sources = []upstream.Source{followedSource(t)}
	w.latest = []string{revB}
	w.accepted = revB
	w.owed = []upstream.Obligation{{Path: ".", Kind: upstream.ObligationDesignerRefresh, Accepted: "accepted1", Cause: "coverage", Owner: "designer", Reason: "judge unavailable", Attempts: 1, Since: w.now.Add(-time.Hour)}}
	w.refresh = func(upstream.Obligation) error { return errors.New("judge unavailable") }
	_ = w.run(60)
	// The obligation starts at attempt 1 (the advance's own refresh), so the
	// budget leaves MaxObligationAttempts-1 judge runs, never one per poll.
	if len(w.refreshes) != upstream.MaxObligationAttempts-1 {
		t.Fatalf("judge runs = %d, want %d", len(w.refreshes), upstream.MaxObligationAttempts-1)
	}
	for i := 1; i < len(w.refreshes); i++ {
		if gap := w.refreshes[i].Sub(w.refreshes[i-1]); gap < upstream.ObligationBackoffBase {
			t.Fatalf("judge runs %d and %d only %s apart: a full judge ran back to back", i-1, i, gap)
		}
	}
	if len(w.owed) != 1 || w.owed[0].Held != upstream.ObligationAttemptsExhausted || w.owed[0].HeldOwner != "designer" {
		t.Fatalf("obligation = %+v, want held as attempts_exhausted owned by the designer", w.owed)
	}
}

// A refresh that ended on agent_not_converging is held on its first retry: the
// same agent repeats the same rejection, so no further judge run is spent.
func TestAgentNotConvergingRefreshHoldsTheObligation(t *testing.T) {
	w := newWorld(t)
	w.sources = []upstream.Source{followedSource(t)}
	w.latest = []string{revB}
	w.accepted = revB
	w.owed = []upstream.Obligation{{Path: ".", Kind: upstream.ObligationDesignerRefresh, Accepted: "accepted1", Cause: "coverage", Owner: "designer", Reason: "judge unavailable", Attempts: 1, Since: w.now.Add(-time.Hour)}}
	w.refresh = func(upstream.Obligation) error {
		return fmt.Errorf("health unverified: %w", lifecycle.NewAgentNotConvergingError(operability.OwnerDesigner, "accepted1", []string{"memory.invalid@.sdo/x"}, false))
	}
	_ = w.run(40)
	if len(w.refreshes) != 1 {
		t.Fatalf("judge runs = %d, want exactly the one that ended not converging", len(w.refreshes))
	}
	if len(w.owed) != 1 || w.owed[0].Held != string(operability.CauseAgentNotConverging) || w.owed[0].HeldOwner != "designer" {
		t.Fatalf("obligation = %+v, want held by agent_not_converging, owner designer", w.owed)
	}
}

// One undecodable obligation record is held by itself: following starts, polls
// and advances as if it were absent.
func TestMalformedObligationRecordDoesNotStopFollowing(t *testing.T) {
	w := newWorld(t)
	w.sources = []upstream.Source{followedSource(t)}
	w.latest = []string{revB}
	w.quarantined = []upstream.Quarantined{{Key: "obligation-0123456789abcdef", Reason: "key does not belong to its record"}}
	w.advance = func(upstream.Request) (lifecycle.AdvanceResult, error) {
		return lifecycle.AdvanceResult{Outcome: lifecycle.AdvanceAccepted, Receipt: broker.Receipt{Commit: "accepted1"}}, nil
	}
	_ = w.run(3)
	if len(w.advances) != 1 {
		t.Fatalf("advances = %d, want following to proceed past the quarantined record", len(w.advances))
	}
}

func TestOwedRefreshIsVisibleWhileOpenAndSurvivesRestart(t *testing.T) {
	w := newWorld(t)
	w.sources = []upstream.Source{followedSource(t)}
	w.latest = []string{revB}
	w.accepted = revB // the advance was accepted before the restart
	w.owed = []upstream.Obligation{{Path: ".", Kind: upstream.ObligationDesignerRefresh, Accepted: "accepted1", Cause: "coverage", Owner: "designer", Reason: "judge unavailable", Attempts: 3, Since: w.now.Add(-time.Hour)}}
	w.refresh = func(o upstream.Obligation) error { return errors.New("still down") }
	start := w.now
	_ = w.run(2)
	if len(w.refreshes) == 0 || !w.refreshes[0].Equal(start) {
		t.Fatalf("restart did not retry at once: %v", w.refreshes)
	}
	if len(w.owed) != 1 || w.owed[0].Attempts < 4 || w.owed[0].Reason != "still down" || w.owed[0].Owner != "designer" {
		t.Fatalf("open obligation = %+v", w.owed)
	}
}

func TestNoAcceptedPlanStopsFollowing(t *testing.T) {
	w := newWorld(t)
	w.sources = []upstream.Source{followedSource(t)}
	w.latest = []string{revB}
	w.advance = func(upstream.Request) (lifecycle.AdvanceResult, error) {
		return lifecycle.AdvanceResult{}, lifecycle.ErrNoAcceptedPlan
	}
	if err := w.run(10); err != nil {
		t.Fatalf("Run = %v, want a clean stop", err)
	}
	if len(w.advances) != 1 || w.sleeps != 0 {
		t.Fatalf("advances = %d, sleeps = %d; want one attempt then stop", len(w.advances), w.sleeps)
	}
}
