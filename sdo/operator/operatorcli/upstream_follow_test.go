package operatorcli

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/lifecycle"
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
	saves    int
	sleeps   int
	maxSleep int
	cancel   context.CancelFunc
	touched  []string
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
		Holds: (*worldHolds)(w),
		Log:   slog.New(slog.DiscardHandler),
		Now:   func() time.Time { return w.now },
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
