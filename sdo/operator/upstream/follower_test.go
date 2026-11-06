package upstream_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"sdo.dev/operator/upstream"
)

var t0 = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

func src(t *testing.T, path, repo string, interval time.Duration) upstream.Source {
	t.Helper()
	cfg, err := anypb.New(wrapperspb.String(repo))
	if err != nil {
		t.Fatal(err)
	}
	return upstream.Source{Path: path, Kind: "git", Config: cfg, PollInterval: interval}
}

func newFollower(t *testing.T, sources ...upstream.Source) *upstream.Follower {
	t.Helper()
	f, err := upstream.NewFollower(upstream.FollowerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Sync(t0, sources); err != nil {
		t.Fatal(err)
	}
	return f
}

func rev(id string) upstream.Revision { return upstream.Revision{ID: id, Ref: "main"} }

func TestObserveDecisions(t *testing.T) {
	s := src(t, ".", "r", time.Minute)
	f := newFollower(t, s)

	d := f.Observe(t0, ".", "a", rev("a"), nil)
	if d.Kind != upstream.Idle {
		t.Fatalf("up to date: %v", d.Kind)
	}
	d = f.Observe(t0, ".", "a", rev("b"), nil)
	if d.Kind != upstream.Advance || d.Request.Revision.ID != "b" || d.Request.Prev != "a" || d.Request.Source.Path != "." {
		t.Fatalf("new revision: %+v", d)
	}
	if d := f.Observe(t0, ".", "a", rev("c"), nil); d.Kind != upstream.Busy {
		t.Fatalf("in flight: %v", d.Kind)
	}
}

func TestObserveUnusableObservations(t *testing.T) {
	f := newFollower(t, src(t, ".", "r", time.Minute))
	for name, c := range map[string]struct {
		accepted string
		latest   upstream.Revision
		err      error
		want     upstream.DecisionKind
	}{
		"unavailable":     {"a", upstream.Revision{}, fmt.Errorf("x: %w", upstream.ErrUnavailable), upstream.Unavailable},
		"permanent":       {"a", upstream.Revision{}, errors.New("no such ref"), upstream.Failed},
		"empty revision":  {"a", upstream.Revision{}, nil, upstream.Failed},
		"no accepted rev": {"", rev("b"), nil, upstream.Failed},
	} {
		d := f.Observe(t0, ".", c.accepted, c.latest, c.err)
		if d.Kind != c.want {
			t.Errorf("%s: kind %v, want %v", name, d.Kind, c.want)
		}
		if d.Err == nil {
			t.Errorf("%s: no error carried", name)
		}
	}
	if d := f.Observe(t0, "nope", "a", rev("b"), nil); d.Kind != upstream.Failed {
		t.Errorf("unknown source: %v", d.Kind)
	}
}

func TestBurstCoalescesToNewest(t *testing.T) {
	f := newFollower(t, src(t, ".", "r", time.Minute))
	first := f.Observe(t0, ".", "a", rev("b"), nil)
	if first.Kind != upstream.Advance {
		t.Fatal(first.Kind)
	}
	// c and d land while b is advancing: nothing is dispatched, nothing due.
	if f.Observe(t0, ".", "a", rev("c"), nil).Kind != upstream.Busy || f.Observe(t0, ".", "a", rev("d"), nil).Kind != upstream.Busy {
		t.Fatal("observations during an advance must be Busy")
	}
	if due := f.Due(t0.Add(time.Hour)); len(due) != 0 {
		t.Fatalf("source due while in flight: %v", due)
	}
	if _, err := f.Finish(t0.Add(time.Second), ".", upstream.Outcome{Kind: upstream.Accepted}); err != nil {
		t.Fatal(err)
	}
	// Re-polled immediately; only the newest (d) is advanced, c never is.
	now := t0.Add(time.Second)
	if due := f.Due(now); len(due) != 1 {
		t.Fatalf("not re-polled after accept: %v", due)
	}
	d := f.Observe(now, ".", "b", rev("d"), nil)
	if d.Kind != upstream.Advance || d.Request.Revision.ID != "d" || d.Request.Prev != "b" {
		t.Fatalf("%+v", d)
	}
}

func TestHoldSuppressesUntilNewerRevision(t *testing.T) {
	f := newFollower(t, src(t, ".", "r", time.Minute))
	f.Observe(t0, ".", "a", rev("bad"), nil)
	changed, err := f.Finish(t0, ".", upstream.Outcome{Kind: upstream.Refused, Reason: "E2 render fails"})
	if err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	for i := 1; i <= 3; i++ {
		d := f.Observe(t0.Add(time.Duration(i)*time.Minute), ".", "a", rev("bad"), nil)
		if d.Kind != upstream.Held {
			t.Fatalf("poll %d: %v", i, d.Kind)
		}
	}
	d := f.Observe(t0.Add(5*time.Minute), ".", "a", rev("fix"), nil)
	if d.Kind != upstream.Advance || d.Request.Revision.ID != "fix" {
		t.Fatalf("newer revision must supersede the hold: %+v", d)
	}
	changed, _ = f.Finish(t0.Add(5*time.Minute), ".", upstream.Outcome{Kind: upstream.Accepted})
	if !changed || len(f.Holds()) != 0 {
		t.Fatalf("accept must clear the hold: changed=%v holds=%v", changed, f.Holds())
	}
}

func TestHoldClearedWhenUpstreamReturnsToAccepted(t *testing.T) {
	f := newFollower(t, src(t, ".", "r", time.Minute))
	f.Observe(t0, ".", "a", rev("bad"), nil)
	f.Finish(t0, ".", upstream.Outcome{Kind: upstream.Refused, Reason: "x"})
	d := f.Observe(t0.Add(time.Minute), ".", "a", rev("a"), nil)
	if d.Kind != upstream.Idle || !d.HoldsChanged || len(f.Holds()) != 0 {
		t.Fatalf("%+v holds=%v", d, f.Holds())
	}
}

func TestRetryKeepsRevisionUnheld(t *testing.T) {
	f := newFollower(t, src(t, ".", "r", time.Minute))
	f.Observe(t0, ".", "a", rev("b"), nil)
	if changed, _ := f.Finish(t0, ".", upstream.Outcome{Kind: upstream.Retry}); changed {
		t.Fatal("retry changes no hold")
	}
	if len(f.Due(t0)) != 0 || len(f.Due(t0.Add(time.Minute))) != 1 {
		t.Fatal("retry waits one poll interval")
	}
	if d := f.Observe(t0.Add(time.Minute), ".", "a", rev("b"), nil); d.Kind != upstream.Advance {
		t.Fatalf("retried revision must be advanced again: %v", d.Kind)
	}
}

func TestFinishErrors(t *testing.T) {
	f := newFollower(t, src(t, ".", "r", time.Minute))
	if _, err := f.Finish(t0, ".", upstream.Outcome{Kind: upstream.Accepted}); err == nil {
		t.Error("finish without an advance in flight")
	}
	f.Observe(t0, ".", "a", rev("b"), nil)
	if _, err := f.Finish(t0, ".", upstream.Outcome{Kind: upstream.Refused}); err == nil {
		t.Error("refused without reason")
	}
	if _, err := f.Finish(t0, ".", upstream.Outcome{Kind: upstream.OutcomeKind(99)}); err == nil {
		t.Error("unknown outcome kind")
	}
	// The failed finishes left the advance in flight.
	if f.Observe(t0, ".", "a", rev("c"), nil).Kind != upstream.Busy {
		t.Error("a rejected Finish must not release the advance")
	}
}

func TestScheduleAndJitter(t *testing.T) {
	r := 0.5
	f, err := upstream.NewFollower(upstream.FollowerConfig{Jitter: 0.2, Rand: func() float64 { return r }})
	if err != nil {
		t.Fatal(err)
	}
	f.Sync(t0, []upstream.Source{src(t, ".", "r", 100*time.Second), src(t, "sub", "r2", 0)})
	if got := f.Due(t0); len(got) != 2 || got[0].Path != "." || got[1].Path != "sub" {
		t.Fatalf("new sources are due at once, sorted: %v", got)
	}
	f.Observe(t0, ".", "a", rev("a"), nil)
	f.Observe(t0, "sub", "a", rev("a"), nil)
	// Default interval 30s + 3s jitter for "sub"; 100s + 10s for ".".
	next, ok := f.NextPoll()
	if !ok || next != t0.Add(33*time.Second) {
		t.Fatalf("next poll %v ok=%v", next, ok)
	}
	if got := f.Due(next); len(got) != 1 || got[0].Path != "sub" {
		t.Fatalf("%v", got)
	}
	if got := f.Due(t0.Add(110 * time.Second)); len(got) != 2 {
		t.Fatalf("%v", got)
	}
	if got := f.Due(t0.Add(109 * time.Second)); len(got) != 1 {
		t.Fatalf("%v", got)
	}
}

func TestNewFollowerValidates(t *testing.T) {
	for _, c := range []upstream.FollowerConfig{{Jitter: -0.1}, {Jitter: 1}, {Jitter: 0.1}} {
		if _, err := upstream.NewFollower(c); err == nil {
			t.Errorf("%+v accepted", c)
		}
	}
}

func TestSyncValidatesAndReconciles(t *testing.T) {
	f := newFollower(t)
	s := src(t, ".", "r", time.Minute)
	for name, bad := range map[string][]upstream.Source{
		"dup":      {s, s},
		"no path":  {{Kind: "git"}},
		"no kind":  {{Path: "."}},
		"negative": {src(t, ".", "r", -time.Second)},
	} {
		if _, err := f.Sync(t0, bad); err == nil {
			t.Errorf("%s accepted", name)
		}
	}

	f = newFollower(t, s)
	f.Observe(t0, ".", "a", rev("a"), nil) // next poll at t0+1m
	// Same upstream, new interval: schedule kept.
	f.Sync(t0.Add(time.Second), []upstream.Source{src(t, ".", "r", 2*time.Minute)})
	if len(f.Due(t0.Add(time.Second))) != 0 {
		t.Fatal("unchanged upstream must keep its schedule")
	}
	// Different repository: polled at once.
	f.Sync(t0.Add(2*time.Second), []upstream.Source{src(t, ".", "other", time.Minute)})
	if len(f.Due(t0.Add(2*time.Second))) != 1 {
		t.Fatal("changed upstream must be polled at once")
	}
	f.Sync(t0.Add(3*time.Second), nil)
	if _, ok := f.NextPoll(); ok {
		t.Fatal("removed source still scheduled")
	}
}

func TestSyncDropsHoldsOfRemovedOrChangedSources(t *testing.T) {
	s := src(t, ".", "r", time.Minute)
	f := newFollower(t, s)
	f.Observe(t0, ".", "a", rev("bad"), nil)
	f.Finish(t0, ".", upstream.Outcome{Kind: upstream.Refused, Reason: "x"})
	enc, err := upstream.EncodeHolds(f.Holds())
	if err != nil {
		t.Fatal(err)
	}

	// Restart: holds survive for the same upstream.
	holds, err := upstream.DecodeHolds(enc)
	if err != nil {
		t.Fatal(err)
	}
	g, _ := upstream.NewFollower(upstream.FollowerConfig{})
	g.LoadHolds(holds)
	if changed, _ := g.Sync(t0, []upstream.Source{s}); changed {
		t.Fatal("hold for an unchanged upstream must survive")
	}
	if d := g.Observe(t0, ".", "a", rev("bad"), nil); d.Kind != upstream.Held {
		t.Fatalf("persisted hold ignored: %v", d.Kind)
	}

	// Same path, different repository: the hold says nothing about it.
	g, _ = upstream.NewFollower(upstream.FollowerConfig{})
	g.LoadHolds(holds)
	if changed, _ := g.Sync(t0, []upstream.Source{src(t, ".", "other", time.Minute)}); !changed || len(g.Holds()) != 0 {
		t.Fatal("hold for a changed upstream must be dropped")
	}
	// Source removed.
	g, _ = upstream.NewFollower(upstream.FollowerConfig{})
	g.LoadHolds(holds)
	if changed, _ := g.Sync(t0, nil); !changed || len(g.Holds()) != 0 {
		t.Fatal("hold for a removed source must be dropped")
	}
}

func TestHoldsCodecRoundTrip(t *testing.T) {
	in := []upstream.Hold{
		{Path: ".", Fingerprint: "ab", Revision: "r1", Reason: "shape change: designer route not in MVP", At: t0},
		{Path: "services/api with space", Fingerprint: "cd", Revision: "r2", Reason: "merge conflict: a.go", At: t0.Add(time.Hour + 5*time.Nanosecond)},
	}
	data, err := upstream.EncodeHolds(in)
	if err != nil {
		t.Fatal(err)
	}
	for k := range data {
		if len(k) > 253 || strings.ContainsAny(k, "/ ") {
			t.Fatalf("invalid ConfigMap key %q", k)
		}
	}
	out, err := upstream.DecodeHolds(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("%v", out)
	}
	byPath := map[string]upstream.Hold{}
	for _, h := range out {
		byPath[h.Path] = h
	}
	for _, want := range in {
		got := byPath[want.Path]
		if got.Fingerprint != want.Fingerprint || got.Revision != want.Revision || got.Reason != want.Reason || !got.At.Equal(want.At) {
			t.Errorf("round trip: %+v != %+v", got, want)
		}
	}
	if empty, err := upstream.DecodeHolds(nil); err != nil || len(empty) != 0 {
		t.Errorf("empty data: %v %v", empty, err)
	}
}

func TestHoldsCodecRejectsMalformed(t *testing.T) {
	good, err := upstream.EncodeHolds([]upstream.Hold{{Path: ".", Fingerprint: "ab", Revision: "r", Reason: "x", At: t0}})
	if err != nil {
		t.Fatal(err)
	}
	var key, val string
	for k, v := range good {
		key, val = k, v
	}
	cases := map[string]map[string]string{
		"foreign key":    {"other": val},
		"not json":       {key: "nope"},
		"unknown field":  {key: strings.Replace(val, `"reason"`, `"extra":1,"reason"`, 1)},
		"trailing data":  {key: val + `{}`},
		"missing reason": {key: strings.Replace(val, `"reason":"x"`, `"reason":""`, 1)},
		"bad time":       {key: strings.Replace(val, `"at":"`, `"at":"yesterday`, 1)},
		"wrong key":      {"hold-0000000000000000": val},
	}
	for name, data := range cases {
		if _, err := upstream.DecodeHolds(data); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	for name, h := range map[string]upstream.Hold{
		"no reason": {Path: ".", Revision: "r", At: t0},
		"no time":   {Path: ".", Revision: "r", Reason: "x"},
	} {
		if _, err := upstream.EncodeHolds([]upstream.Hold{h}); err == nil {
			t.Errorf("encode %s accepted", name)
		}
	}
	if _, err := upstream.EncodeHolds([]upstream.Hold{
		{Path: ".", Revision: "r", Reason: "x", At: t0}, {Path: ".", Revision: "s", Reason: "x", At: t0},
	}); err == nil {
		t.Error("duplicate path accepted")
	}
}

func TestObligationOutlivesObservationOfTheAcceptedRevision(t *testing.T) {
	f := newFollower(t, src(t, ".", "r", time.Minute))
	now := time.Unix(1_700_000_000, 0)
	if _, err := f.Owe(now, upstream.Obligation{Path: ".", Kind: upstream.ObligationDesignerRefresh, Accepted: "c1", Cause: "coverage", Owner: "designer", Reason: "judge down"}); err != nil {
		t.Fatal(err)
	}
	// Upstream equals the accepted revision: a hold would clear here.
	f.Observe(now, ".", "r1", upstream.Revision{ID: "r1"}, nil)
	if got := f.Obligations(); len(got) != 1 || got[0].Accepted != "c1" {
		t.Fatalf("obligations after observing the accepted revision = %+v", got)
	}
	if due := f.DueObligations(now); len(due) != 0 {
		t.Fatalf("due before the backoff: %+v", due)
	}
	if due := f.DueObligations(now.Add(upstream.ObligationBackoffBase)); len(due) != 1 {
		t.Fatalf("not due after the backoff")
	}
	o := f.Obligations()[0]
	f.Retried(now, o, "again")
	for range 20 {
		o = f.Obligations()[0]
		f.Retried(now, o, "again")
	}
	if wait := f.Obligations()[0].Next.Sub(now); wait != upstream.ObligationBackoffMax {
		t.Fatalf("backoff = %s, want the cap %s", wait, upstream.ObligationBackoffMax)
	}
	if !f.Settle(f.Obligations()[0]) || len(f.Obligations()) != 0 {
		t.Fatal("settle did not close the obligation")
	}
}

func TestObligationsCodecRoundTrip(t *testing.T) {
	in := []upstream.Obligation{{Path: ".", Kind: upstream.ObligationDesignerRefresh, Accepted: "c1", Upstream: "r1", Cause: "coverage", Owner: "designer", Reason: "x", Attempts: 2, Since: time.Unix(1_700_000_000, 0).UTC()}}
	data, err := upstream.EncodeObligations(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := upstream.DecodeObligations(data)
	if err != nil || len(out) != 1 || out[0].Accepted != "c1" || out[0].Attempts != 2 || !out[0].Since.Equal(in[0].Since) {
		t.Fatalf("round trip = %+v, %v", out, err)
	}
	if _, err := upstream.DecodeObligations(map[string]string{"junk": "{}"}); err == nil {
		t.Fatal("foreign key accepted")
	}
}
