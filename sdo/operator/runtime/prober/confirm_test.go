package prober

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

// faultyApp is a synthetic application whose answers depend on the time since
// its start.
type faultyApp struct {
	mu sync.Mutex
	// clock is the synctest bubble's.
	clock   traffic.Clock
	start   time.Time
	sent    map[string][]time.Duration
	running map[string]int
	overlap bool
	answer  func(path string, at time.Duration) (status int, hang time.Duration, err error)
}

func (a *faultyApp) Do(request *http.Request) (*http.Response, error) {
	at := a.clock.Now().Sub(a.start)
	user := request.URL.Query().Get("user")
	a.mu.Lock()
	a.sent[request.URL.Path] = append(a.sent[request.URL.Path], at)
	a.running[user]++
	a.overlap = a.overlap || user != "" && a.running[user] > 1
	a.mu.Unlock()
	defer func() { a.mu.Lock(); a.running[user]--; a.mu.Unlock() }()
	status, hang, err := a.answer(request.URL.Path, at)
	select {
	case <-request.Context().Done():
		return nil, request.Context().Err()
	case <-a.clock.After(hang + 50*time.Millisecond):
	}
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("body")), Request: request}, nil
}

type openView struct {
	sdk.DetectionContext
	windows map[string]traffic.Window
}

func (openView) Namespace() string                           { return "app" }
func (v openView) TrafficWindows() map[string]traffic.Window { return v.windows }
func (v openView) TrafficWindow(name string) (traffic.Window, bool) {
	w, ok := v.windows[name]
	return w, ok
}

// noLock is a setup lock for one process.
type noLock struct{}

func (noLock) Lock(context.Context, string) (func(), error) { return func() {}, nil }

// onset is when a fault ends the probe's healthy time: between two scheduled
// samples, after the window is ready.
const onset = 60*time.Second - 300*time.Millisecond

// runFaulty probes ids (two of them writes of one synthetic user when writes
// is set) for total and returns when the detector first reported, zero if it
// never did, and the application.
func runFaulty(t *testing.T, ids []string, writes bool, total time.Duration, answer func(string, time.Duration) (int, time.Duration, error)) (paged time.Duration, app *faultyApp) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		app = &faultyApp{clock: traffic.RealClock, start: traffic.RealClock.Now(), sent: map[string][]time.Duration{}, running: map[string]int{}, answer: answer}
		workload := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "probe", Purpose: traffic.PurposeHealthProbe,
			Arrival: traffic.ArrivalUniform, RatePerSecond: float64(len(ids)) / 10}
		var catalog traffic.Catalog
		for _, id := range ids {
			step := traffic.GET("/"+id, nil)
			scenario := traffic.Scenario{ID: id, Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead}
			if writes {
				step = traffic.POST("/"+id, traffic.Params{"user": traffic.Const("sdoprobe-one")})
				scenario.SideEffect, scenario.Marker = traffic.SideEffectIdempotentWrite, "sdoprobe"
				scenario.Users = []traffic.ProbeUser{{Name: "sdoprobe-one", As: "password"}}
			}
			scenario.Steps = []traffic.Step{{Name: "send", Endpoint: step}}
			catalog = append(catalog, scenario)
			workload.Scenarios = append(workload.Scenarios, traffic.WorkloadScenario{ID: id})
		}
		p, err := New(Config{Namespace: "app", Catalog: catalog, Workloads: []traffic.Workload{workload}, Client: app,
			BaseURL: func(traffic.Target) string { return "http://web:80" }, SetupLock: noLock{}, Clock: app.clock, Passwords: onePassword{}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() { defer close(done); p.Run(ctx) }()
		detector := traffic.NewDetector(sdk.DetectorSpec{ID: "traffic-probe", Class: sdk.DetectorClassHealth}, "probe")
		for app.clock.Now().Sub(app.start) < total {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
			findings, err := detector.Detect(ctx, openView{windows: p.Windows()})
			if err != nil {
				t.Fatal(err)
			}
			if len(findings) > 0 && paged == 0 {
				paged = app.clock.Now().Sub(app.start)
			}
		}
		cancel()
		<-done
	})
	return paged, app
}

type onePassword struct{}

func (onePassword) Password(context.Context, string) (string, error) {
	return "a-synthetic-password", nil
}

func failingFrom(status int, hang time.Duration, err error) func(string, time.Duration) (int, time.Duration, error) {
	return func(_ string, at time.Duration) (int, time.Duration, error) {
		if at < onset {
			return 200, 0, nil
		}
		return status, hang, err
	}
}

// A failure is sampled again 1 s after it and reported on the second failure;
// one the application never answered must also cover 4 s, so a stall of a few
// seconds reports nothing.
func TestAFailingScenarioIsSampledAgainAndReported(t *testing.T) {
	refused := errors.New("connection refused")
	stall := func(_ string, at time.Duration) (int, time.Duration, error) {
		if at >= onset && at < onset+3*time.Second {
			return 0, time.Second, refused
		}
		return 200, 0, nil
	}
	blip := func(_ string, at time.Duration) (int, time.Duration, error) {
		if at >= onset && at < onset+900*time.Millisecond {
			return 500, 0, nil
		}
		return 200, 0, nil
	}
	five := []string{"a", "b", "c", "d", "e"}
	for _, tc := range []struct {
		name     string
		ids      []string
		answer   func(string, time.Duration) (int, time.Duration, error)
		min, max time.Duration // after the onset; zero means never
	}{
		{"wrong status, one scenario", []string{"a"}, failingFrom(403, 0, nil), time.Second, 2 * time.Second},
		{"5xx, five scenarios", five, failingFrom(500, 0, nil), time.Second, 2 * time.Second},
		{"refused, one scenario", []string{"a"}, failingFrom(0, 0, refused), 4 * time.Second, 5 * time.Second},
		{"hang until the timeout, one scenario", []string{"a"}, failingFrom(0, time.Minute, nil), 4 * time.Second, 6 * time.Second},
		{"hang until the timeout, five scenarios", five, failingFrom(0, time.Minute, nil), 4 * time.Second, 5 * time.Second},
		{"healthy", five, failingFrom(200, 0, nil), 0, 0},
		{"one flapping failure", []string{"a"}, blip, 0, 0},
		{"3 s stall, one scenario", []string{"a"}, stall, 0, 0},
		{"3 s stall, five scenarios", five, stall, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paged, _ := runFaulty(t, tc.ids, false, onset+25*time.Second, tc.answer)
			if tc.max == 0 && paged != 0 {
				t.Fatalf("reported %s after the onset", paged-onset)
			}
			if after := paged - onset; tc.max > 0 && (paged == 0 || after < tc.min || after > tc.max) {
				t.Fatalf("reported %s after the onset, want %s to %s", after, tc.min, tc.max)
			}
		})
	}
}

// A healthy probe sends only its scheduled samples. When every scenario of a
// large probe starts to fail, each is sampled at most twice more, once; a
// scenario that flaps at every scheduled sample gets one more sample each.
func TestConfirmingSamplesAreBounded(t *testing.T) {
	ids := strings.Split("a b c d e f g h i j k l m", " ")
	total := onset + 60*time.Second
	count := func(app *faultyApp) (after int) {
		for _, sent := range app.sent {
			for _, at := range sent {
				if at >= onset {
					after++
				}
			}
		}
		return after
	}
	_, healthy := runFaulty(t, ids, false, total, failingFrom(200, 0, nil))
	if scheduled := count(healthy); scheduled < 77 || scheduled > 80 {
		t.Fatalf("a healthy probe sent %d samples in 60 s, want its 78 scheduled", scheduled)
	}
	_, wrong := runFaulty(t, ids, false, total, failingFrom(500, 0, nil))
	if extra := count(wrong) - count(healthy); extra != len(ids) {
		t.Fatalf("%d extra samples for wrong answers, want %d", extra, len(ids))
	}
	_, refused := runFaulty(t, ids, false, total, failingFrom(0, 0, errors.New("connection refused")))
	if extra := count(refused) - count(healthy); extra != 2*len(ids) {
		t.Fatalf("%d extra samples for refused connections, want %d", extra, 2*len(ids))
	}
	failed := map[string]bool{}
	_, flapping := runFaulty(t, ids[:1], false, total, func(_ string, at time.Duration) (int, time.Duration, error) {
		if scheduled := at%(10*time.Second) < time.Second; at >= onset && scheduled {
			failed[at.String()] = true
			return 500, 0, nil
		}
		return 200, 0, nil
	})
	if sent := count(flapping); len(failed) == 0 || sent != 2*len(failed) {
		t.Fatalf("%d samples for %d flapping scheduled ones, want one more each", sent, len(failed))
	}
}

// Two failing scenarios of one synthetic user are confirmed without their
// samples ever overlapping.
func TestConfirmingSamplesOfOneUserDoNotOverlap(t *testing.T) {
	paged, app := runFaulty(t, []string{"a", "b"}, true, onset+30*time.Second, failingFrom(0, time.Minute, nil))
	if app.overlap {
		t.Fatal("two samples of one synthetic user overlapped")
	}
	if paged == 0 || paged-onset > 8*time.Second {
		t.Fatalf("reported %s after the onset, want within 8s", paged-onset)
	}
}

// A 429 is the probe's load: it is neither a failure nor sampled again.
func TestARateLimitedSampleIsNotSampledAgain(t *testing.T) {
	paged, app := runFaulty(t, []string{"a"}, false, onset+9*time.Second, failingFrom(429, 0, nil))
	var after int
	for _, at := range app.sent["/a"] {
		if at >= onset {
			after++
		}
	}
	if paged != 0 || after != 1 {
		t.Fatalf("reported at %s after %d requests, want none after one", paged, after)
	}
}
