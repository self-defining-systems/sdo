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
	// inFlight is how many requests are unanswered, and most the highest it was.
	inFlight, most int
	answer         func(path string, at time.Duration) (status int, hang time.Duration, err error)
}

func (a *faultyApp) Do(request *http.Request) (*http.Response, error) {
	at := a.clock.Now().Sub(a.start)
	user := request.URL.Query().Get("user")
	a.mu.Lock()
	a.sent[request.URL.Path] = append(a.sent[request.URL.Path], at)
	a.running[user]++
	a.inFlight++
	a.most = max(a.most, a.inFlight)
	a.overlap = a.overlap || user != "" && a.running[user] > 1
	a.mu.Unlock()
	defer func() { a.mu.Lock(); a.running[user]--; a.inFlight--; a.mu.Unlock() }()
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

// faultyRun is what probing a faulty application observed.
type faultyRun struct {
	app *faultyApp
	// paged is when the detector first reported, and slo when the scheduled
	// samples alone violated the SLO for the hold; zero for never.
	paged, slo time.Duration
	confirming int
}

// runFaulty probes ids for total; change may adjust the workload and scenarios.
func runFaulty(t *testing.T, ids []string, total time.Duration, change func(*traffic.Workload, *traffic.Scenario), answer func(string, time.Duration) (int, time.Duration, error)) (run faultyRun) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		app := &faultyApp{clock: traffic.RealClock, start: traffic.RealClock.Now(), sent: map[string][]time.Duration{}, running: map[string]int{}, answer: answer}
		run.app = app
		workload := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "probe", Purpose: traffic.PurposeHealthProbe,
			Arrival: traffic.ArrivalUniform, RatePerSecond: float64(len(ids)) / 10}
		var catalog traffic.Catalog
		for _, id := range ids {
			scenario := traffic.Scenario{ID: id, Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead,
				Steps: []traffic.Step{{Name: "send", Endpoint: traffic.GET("/"+id, nil)}}}
			if change != nil {
				change(&workload, &scenario)
			}
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
		time.Sleep(onset)
		for app.clock.Now().Sub(app.start) < total {
			time.Sleep(200 * time.Millisecond)
			synctest.Wait()
			at, windows := app.clock.Now().Sub(app.start), p.Windows()
			window := windows["probe"]
			for i := range window.Scenarios {
				window.Scenarios[i].UserTest = true
			}
			for _, verdict := range traffic.Judge(window) {
				if run.slo == 0 && !verdict.Healthy && verdict.FailureSpan >= traffic.DefaultHealthMinDuration {
					run.slo = at
				}
			}
			findings, err := detector.Detect(ctx, openView{windows: windows})
			if err != nil {
				t.Fatal(err)
			}
			if len(findings) > 0 && run.paged == 0 {
				run.paged = at
			}
		}
		for _, observed := range p.Windows()["probe"].Scenarios {
			run.confirming += len(observed.Confirming)
		}
		cancel()
		<-done
	})
	return run
}

// oneUser makes every scenario a write of one synthetic user.
func oneUser(_ *traffic.Workload, scenario *traffic.Scenario) {
	scenario.Steps = []traffic.Step{{Name: "send", Endpoint: traffic.POST("/"+scenario.ID, traffic.Params{"user": traffic.Const("sdoprobe-one")})}}
	scenario.SideEffect, scenario.Marker = traffic.SideEffectIdempotentWrite, "sdoprobe"
	scenario.Users = []traffic.ProbeUser{{Name: "sdoprobe-one", As: "password"}}
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

// A failure is sampled again 1 s after it and reported once it spans 4 s, so a
// restart or a stall of a few seconds reports nothing.
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
		{"wrong status, one scenario", []string{"a"}, failingFrom(403, 0, nil), 4 * time.Second, 5 * time.Second},
		{"5xx, five scenarios", five, failingFrom(500, 0, nil), 4 * time.Second, 5 * time.Second},
		{"refused, one scenario", []string{"a"}, failingFrom(0, 0, refused), 4 * time.Second, 5 * time.Second},
		{"hang until the timeout, one scenario", []string{"a"}, failingFrom(0, time.Minute, nil), 7 * time.Second, 8 * time.Second},
		{"hang until the timeout, five scenarios", five, failingFrom(0, time.Minute, nil), 6 * time.Second, 8 * time.Second},
		{"healthy", five, failingFrom(200, 0, nil), 0, 0},
		{"one flapping failure", []string{"a"}, blip, 0, 0},
		{"3 s stall, one scenario", []string{"a"}, stall, 0, 0},
		{"3 s stall, five scenarios", five, stall, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paged := runFaulty(t, tc.ids, onset+25*time.Second, nil, tc.answer).paged
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
	count := func(run faultyRun) (after int) {
		for _, sent := range run.app.sent {
			for _, at := range sent {
				if at >= onset {
					after++
				}
			}
		}
		return after
	}
	healthy := runFaulty(t, ids, total, nil, failingFrom(200, 0, nil))
	if scheduled := count(healthy); scheduled < 77 || scheduled > 80 {
		t.Fatalf("a healthy probe sent %d samples in 60 s, want its 78 scheduled", scheduled)
	}
	refused := runFaulty(t, ids, total, nil, failingFrom(0, 0, errors.New("connection refused")))
	if extra := count(refused) - count(healthy); extra != 2*len(ids) {
		t.Fatalf("%d extra samples, want %d", extra, 2*len(ids))
	}
	failed := map[string]bool{}
	flapping := runFaulty(t, ids[:1], total, nil, func(_ string, at time.Duration) (int, time.Duration, error) {
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
	run := runFaulty(t, []string{"a", "b"}, onset+30*time.Second, oneUser, failingFrom(0, time.Minute, nil))
	paged, app := run.paged, run.app
	if app.overlap {
		t.Fatal("two samples of one synthetic user overlapped")
	}
	if paged == 0 || paged-onset > 10*time.Second {
		t.Fatalf("reported %s after the onset, want within 10s", paged-onset)
	}
}

// A 429 is the probe's load: it is neither a failure nor sampled again.
func TestARateLimitedSampleIsNotSampledAgain(t *testing.T) {
	run := runFaulty(t, []string{"a"}, onset+9*time.Second, nil, failingFrom(429, 0, nil))
	paged, app := run.paged, run.app
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

// Confirming samples run inside the workload's in-flight slots.
func TestConfirmingSamplesTakeAnInFlightSlot(t *testing.T) {
	ids := strings.Split("a b c d e f g h i j k l m", " ")
	slow := func(workload *traffic.Workload, _ *traffic.Scenario) {
		workload.Timeout, workload.IterationTimeout = traffic.Duration(5*time.Second), traffic.Duration(6*time.Second)
	}
	run := runFaulty(t, ids, onset+40*time.Second, slow, failingFrom(0, time.Minute, nil))
	if run.confirming == 0 || run.app.most != traffic.MaxInFlight {
		t.Fatalf("%d confirming samples, %d requests in flight at once, want the limit of %d reached and kept", run.confirming, run.app.most, traffic.MaxInFlight)
	}
}

// A scenario that fails again while its failure is being confirmed starts no
// second confirmation.
func TestOneConfirmationPerScenarioAtATime(t *testing.T) {
	everySecond := func(workload *traffic.Workload, _ *traffic.Scenario) { workload.RatePerSecond = 1 }
	failing := func(from, to time.Duration, at time.Duration) bool { return at >= onset+from && at < onset+to }
	run := runFaulty(t, []string{"a"}, onset+20*time.Second, everySecond, func(_ string, at time.Duration) (int, time.Duration, error) {
		// Scheduled at 0.3, 1.3, 2.3 and 3.3 s after the onset; confirming at 1.35 and 4.3 s.
		if failing(0, 1500*time.Millisecond, at) || failing(3*time.Second, 3500*time.Millisecond, at) {
			return 500, 0, nil
		}
		return 200, 0, nil
	})
	if run.confirming != 2 {
		t.Fatalf("%d confirming samples, want the one confirmation's 2", run.confirming)
	}
}

// Restarts and stalls of 3 s or less report nothing, as the SLO and hold alone
// report nothing, whichever scenario they hit and in whatever phase. A longer
// outage may be reported, and is never reported later than the SLO does.
func TestShortOutagesReportNothing(t *testing.T) {
	ids := strings.Split("a b c d e f g h i j k l m", " ")
	outages := map[string]func(left time.Duration) (int, time.Duration, error){
		"502":         func(time.Duration) (int, time.Duration, error) { return 502, 0, nil },
		"lost":        func(time.Duration) (int, time.Duration, error) { return 0, time.Minute, nil },
		"slow 504":    func(time.Duration) (int, time.Duration, error) { return 504, 1500 * time.Millisecond, nil },
		"late answer": func(left time.Duration) (int, time.Duration, error) { return 200, left, nil },
	}
	const phases = 3
	for kind, answer := range outages {
		t.Run(kind, func(t *testing.T) {
			for _, count := range []int{1, 5, 13} {
				for _, length := range []time.Duration{1500 * time.Millisecond, 2 * time.Second, 3 * time.Second, 5 * time.Second, 8 * time.Second} {
					paged, slo := 0, 0
					for phase := range phases {
						start := onset + time.Duration(phase)*10*time.Second/phases + 130*time.Millisecond
						run := runFaulty(t, ids[:count], start+length+8*time.Second, nil, func(_ string, at time.Duration) (int, time.Duration, error) {
							if at < start || at >= start+length {
								return 200, 0, nil
							}
							return answer(start + length - at)
						})
						if run.paged != 0 {
							paged++
						}
						if run.slo != 0 {
							slo++
							if run.paged == 0 || run.paged > run.slo {
								t.Errorf("%s, %d scenarios, %s: reported at %s, the SLO alone at %s", kind, count, length, run.paged, run.slo)
							}
						}
					}
					t.Logf("%-11s %2d scenarios %4s outage: reported %d of %d, SLO alone %d", kind, count, length, paged, phases, slo)
					if length <= 3*time.Second && paged+slo > 0 {
						t.Errorf("%s, %d scenarios, %s: reported %d of %d", kind, count, length, paged, phases)
					}
				}
			}
		})
	}
}
