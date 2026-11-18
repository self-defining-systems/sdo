package onboardcheck

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk/metrics"
)

func harmRun(t *testing.T, fail, reqs float64, pausedReqs float64) Report {
	t.Helper()
	return harmRunWith(t, fail, reqs, map[string]float64{requestsKey("web", "/items"): pausedReqs}, 0, 0)
}

// harmRunLatency runs with users sending 600 requests/min; the mean latency
// is pausedLat seconds when the probes are paused and runLat while they run.
func harmRunLatency(t *testing.T, pausedLat, runLat float64) Report {
	t.Helper()
	return harmRunWith(t, 0, 600, map[string]float64{requestsKey("web", "/items"): 600,
		latencyCountKey("web"): 600, latencySumKey("web"): 600 * pausedLat}, 600, runLat)
}

// sampleGate releases the quiet evaluator once SampleHarm's last query, the
// failure counters, has been answered.
type sampleGate struct {
	Querier
	once     sync.Once
	released chan struct{}
}

func (g *sampleGate) Query(ctx context.Context, text string) ([]metrics.Series, error) {
	out, err := g.Querier.Query(ctx, text)
	if text == FailureCounters {
		g.once.Do(func() { close(g.released) })
	}
	return out, err
}

type gatedEvaluator struct {
	Evaluator
	released <-chan struct{}
}

func (g gatedEvaluator) Evaluate(ctx context.Context) (Verdict, error) {
	select {
	case <-g.released:
	case <-ctx.Done():
		return Verdict{}, ctx.Err()
	}
	return g.Evaluator.Evaluate(ctx)
}

func harmRunWith(t *testing.T, fail, reqs float64, pausedRequests map[string]float64, durReqs, durMean float64) Report {
	t.Helper()
	w := newWorld()
	m, err := appmap.Parse(liveMap)
	if err != nil {
		t.Fatal(err)
	}
	m.Entries = []appmap.MetricEntry{{Service: "web", Metric: "http_requests_total", Selector: map[string]string{"job": "web"}, Labels: appmap.EntryLabels{Route: "route", Method: "method", Status: "code"}, Errors: "5.."}}
	m.Operations[0].Entry.Path = "/items"
	m.Operations[1].Entry.Path = "/mail"
	p := &fakeProm{}
	start := w.Now()
	paused := HarmConfig{
		PausedStart: HarmSample{At: start.Add(-2 * time.Minute), Counts: map[string]float64{}},
		PausedEnd:   HarmSample{At: start.Add(-time.Minute), Counts: map[string]float64{"failures|jobs_failed_total{worker=CreatePipelineWorker}": 0.5}},
		MinRunning:  time.Minute,
	}
	for k, v := range pausedRequests {
		paused.PausedEnd.Counts[k] = v
	}
	// While the probes run, the counters follow the clock at the given rates
	// per minute.
	last := start
	p.tick = func() {
		now := w.Now()
		minutes := now.Sub(last).Minutes()
		last = now
		if fail > 0 {
			p.add("jobs_failed_total", map[string]string{"job": "x", "worker": "CreatePipelineWorker", "pod": "p1"}, fail*minutes)
		}
		if reqs > 0 {
			p.add("http_requests_total", map[string]string{"job": "web", "route": "/items", "method": "GET", "code": "200"}, reqs*minutes)
		}
		if durReqs > 0 {
			p.add("http_request_duration_seconds_count", map[string]string{"job": "web", "route": "/items"}, durReqs*minutes)
			p.add("http_request_duration_seconds_sum", map[string]string{"job": "web", "route": "/items"}, durReqs*minutes*durMean)
		}
	}
	// Run samples the running window's start while the quiet goroutine is
	// still advancing the shared fake clock. The sample reads the clock, then
	// queries counters that follow the clock, so a clock move in between
	// pairs counters from a later time with an earlier timestamp (a 5 minute
	// window holding almost no failures). The quiet evaluator therefore
	// holds the clock until that sample has been read.
	gate := &sampleGate{Querier: p, released: make(chan struct{})}
	cfg := Config{Map: m, Commit: "c", Prom: gate, Sender: fakeSender{prom: p, emit: perRoute}, Requests: 1, Poll: time.Second, Wait: w.Wait, Now: w.Now,
		Quiet: &QuietConfig{Eval: gatedEvaluator{Evaluator: w, released: gate.released}, Window: 5 * time.Minute, Interval: time.Minute},
		Harm:  &paused}
	r, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestProbesThatCauseFailuresAreRed(t *testing.T) {
	r := harmRun(t, 3, 0, 0)
	if status(r, ItemHarm, "jobs_failed_total{worker=CreatePipelineWorker}") == Red {
		return
	}
	t.Fatalf("job failures with probes running must be red:\n%s", r.Text())
}

func TestRedHarmNamesTheProbeScenarios(t *testing.T) {
	r := harmRun(t, 3, 0, 0)
	if !strings.Contains(r.Text(), "CreatePipelineWorker") || !strings.Contains(r.Text(), "browse") {
		t.Fatalf("report must name the failing class and the probes:\n%s", r.Text())
	}
}

func TestQuietProbesAreGreen(t *testing.T) {
	r := harmRun(t, 0, 0, 0)
	if status(r, ItemHarm, "probes") != Green {
		t.Fatalf("no harm expected:\n%s", r.Text())
	}
}

func TestProbeShareOfTrafficIsInformationOnly(t *testing.T) {
	// 60 requests/min from users paused; the probes add far more while running,
	// which is how small the user traffic is, not harm.
	r := harmRun(t, 0, 200, 60)
	if status(r, ItemHarm, "probes") != Green || !strings.Contains(r.Text(), "for information") {
		t.Fatalf("share must be shown and not judged:\n%s", r.Text())
	}
}

func TestProbesThatSlowUsersAreRed(t *testing.T) {
	r := harmRunLatency(t, 0.1, 0.4)
	if status(r, ItemHarm, "web") != Red || !strings.Contains(r.Text(), "longer per request") {
		t.Fatalf("added latency:\n%s", r.Text())
	}
}

func TestSmallLatencyChangesAreGreen(t *testing.T) {
	r := harmRunLatency(t, 0.1, 0.15)
	if status(r, ItemHarm, "probes") != Green {
		t.Fatalf("50 ms is noise:\n%s", r.Text())
	}
}

func TestHarmNeedsTheQuietWindow(t *testing.T) {
	w := newWorld()
	m, _ := appmap.Parse(liveMap)
	p := &fakeProm{}
	_, err := Run(context.Background(), Config{Map: m, Commit: "c", Prom: p, Sender: fakeSender{prom: p, emit: perRoute}, Requests: 1, Poll: time.Second, Wait: w.Wait, Now: w.Now,
		Harm: &HarmConfig{MinRunning: time.Minute}})
	if err == nil {
		t.Fatal("harm without the quiet window accepted")
	}
}

func TestCoverageIsJudgedOnTheProbesPausedWindow(t *testing.T) {
	// The probes hammer the declared route (cumulative counters look fully
	// covered); real users, seen with the probes paused, mostly hit routes no
	// operation declares.
	r := harmRunWith(t, 0, 200, map[string]float64{requestsKey("web", "/items"): 10, requestsKey("web", "/other"): 90}, 0, 0)
	if status(r, ItemObserved, "web") != Red || !strings.Contains(r.Text(), "probes were paused") || !strings.Contains(r.Text(), "/other (90)") {
		t.Fatalf("coverage must come from the paused window:\n%s", r.Text())
	}
	r = harmRunWith(t, 0, 200, map[string]float64{requestsKey("web", "/items"): 90, requestsKey("web", "/other"): 10}, 0, 0)
	if status(r, ItemObserved, "web") != Green {
		t.Fatalf("real traffic mostly declared:\n%s", r.Text())
	}
}
