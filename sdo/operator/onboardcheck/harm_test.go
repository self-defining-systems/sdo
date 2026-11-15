package onboardcheck

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/memory/appmap"
)

func harmRun(t *testing.T, fail, reqs float64, pausedReqs float64) Report {
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
		PausedEnd:   HarmSample{At: start.Add(-time.Minute), Counts: map[string]float64{"failures|jobs_failed_total{worker=CreatePipelineWorker}": 0.5, requestsKey("web", "/items"): pausedReqs}},
		MinRunning:  time.Minute,
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
	}
	cfg := Config{Map: m, Commit: "c", Prom: p, Sender: fakeSender{prom: p, emit: perRoute}, Requests: 1, Poll: time.Second, Wait: w.Wait, Now: w.Now,
		Quiet: &QuietConfig{Eval: w, Window: 5 * time.Minute, Interval: time.Minute},
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

func TestProbesThatAreMostTrafficAreRedWhenUsersExist(t *testing.T) {
	// 60 requests/min from users paused; the probes add far more while running.
	r := harmRun(t, 0, 200, 60)
	if status(r, ItemHarm, "web") != Red || !strings.Contains(r.Text(), "probes are") {
		t.Fatalf("probe share:\n%s", r.Text())
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
