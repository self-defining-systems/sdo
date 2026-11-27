package traffic

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// jobScenario creates a job and waits for background work to move it from
// "created" to "done": the effect of the write arrives after the request.
func jobScenario() Scenario {
	return Scenario{
		ID: "job-runs", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectIdempotentWrite, Marker: "sdo-job",
		Steps: []Step{
			{Name: "create-job", Endpoint: POST("/jobs", Params{"name": Const("sdo-job")}).Status(201)},
			{Name: "job-done", Endpoint: Eventually(GET("/jobs/sdo-job", nil).Status(200).JSONEquals("status", "done"))},
		},
	}
}

// workerServer answers "created" until doneAt, then "done"; a zero doneAt
// means the worker never runs.
func workerServer(clock *simClock, doneAt time.Time) *simServer {
	return &simServer{clock: clock, handle: func(now time.Time, request *http.Request) (int, string) {
		if request.Method == http.MethodPost {
			return 201, `{"status":"created"}`
		}
		if !doneAt.IsZero() && !now.Before(doneAt) {
			return 200, `{"status":"done"}`
		}
		return 200, `{"status":"created"}`
	}}
}

func TestEventualStepPassesOnceTheEffectAppears(t *testing.T) {
	clock := newSimClock()
	server := workerServer(clock, clock.Now().Add(3*time.Second))
	engine := simEngine(t, probeWorkload("job-runs"), Catalog{jobScenario()}, server)
	_, sample := engine.Iterate(t.Context(), 0)
	if sample.Outcome != OutcomeOK {
		t.Fatalf("outcome %s: %s", sample.Outcome, sample.Error)
	}
	if sample.Requests < 4 {
		t.Fatalf("requests %d: every read counts", sample.Requests)
	}
	// The time until the effect appeared is the step's convergence.
	if len(sample.Convergence) != 1 || !sample.Convergence[0].Converged || sample.Convergence[0].Took < 3*time.Second {
		t.Fatalf("convergence %+v does not record the wait for the effect", sample.Convergence)
	}
}

func TestEventualStepFailsAtTheDeadlineWithTheLastReason(t *testing.T) {
	clock := newSimClock()
	server := workerServer(clock, time.Time{})
	engine := simEngine(t, probeWorkload("job-runs"), Catalog{jobScenario()}, server)
	start := clock.Now()
	_, sample := engine.Iterate(t.Context(), 0)
	if sample.Outcome != OutcomeError || sample.Step != "job-done" {
		t.Fatalf("outcome %s step %s: want an error on job-done", sample.Outcome, sample.Step)
	}
	if !strings.Contains(sample.Error, "still not there") || !strings.Contains(sample.Error, "status") {
		t.Fatalf("error %q names neither the wait nor the last rejection", sample.Error)
	}
	// The last read starts only when a full request still fits before the
	// iteration deadline, so the sample is a wrong answer, never a timeout.
	if elapsed := clock.Now().Sub(start); elapsed < DefaultIterationTimeout-DefaultTimeout-time.Second || elapsed > DefaultIterationTimeout {
		t.Fatalf("gave up after %s, want just before the %s iteration deadline", elapsed, DefaultIterationTimeout)
	}
	if sample.Done.IsZero() || sample.ObservedAt().Before(start.Add(DefaultIterationTimeout-DefaultTimeout-time.Second)) {
		t.Fatalf("observed at %s: a sample is observed when its steps finished", sample.ObservedAt())
	}
}

func TestEventualStepFailsAtOnceOnServerErrors(t *testing.T) {
	clock := newSimClock()
	server := &simServer{clock: clock, handle: func(_ time.Time, request *http.Request) (int, string) {
		if request.Method == http.MethodPost {
			return 201, `{}`
		}
		return 503, `unavailable`
	}}
	engine := simEngine(t, probeWorkload("job-runs"), Catalog{jobScenario()}, server)
	start := clock.Now()
	_, sample := engine.Iterate(t.Context(), 0)
	if sample.Outcome != OutcomeError || sample.Status != 503 {
		t.Fatalf("outcome %s status %d: want the 503 reported", sample.Outcome, sample.Status)
	}
	if elapsed := clock.Now().Sub(start); elapsed > time.Second {
		t.Fatalf("a 5xx waited %s; an unhealthy target must be reported at once", elapsed)
	}
}

func TestEventualStepRepeatsOnlyReads(t *testing.T) {
	clock := newSimClock()
	server := workerServer(clock, time.Time{})
	scenario := jobScenario()
	scenario.Steps[1].Endpoint = Eventually(POST("/jobs/sdo-job/poke", Params{"m": Const("sdo-job")}))
	engine := simEngine(t, probeWorkload("job-runs"), Catalog{scenario}, server)
	_, sample := engine.Iterate(t.Context(), 0)
	if sample.Outcome != OutcomeInvalid || !strings.Contains(sample.Error, "GET or HEAD") {
		t.Fatalf("outcome %s error %q: a repeated write must be an invalid sample", sample.Outcome, sample.Error)
	}
}

func TestEventualScenarioFacts(t *testing.T) {
	scenario := jobScenario()
	if !scenario.WaitsForEffect() {
		t.Fatal("WaitsForEffect is false for a scenario with an eventual step")
	}
	plain := Scenario{Steps: []Step{{Name: "read", Endpoint: GET("/x", nil).Status(200)}}}
	if plain.WaitsForEffect() {
		t.Fatal("WaitsForEffect is true without an eventual step")
	}
	statusOnly := Scenario{Steps: []Step{{Name: "read", Endpoint: Eventually(GET("/x", nil).Status(200))}}}
	if !statusOnly.StatusOnly() {
		t.Fatal("StatusOnly does not look inside the eventual wrapper")
	}
	if scenario.StatusOnly() {
		t.Fatal("a content check inside the wrapper reads as status-only")
	}
}

// A failing eventual sample finishes about an iteration timeout after it
// started. Its freshness is judged from when it was observed, so a 25 s wait
// with the default 30 s maxAge still accumulates MinSamples failures.
func TestEventualFailuresStayFreshUntilTheyCount(t *testing.T) {
	start := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	var samples []Sample
	for i := range 3 {
		samples = append(samples, Sample{At: start.Add(time.Duration(i) * MinSampleInterval), Latency: time.Second, Done: start.Add(time.Duration(i)*MinSampleInterval + 25*time.Second), Outcome: OutcomeError, Step: "job-done"})
	}
	now := samples[2].ObservedAt()
	verdict := Evaluate("job-runs", defaultSLO(), samples, now)
	if !verdict.Evaluated || verdict.Healthy {
		t.Fatalf("evaluated %v healthy %v samples %d: three observed failures inside maxAge must fire", verdict.Evaluated, verdict.Healthy, verdict.Samples)
	}
}

// Hermetic repro of a stopped background worker: the eventual journey fails
// every sample, the scenario's SLO fires on the default settings, and once
// the worker runs again fresh samples clear it. The prober starts a sample
// every MinSampleInterval and runs them concurrently, so each iteration runs
// on its own clock started on that schedule.
func TestStoppedWorkerFiresAndRecoversThroughTheSLO(t *testing.T) {
	base := newSimClock().Now()
	running := false
	var samples []Sample
	var now time.Time
	run := func(n int) SLOVerdict {
		for range n {
			clock := &simClock{now: base.Add(time.Duration(len(samples)) * MinSampleInterval)}
			server := &simServer{clock: clock, handle: func(_ time.Time, request *http.Request) (int, string) {
				if request.Method == http.MethodPost {
					return 201, `{"status":"created"}`
				}
				if running {
					return 200, `{"status":"done"}`
				}
				return 200, `{"status":"created"}`
			}}
			engine := simEngine(t, probeWorkload("job-runs"), Catalog{jobScenario()}, server)
			_, sample := engine.Iterate(t.Context(), uint64(len(samples)))
			samples = append(samples, sample)
			now = sample.ObservedAt()
		}
		return Evaluate("job-runs", defaultSLO(), samples, now)
	}
	if verdict := run(3); !verdict.Evaluated || verdict.Healthy {
		t.Fatalf("stopped worker: evaluated %v healthy %v samples %d", verdict.Evaluated, verdict.Healthy, verdict.Samples)
	}
	running = true
	if verdict := run(5); !verdict.Healthy {
		t.Fatalf("restored worker still fails: %v", verdict.Violations)
	}
}
