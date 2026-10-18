package runtime

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// scriptedGauge is a deterministic, test-controlled host-load source. A test
// sets the pressure a load trajectory should report and drives the controller's
// fake clock, so an admission decision never depends on wall-clock timing.
type scriptedGauge struct {
	mu       sync.Mutex
	pressure float64
	samples  int
}

func (g *scriptedGauge) set(pressure float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pressure = pressure
}

func (g *scriptedGauge) Sample() (LoadSample, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.samples++
	return LoadSample{Pressure: g.pressure}, nil
}

func (g *scriptedGauge) sampleCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.samples
}

func TestAdmissionConfigValidateRejectsMisconfiguration(t *testing.T) {
	cases := []struct {
		name string
		cfg  AdmissionConfig
		ok   bool
	}{
		{name: "disabled-zero-value", cfg: AdmissionConfig{}, ok: true},
		{
			name: "load-term",
			cfg:  AdmissionConfig{Enabled: true, HighWatermark: 80, LowWatermark: 50, RecheckInterval: time.Second},
			ok:   true,
		},
		{
			name: "backlog-term-only",
			cfg: AdmissionConfig{
				Enabled: true, ReleaseBacklogHighWatermark: 4, ReleaseBacklogLowWatermark: 2, RecheckInterval: time.Second,
			},
			ok: true,
		},
		{
			name: "enabled-without-high-watermark",
			cfg:  AdmissionConfig{Enabled: true, RecheckInterval: time.Second},
			ok:   false,
		},
		{
			name: "inverted-load-watermarks",
			cfg:  AdmissionConfig{Enabled: true, HighWatermark: 50, LowWatermark: 80, RecheckInterval: time.Second},
			ok:   false,
		},
		{
			name: "inverted-backlog-watermarks",
			cfg: AdmissionConfig{
				Enabled: true, ReleaseBacklogHighWatermark: 2, ReleaseBacklogLowWatermark: 5, RecheckInterval: time.Second,
			},
			ok: false,
		},
		{
			name: "non-positive-recheck",
			cfg:  AdmissionConfig{Enabled: true, HighWatermark: 80, LowWatermark: 50},
			ok:   false,
		},
		{
			name: "negative-watermark",
			cfg:  AdmissionConfig{Enabled: true, HighWatermark: -1, RecheckInterval: time.Second},
			ok:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			if tc.ok && err != nil {
				t.Fatalf("valid config rejected: %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("invalid config accepted")
			}
		})
	}
}

func TestAdmissionAssessAppliesHysteresis(t *testing.T) {
	cfg := AdmissionConfig{
		Enabled: true, HighWatermark: 80, LowWatermark: 50,
		ReleaseBacklogHighWatermark: 4, ReleaseBacklogLowWatermark: 2, RecheckInterval: time.Second,
	}
	cases := []struct {
		name       string
		deferred   bool
		pressure   float64
		backlog    int
		wantDefer  bool
		wantReason string
	}{
		{name: "below-high-admits", deferred: false, pressure: 10, backlog: 0, wantDefer: false, wantReason: ""},
		{name: "load-at-high-defers", deferred: false, pressure: 80, backlog: 0, wantDefer: true, wantReason: admissionReasonLoad},
		{name: "backlog-at-high-defers", deferred: false, pressure: 0, backlog: 4, wantDefer: true, wantReason: admissionReasonBacklog},
		// Hysteresis: a held incident stays held in the band between low and high.
		{name: "deferred-in-band-stays", deferred: true, pressure: 60, backlog: 0, wantDefer: true, wantReason: admissionReasonLoad},
		{name: "deferred-backlog-in-band-stays", deferred: true, pressure: 0, backlog: 3, wantDefer: true, wantReason: admissionReasonBacklog},
		{name: "both-below-low-resumes", deferred: true, pressure: 49, backlog: 1, wantDefer: false, wantReason: admissionReasonCleared},
		// Resume requires BOTH terms to clear their low watermarks.
		{name: "load-clear-backlog-hot-stays", deferred: true, pressure: 10, backlog: 2, wantDefer: true, wantReason: admissionReasonBacklog},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cfg.assess(tc.deferred, LoadSample{Pressure: tc.pressure}, tc.backlog)
			if got.Defer != tc.wantDefer {
				t.Fatalf("defer = %v, want %v", got.Defer, tc.wantDefer)
			}
			if got.Reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q", got.Reason, tc.wantReason)
			}
		})
	}
}

func TestAdmissionAssessDisabledNeverDefers(t *testing.T) {
	cfg := AdmissionConfig{}
	if got := cfg.assess(false, LoadSample{Pressure: 1_000_000}, 1_000); got.Defer {
		t.Fatalf("disabled admission deferred under load")
	}
}

func TestProcPressureGaugeReadsAvg10(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu")
	content := "some avg10=12.34 avg60=5.00 avg300=1.00 total=99999\n" +
		"full avg10=1.00 avg60=0.50 avg300=0.10 total=4242\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write pressure file: %v", err)
	}
	sample, err := ProcPressureGauge{Path: path}.Sample()
	if err != nil {
		t.Fatalf("sample: %v", err)
	}
	if sample.Pressure != 12.34 {
		t.Fatalf("pressure = %v, want 12.34", sample.Pressure)
	}
}

func TestProcPressureGaugeMissingFileReportsZero(t *testing.T) {
	sample, err := ProcPressureGauge{Path: filepath.Join(t.TempDir(), "absent")}.Sample()
	if err != nil {
		t.Fatalf("missing pressure file should not error: %v", err)
	}
	if sample.Pressure != 0 {
		t.Fatalf("missing pressure file reported %v, want 0", sample.Pressure)
	}
}

func TestProcPressureGaugeRejectsMalformedLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu")
	if err := os.WriteFile(path, []byte("some avg10=notanumber avg60=0\n"), 0o644); err != nil {
		t.Fatalf("write pressure file: %v", err)
	}
	if _, err := (ProcPressureGauge{Path: path}).Sample(); err == nil {
		t.Fatalf("malformed pressure line accepted")
	}
}

// admissionController builds a brokerless controller with a scripted load gauge,
// a memory lifecycle sink, and a fully controlled clock, so admission decisions
// land at deterministic instants.
func admissionController(
	t *testing.T, cfg AdmissionConfig, detectors ...sdk.Detector,
) (*Controller, *recordingDispatcher, *scriptedGauge, *memoryLifecycleSink, func(int64)) {
	t.Helper()
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 4)}
	config := testControllerConfig()
	config.Admission = cfg
	controller, err := NewController(
		config, detectors, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	gauge := &scriptedGauge{}
	controller.SetLoadGauge(gauge)
	lifecycle := &memoryLifecycleSink{}
	controller.SetLifecycleSink(lifecycle)
	step := func(sec int64) {
		now := time.Unix(sec, 0).UTC()
		if err := controller.Step(context.Background(), now, nil); err != nil {
			t.Fatalf("step %d: %v", sec, err)
		}
	}
	return controller, dispatcher, gauge, lifecycle, step
}

func admissionDetector() *sequenceDetector {
	return controllerDetector(
		"health", time.Second,
		stateFinding("fault"), stateFinding("fault"), stateFinding("fault"),
		stateFinding("fault"), stateFinding("fault"), stateFinding("fault"),
	)
}

// TestAdmissionDefersThenResumesUnderLoad proves the core contract: a ready batch
// is held (not dropped) while host load is above the high watermark, the hold is
// observable as a single admission_deferred lifecycle event, and once load falls
// below the low watermark the SAME incident resumes, emits one admission_resumed
// event, and dispatches exactly once.
func TestAdmissionDefersThenResumesUnderLoad(t *testing.T) {
	cfg := AdmissionConfig{Enabled: true, HighWatermark: 80, LowWatermark: 50, RecheckInterval: time.Second}
	controller, dispatcher, gauge, lifecycle, step := admissionController(t, cfg, admissionDetector())

	gauge.set(90) // above the high watermark
	step(0)       // first firing sample, below the firing threshold
	step(1)       // firing threshold reached: the incident opens, but is held

	if got := controller.ExportState().DispatchState; got != admissionDeferredState {
		t.Fatalf("incident was not held under load: dispatch state %q", got)
	}
	if _, ok := controller.PendingDispatchEffect(); ok {
		t.Fatal("a held incident produced a dispatch effect")
	}
	assertNoRequest(t, dispatcher.requests)
	opened := lifecycle.byPhase(PhaseOpened)
	if len(opened) != 1 {
		t.Fatalf("expected exactly one incident_opened event, got %d", len(opened))
	}
	deferredEvents := lifecycle.byPhase(PhaseAdmissionDeferred)
	if len(deferredEvents) != 1 {
		t.Fatalf("expected exactly one admission_deferred event, got %d", len(deferredEvents))
	}
	deferral := deferredEvents[0]
	if deferral.IncidentID != opened[0].IncidentID {
		t.Fatalf("deferral %q not keyed to the opened incident %q", deferral.IncidentID, opened[0].IncidentID)
	}
	if deferral.Reason != admissionReasonLoad || deferral.LoadThreshold != 80 || deferral.LoadPressure != 90 {
		t.Fatalf("admission_deferred did not record the load decision: %+v", deferral)
	}

	// Still above the low watermark: the hold persists and emits nothing new.
	gauge.set(60)
	step(2)
	if got := controller.ExportState().DispatchState; got != admissionDeferredState {
		t.Fatalf("incident resumed inside the hysteresis band: dispatch state %q", got)
	}
	if len(lifecycle.byPhase(PhaseAdmissionDeferred)) != 1 {
		t.Fatal("admission_deferred was re-emitted during a continuous hold")
	}
	if len(lifecycle.byPhase(PhaseAdmissionResumed)) != 0 {
		t.Fatal("incident resumed before load cleared the low watermark")
	}

	// Below the low watermark: the incident resumes and dispatches, once.
	gauge.set(40)
	step(3)
	resumed := lifecycle.byPhase(PhaseAdmissionResumed)
	if len(resumed) != 1 {
		t.Fatalf("expected exactly one admission_resumed event, got %d", len(resumed))
	}
	if resumed[0].Reason != admissionReasonCleared || resumed[0].LoadThreshold != 50 {
		t.Fatalf("admission_resumed did not record the clearing decision: %+v", resumed[0])
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if request.IncidentID != opened[0].IncidentID {
		t.Fatalf("resumed dispatch opened a different incident: got %q want %q", request.IncidentID, opened[0].IncidentID)
	}
	if dispatcher.callCount() != 1 {
		t.Fatalf("expected exactly one dispatch after resume, got %d", dispatcher.callCount())
	}
}

// TestAdmissionHeldIncidentCoalescesLaterFindings proves deferral queues work
// rather than dropping it: a finding that activates during the hold is attached
// to the held incident, so the responder still sees all corroborating evidence.
func TestAdmissionHeldIncidentCoalescesLaterFindings(t *testing.T) {
	cfg := AdmissionConfig{Enabled: true, HighWatermark: 80, LowWatermark: 50, RecheckInterval: time.Second}
	second := controllerDetector(
		"second", time.Second,
		sdk.Finding{}, sdk.Finding{}, stateFinding("other"), stateFinding("other"), stateFinding("other"),
	)
	controller, dispatcher, gauge, lifecycle, step := admissionController(t, cfg, admissionDetector(), second)

	gauge.set(90)
	step(0)
	step(1) // health opens the incident, held under load
	if got := controller.ExportState().DispatchState; got != admissionDeferredState {
		t.Fatalf("incident was not held: %q", got)
	}
	// A second detector activates while the incident is held.
	step(2)
	step(3)

	gauge.set(10)
	step(4) // resume
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if len(request.Findings) != 2 {
		t.Fatalf("held incident did not coalesce the later finding: %#v", request.Findings)
	}
	if len(lifecycle.byPhase(PhaseOpened)) != 1 {
		t.Fatal("coalescing opened a second incident")
	}
}

// TestAdmissionDefersOnReleaseBacklog proves the controller's own stranded-worktree
// reap backlog is a backpressure term: a deep backlog defers new incident
// admission, and clearing it resumes the incident, with no host-load gauge.
func TestAdmissionDefersOnReleaseBacklog(t *testing.T) {
	cfg := AdmissionConfig{
		Enabled: true, ReleaseBacklogHighWatermark: 2, ReleaseBacklogLowWatermark: 1, RecheckInterval: time.Second,
	}
	controller, dispatcher, _, lifecycle, step := admissionController(t, cfg, admissionDetector())

	// Simulate a congested drain: two worktrees still awaiting reap.
	controller.mu.Lock()
	controller.pendingReleases = []string{"incident-a", "incident-b"}
	controller.mu.Unlock()

	step(0)
	step(1) // incident opens but is held behind the reap backlog
	if got := controller.ExportState().DispatchState; got != admissionDeferredState {
		t.Fatalf("incident was not held behind the release backlog: %q", got)
	}
	deferredEvents := lifecycle.byPhase(PhaseAdmissionDeferred)
	if len(deferredEvents) != 1 || deferredEvents[0].Reason != admissionReasonBacklog {
		t.Fatalf("expected one backlog deferral, got %+v", deferredEvents)
	}
	if deferredEvents[0].ReleaseBacklog != 2 {
		t.Fatalf("deferral did not record the backlog depth: %+v", deferredEvents[0])
	}

	// The drain catches up below the low watermark.
	controller.mu.Lock()
	controller.pendingReleases = nil
	controller.mu.Unlock()
	step(2)
	if len(lifecycle.byPhase(PhaseAdmissionResumed)) != 1 {
		t.Fatal("clearing the release backlog did not resume the incident")
	}
	executePendingEffect(t, controller)
	awaitRequest(t, dispatcher.requests)
	if dispatcher.callCount() != 1 {
		t.Fatalf("expected one dispatch after the backlog cleared, got %d", dispatcher.callCount())
	}
}

// TestAdmissionDeferredStateSurvivesRestart proves a held incident is durable:
// the admission_deferred dispatch state round-trips through ExportState /
// RestoreState, and a restored controller re-samples and resumes once load clears.
func TestAdmissionDeferredStateSurvivesRestart(t *testing.T) {
	cfg := AdmissionConfig{Enabled: true, HighWatermark: 80, LowWatermark: 50, RecheckInterval: time.Second}
	controller, _, gauge, _, step := admissionController(t, cfg, admissionDetector())

	gauge.set(90)
	step(0)
	step(1)
	state := controller.ExportState()
	if state.DispatchState != admissionDeferredState {
		t.Fatalf("held incident not exported as deferred: %q", state.DispatchState)
	}

	// Restart: a fresh controller restores the held incident.
	restarted, dispatcher, lowGauge, lifecycle, restartStep := admissionController(t, cfg, admissionDetector())
	lowGauge.set(40) // load has cleared by the time the controller comes back
	if err := restarted.RestoreState(state); err != nil {
		t.Fatalf("restore held incident: %v", err)
	}
	if got := restarted.ExportState().DispatchState; got != admissionDeferredState {
		t.Fatalf("restore lost the held state: %q", got)
	}
	restartStep(2) // re-sample: load has cleared, the incident resumes
	if len(lifecycle.byPhase(PhaseAdmissionResumed)) != 1 {
		t.Fatal("restored held incident did not resume once load cleared")
	}
	executePendingEffect(t, restarted)
	awaitRequest(t, dispatcher.requests)
	if lowGauge.sampleCount() == 0 {
		t.Fatal("restored controller never re-sampled host load")
	}
}

// TestAdmissionDisabledAdmitsImmediately pins the default: with admission off a
// ready batch dispatches with no deferral and no admission lifecycle events,
// byte-for-byte as before.
func TestAdmissionDisabledAdmitsImmediately(t *testing.T) {
	controller, dispatcher, _, lifecycle, step := admissionController(t, AdmissionConfig{}, admissionDetector())
	step(0)
	step(1)
	if got := controller.ExportState().DispatchState; got != "pending" {
		t.Fatalf("disabled admission held the incident: %q", got)
	}
	if len(lifecycle.byPhase(PhaseAdmissionDeferred)) != 0 || len(lifecycle.byPhase(PhaseAdmissionResumed)) != 0 {
		t.Fatal("disabled admission emitted admission lifecycle events")
	}
	executePendingEffect(t, controller)
	awaitRequest(t, dispatcher.requests)
}
