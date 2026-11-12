package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

// probeSamples is a user-test probe that fails on the given evaluation
// indexes (0-based) and is clear otherwise; past the list it repeats the last.
func probeSamples(total int, failing func(int) bool) *sequenceDetector {
	samples := make([]sdk.Finding, total)
	for i := range samples {
		if failing(i) {
			samples[i] = userTestFinding("browse-items")
		}
	}
	return controllerDetector("traffic-probe", time.Second, samples...)
}

func startupController(t *testing.T, bound time.Duration, start int64, detectors ...sdk.Detector) *Controller {
	t.Helper()
	cfg := testControllerConfig()
	cfg.StartupBound = bound
	cfg.Namespace, cfg.SourceCommit, cfg.RepositoryWorktree = "app", "synthetic-accepted", "synthetic-worktree"
	c, err := NewController(cfg, detectors, staticProvider{}, &recordingDispatcher{requests: make(chan IncidentRequest, 4)}, time.Unix(start, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	c.gitRead = fakeArchitecture("")
	return c
}

func startupState(t *testing.T, c *Controller) *contracts.StartupPeriod {
	t.Helper()
	state, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	return state.GetStartup()
}

func at(second int64) time.Time { return time.Unix(second, 0).UTC() }

// The production setting reaches the controller.
func TestControllerTakesItsStartupBoundFromTheResponseSection(t *testing.T) {
	policy := settings.Default()
	policy.Response.StartupBound = settings.Duration(7 * time.Minute)
	if got := (ControllerConfig{}).WithSettings(policy).StartupBound; got != 7*time.Minute {
		t.Fatalf("StartupBound = %s, want 7m", got)
	}
}

// An opener failing at start opens nothing: the finding is tracked and
// recorded, and nothing is dispatched. Once no opener has fired for one settle
// window the install is seen healthy, the controller is idle, and the next
// failure opens an incident at once.
func TestStartupHoldsOpenersUntilSeenHealthy(t *testing.T) {
	probe := probeSamples(141, func(i int) bool { return i < 10 || i >= 140 })
	c := startupController(t, 30*time.Minute, 0, probe)
	for second := int64(0); second < 10; second++ {
		step(t, c, second)
		if c.IncidentOpen() {
			t.Fatalf("an incident opened during the start-up period at %ds", second)
		}
	}
	if !c.openerFiring("traffic-probe") {
		t.Fatal("the held opener must still be tracked")
	}
	state, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	if !state.GetBusyUntil().AsTime().Equal(at(1800)) {
		t.Fatalf("busy_until while holding = %v, want the bound", state.GetBusyUntil())
	}
	if why := func() string { _, why := c.nextWake(); return why }(); why == "finding batch deadline" {
		t.Fatal("a held batch must not wake the loop")
	}
	for second := int64(10); second < 130; second++ {
		step(t, c, second)
	}
	if startupState(t, c).GetEstablishedAt() != nil {
		t.Fatal("established before a full settle window")
	}
	step(t, c, 130)
	period := startupState(t, c)
	if !period.GetEstablishedAt().AsTime().Equal(at(130)) || period.GetBoundReachedAt() != nil {
		t.Fatalf("period = %v, want established at 130s", period)
	}
	if state, _ := c.ExportState(); state.GetBusyUntil() != nil {
		t.Fatalf("an established install with nothing to do is idle, busy_until = %v", state.GetBusyUntil())
	}
	for second := int64(131); second <= 140; second++ {
		step(t, c, second)
	}
	if !c.IncidentOpen() {
		t.Fatal("a failure after establishment must open an incident")
	}
}

// Any opener sample restarts the window.
func TestStartupWindowRestartsOnAFlap(t *testing.T) {
	probe := probeSamples(300, func(i int) bool { return i == 0 || i == 91 })
	c := startupController(t, 30*time.Minute, 0, probe)
	for second := int64(0); second < 211; second++ {
		step(t, c, second)
	}
	if startupState(t, c).GetEstablishedAt() != nil || c.IncidentOpen() {
		t.Fatal("the flap at 91s must restart the window and open nothing")
	}
	step(t, c, 212)
	if !startupState(t, c).GetEstablishedAt().AsTime().Equal(at(212)) {
		t.Fatalf("period = %v, want established 2 min after the flap cleared", startupState(t, c))
	}
}

// The period cannot hold forever: at the bound it ends and a fault present
// since start opens an incident through the ordinary path.
func TestStartupBoundOpensHeldOpeners(t *testing.T) {
	probe := probeSamples(1, func(int) bool { return true })
	c := startupController(t, 5*time.Minute, 0, probe)
	for second := int64(0); second < 300; second += 5 {
		step(t, c, second)
		if c.IncidentOpen() {
			t.Fatalf("incident opened at %ds, before the bound", second)
		}
	}
	step(t, c, 300)
	if !c.IncidentOpen() {
		t.Fatal("the held opener must open an incident at the bound")
	}
	period := startupState(t, c)
	if !period.GetBoundReachedAt().AsTime().Equal(at(300)) || period.GetEstablishedAt() != nil {
		t.Fatalf("period = %v, want bound reached at 300s", period)
	}
}

// Only openers count: a clue detector that errors does not hold the period.
func TestStartupIgnoresClueErrors(t *testing.T) {
	flaky := controllerDetector("cluster-wiring", time.Second, sdk.Finding{})
	for i := 0; i < 200; i += 2 {
		flaky.errors[i] = errors.New("metrics source timed out")
	}
	probe := probeSamples(1, func(int) bool { return false })
	c := startupController(t, 30*time.Minute, 0, flaky, probe)
	for second := int64(0); second <= 120; second++ {
		step(t, c, second)
	}
	if !startupState(t, c).GetEstablishedAt().AsTime().Equal(at(120)) {
		t.Fatalf("period = %v, want established at 120s despite clue errors", startupState(t, c))
	}
}

// A window cannot establish before every opener detector has completed an
// evaluation in this process.
func TestStartupWaitsForEveryOpenerDetectorToRun(t *testing.T) {
	probe := probeSamples(1, func(int) bool { return false })
	for i := 0; i < 30; i++ {
		probe.errors[i] = errors.New("probe helper not ready")
	}
	c := startupController(t, 30*time.Minute, 0, probe)
	for second := int64(0); second < 150; second++ {
		step(t, c, second)
	}
	if startupState(t, c).GetEstablishedAt() != nil {
		t.Fatal("established before the probe ever completed")
	}
	step(t, c, 150)
	if !startupState(t, c).GetEstablishedAt().AsTime().Equal(at(150)) {
		t.Fatalf("period = %v, want established 2 min after the probe first completed", startupState(t, c))
	}
}

// Seen-healthy is a fact of the install: a restart of an established install
// does not hold the next fault.
func TestRestartOfAnEstablishedInstallDoesNotHold(t *testing.T) {
	first := startupController(t, 30*time.Minute, 0, probeSamples(1, func(int) bool { return false }))
	for second := int64(0); second <= 120; second++ {
		step(t, first, second)
	}
	state, err := first.ExportState()
	if err != nil || state.GetStartup().GetEstablishedAt() == nil {
		t.Fatalf("first process did not establish: %v %v", state.GetStartup(), err)
	}
	second := startupController(t, 30*time.Minute, 500, probeSamples(1, func(int) bool { return true }))
	if err := second.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	step(t, second, 500)
	if !second.IncidentOpen() {
		t.Fatal("a fault after a restart of an established install must open at once")
	}
}

// A restart inside the period continues it with the persisted bound.
func TestRestartInsideThePeriodKeepsItsBound(t *testing.T) {
	first := startupController(t, 30*time.Minute, 0, probeSamples(1, func(int) bool { return true }))
	step(t, first, 0)
	state, err := first.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	second := startupController(t, 30*time.Minute, 600, probeSamples(1, func(int) bool { return true }))
	if err := second.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	step(t, second, 600)
	if second.IncidentOpen() {
		t.Fatal("a restart inside the period must keep holding")
	}
	if period := startupState(t, second); !period.GetBoundAt().AsTime().Equal(at(1800)) || !period.GetBeganAt().AsTime().Equal(at(0)) {
		t.Fatalf("period = %v, want the first process's bound", period)
	}
	step(t, second, 1800)
	if !second.IncidentOpen() {
		t.Fatal("the persisted bound must still end the period")
	}
}

// State persisted before the period existed is an install already running:
// no period, and a restored batched opener dispatches.
func TestRestoredStateWithoutAPeriodSkipsIt(t *testing.T) {
	first := startupController(t, 0, 0, probeSamples(1, func(int) bool { return true }))
	first.batcher = NewDebouncedBatcher(10 * time.Second)
	step(t, first, 0)
	state, err := first.ExportState()
	if err != nil || first.IncidentOpen() || state.GetStartup() != nil || len(state.GetPendingBatch().GetFindings()) != 1 {
		t.Fatalf("want one batched opener and no period: open=%v startup=%v err=%v", first.IncidentOpen(), state.GetStartup(), err)
	}
	second := startupController(t, 30*time.Minute, 5, probeSamples(1, func(int) bool { return true }))
	if err := second.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	if period := startupState(t, second); period.GetSkippedReason() != "restored-state" {
		t.Fatalf("period = %v, want skipped restored-state", period)
	}
	step(t, second, 11)
	if !second.IncidentOpen() {
		t.Fatal("a restored batched opener must dispatch")
	}
}

// A maintenance resume starts no period: a fault injected while paused opens.
func TestResumeAfterPauseStartsNoPeriod(t *testing.T) {
	probe := probeSamples(200, func(i int) bool { return i >= 121 })
	c := startupController(t, 30*time.Minute, 0, probe)
	for second := int64(0); second <= 120; second++ {
		step(t, c, second)
	}
	if startupState(t, c).GetEstablishedAt() == nil {
		t.Fatal("not established before the pause")
	}
	c.now = func() time.Time { return at(400) }
	if err := c.EvaluateAll(context.Background(), at(400)); err != nil {
		t.Fatal(err)
	}
	if !c.IncidentOpen() {
		t.Fatal("a fault present at resume must open an incident")
	}
}

// The operator reset neither starts nor ends the period.
func TestResetDuringThePeriodKeepsIt(t *testing.T) {
	c := startupController(t, 30*time.Minute, 0, probeSamples(1, func(int) bool { return true }))
	step(t, c, 0)
	before := startupState(t, c)
	if _, err := c.ResetIncidents(context.Background(), at(1)); err != nil {
		t.Fatal(err)
	}
	step(t, c, 2)
	after := startupState(t, c)
	if !after.GetBeganAt().AsTime().Equal(before.GetBeganAt().AsTime()) || !after.GetBoundAt().AsTime().Equal(before.GetBoundAt().AsTime()) || after.GetEstablishedAt() != nil || c.IncidentOpen() {
		t.Fatalf("reset changed the period: before %v after %v open %v", before, after, c.IncidentOpen())
	}
}

// The loop wakes at the bound while holding and never at a past bound.
func TestStartupWakeIsTheBoundOnlyWhileHolding(t *testing.T) {
	slow := controllerDetector("traffic-probe", 10*time.Minute, userTestFinding("browse-items"))
	c := startupController(t, 5*time.Minute, 0, slow)
	step(t, c, 0)
	if wake, why := c.nextWake(); !wake.Equal(at(300)) || why != "start-up bound" {
		t.Fatalf("wake = %v %q, want the bound", wake, why)
	}
	step(t, c, 300)
	if !c.IncidentOpen() {
		t.Fatal("the bound must release the held opener")
	}
	if wake, why := c.nextWake(); why == "start-up bound" {
		t.Fatalf("a past bound must not wake the loop: %v %q", wake, why)
	}
}

// External mode and a zero bound run no period.
func TestNoPeriodForExternalModeOrAZeroBound(t *testing.T) {
	if period := startupState(t, startupController(t, 0, 0, probeSamples(1, func(int) bool { return true }))); period != nil {
		t.Fatalf("zero bound: period = %v", period)
	}
	cfg := testControllerConfig()
	cfg.StartupBound, cfg.ExternalIncidentsOnly = 30*time.Minute, true
	c, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, at(0))
	if err != nil {
		t.Fatal(err)
	}
	if period := startupState(t, c); period != nil {
		t.Fatalf("external mode: period = %v", period)
	}
}
