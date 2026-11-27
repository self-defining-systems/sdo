package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/journeycheck"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

type memorySoakStore struct {
	record *journeycheck.SoakRecord
	saves  int
}

func (s *memorySoakStore) Load(context.Context) (*journeycheck.SoakRecord, error) {
	return s.record, nil
}

func (s *memorySoakStore) Save(_ context.Context, record journeycheck.SoakRecord) error {
	s.saves++
	if s.record == nil {
		s.record = &record
	}
	return nil
}

// soakController is a start-up controller with a 5 minute onboarding soak
// whose snapshot carries one health-probe window with the given samples.
func soakController(t *testing.T, store *memorySoakStore, samples []traffic.Sample, detectors ...sdk.Detector) *Controller {
	t.Helper()
	cfg := testControllerConfig()
	cfg.StartupBound, cfg.OnboardingSoak = 30*time.Minute, 5*time.Minute
	cfg.Namespace, cfg.SourceCommit, cfg.RepositoryWorktree = "app", "synthetic-accepted", "synthetic-worktree"
	window := traffic.Window{Workload: traffic.Workload{Name: "probe-call", Purpose: traffic.PurposeHealthProbe}.WithDefaults(),
		Scenarios: []traffic.ScenarioObservations{{Scenario: traffic.Descriptor{ID: "sign-in"}, Qualified: true, Samples: samples}}}
	provider := staticProvider{snapshot: windowSnapshot{DetectionContext: sdktest.Snapshot{}, windows: map[string]traffic.Window{"probe-call": window}}}
	c, err := NewController(cfg, detectors, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 4)}, at(0))
	if err != nil {
		t.Fatal(err)
	}
	c.gitRead = fakeArchitecture("")
	if err := c.AttachStateStore(context.Background(), &memoryStateStore{}); err != nil {
		t.Fatal(err)
	}
	if err := c.AttachOnboardingSoak(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	return c
}

func runSeconds(t *testing.T, c *Controller, from, to int64) {
	t.Helper()
	for second := from; second <= to; second++ {
		step(t, c, second)
		if err := c.PersistState(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

// A new install soaks once every opener ran: the period is not established
// before the soak's end, though nothing fires, and the record is written
// once; after it the period ends the ordinary way.
func TestNewInstallSoaksBeforeItsStartupPeriodCanEnd(t *testing.T) {
	store := &memorySoakStore{}
	ok := []traffic.Sample{{At: at(10), Iteration: 1, Outcome: traffic.OutcomeOK}}
	c := soakController(t, store, ok, probeSamples(1, func(int) bool { return false }))
	runSeconds(t, c, 0, 299)
	if startupState(t, c).GetEstablishedAt() != nil || store.record != nil {
		t.Fatalf("established or judged inside the soak: %v %+v", startupState(t, c), store.record)
	}
	if holders := strings.Join(startupState(t, c).GetHolders(), "; "); !strings.Contains(holders, "onboarding soak until") {
		t.Fatalf("holders = %q", holders)
	}
	runSeconds(t, c, 300, 430)
	if store.record == nil || len(store.record.Red()) != 0 || len(store.record.Results) != 1 || store.saves != 1 {
		t.Fatalf("record = %+v (saves %d), want one green line saved once", store.record, store.saves)
	}
	if startupState(t, c).GetEstablishedAt() == nil {
		t.Fatal("a green soak with nothing firing must let the period end")
	}
}

// A failed probe sample in the soak is red although no opener fired, and
// nothing opens.
func TestSoakRecordsAFlakyProbeWithoutOpeningAnIncident(t *testing.T) {
	store := &memorySoakStore{}
	samples := []traffic.Sample{{At: at(5), Iteration: 1, Outcome: traffic.OutcomeOK}, {At: at(60), Iteration: 2, Outcome: traffic.OutcomeError, Error: "synthetic flake"}}
	c := soakController(t, store, samples, probeSamples(1, func(int) bool { return false }))
	runSeconds(t, c, 0, 310)
	if store.record == nil || len(store.record.Red()) != 1 || !strings.Contains(journeycheck.SoakFollowUp(*store.record), "probe-call/sign-in") {
		t.Fatalf("record = %+v", store.record)
	}
	if c.IncidentOpen() {
		t.Fatal("a soak red must never open an incident")
	}
}

// An install that already soaked never soaks again: one follow-up at most.
func TestInstallWithASoakRecordDoesNotSoakAgain(t *testing.T) {
	store := &memorySoakStore{record: &journeycheck.SoakRecord{EndedAt: at(-60)}}
	c := soakController(t, store, nil, probeSamples(1, func(int) bool { return false }))
	runSeconds(t, c, 0, 130)
	if startupState(t, c).GetEstablishedAt() == nil || store.saves != 0 {
		t.Fatalf("period %v, saves %d: a soaked install holds only for the ordinary settle window", startupState(t, c), store.saves)
	}
}

// The default soak is the in-session healthy run's 5 minutes.
func TestControllerTakesItsOnboardingSoakFromTheOnboardingSection(t *testing.T) {
	if got := (ControllerConfig{}).WithSettings(settings.Default()).OnboardingSoak; got != 5*time.Minute {
		t.Fatalf("OnboardingSoak = %s, want 5m", got)
	}
}
