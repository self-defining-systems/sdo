package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
)

// churnArch maps the settling world's api and a second component web that
// no stage write touches.
const churnArch = "# Manifest inventory\n\n<!-- sdo:application-map:begin -->\n```yaml\nversion: 1\ncomponents:\n" +
	"  - {name: api, workload: Deployment/api, services: [api], holds_data: false}\n" +
	"  - {name: web, workload: Deployment/web, services: [web], holds_data: false}\n" +
	"edges: []\noperations: []\nprobes: []\n```\n<!-- sdo:application-map:end -->\n"

// churnDetector reports web unavailable between from and until: churn (a
// drained node, a scale-up) on a component the stage never wrote.
type churnDetector struct {
	clock       *settlingClock
	from, until time.Time
}

func (churnDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "workload-health", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}, Interval: time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingCommit: "seed"}
}

func (d churnDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if now := d.clock.Now(); now.Before(d.from) || !now.Before(d.until) {
		return []sdk.Finding{}, nil
	}
	return []sdk.Finding{{DetectorID: "workload-health", RuleID: "unavailable", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}, Summary: "web has no ready replica"}}, nil
}

// recoveryHeld is the one Recovered decision closure and the stage share: it
// credits only a whole alert set evaluated clear at every observation through
// the stability window. A blocker, a detector that is not evaluated clear
// (unavailable, pending, missing) or a single observation never credits.
func TestRecoveryHeldCreditsOnlyAnAlertSetEvaluatedClearThroughTheWindow(t *testing.T) {
	start := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return start.Add(time.Duration(s) * time.Second) }
	clear := DetectorEvaluation{DetectorID: "health", Status: DetectorEvaluationClear}
	unavailable := DetectorEvaluation{DetectorID: "health", Status: DetectorEvaluationError, Error: "unavailable"}
	type obs struct {
		at      int
		state   DetectorEvaluation
		blocked string
		want    bool
	}
	for name, observations := range map[string][]obs{
		"held through the window": {{0, clear, "", false}, {20, clear, "", false}, {40, clear, "", true}},
		"one observation":         {{100, clear, "", false}},
		"unavailable after one clear sample": {
			{0, clear, "", false}, {10, unavailable, "", false}, {40, unavailable, "", false}, {50, clear, "", false}, {89, clear, "", false}, {90, clear, "", true}},
		"blocked restarts the window": {{0, clear, "", false}, {30, clear, "user test read has 1 of 3 samples", false}, {60, clear, "", false}, {100, clear, "", true}},
		"missing evaluation":          {{0, DetectorEvaluation{DetectorID: "health", Status: DetectorEvaluationPending}, "", false}, {60, DetectorEvaluation{DetectorID: "health", Status: DetectorEvaluationPending}, "", false}},
	} {
		t.Run(name, func(t *testing.T) {
			streaks := clearStreaks{}
			var run time.Time
			for _, o := range observations {
				streaks.track(o.state.DetectorID, at(o.at), o.state.Status == DetectorEvaluationClear)
				got, reason := recoveryHeld(&run, at(o.at), 40*time.Second, []DetectorEvaluation{o.state}, streaks, start, o.blocked)
				if got != o.want {
					t.Fatalf("at %ds held = %v (%s), want %v", o.at, got, reason, o.want)
				}
			}
		})
	}
}

// A stage that applied nothing while findings remain says so: NOTHING_APPLIED
// is not a coverage verdict, and the table counts it apart from UNCOVERED.
func TestStageWithNothingAppliedSaysSo(t *testing.T) {
	stage, state, _, _ := settlingStage(t, settlingCase{stallWindow: 60 * time.Second})
	loop := stage.NewLoop(state.Request)
	report := &contracts.StageReport{Stage: "deterministic"}
	if err := stage.awaitApplications(context.Background(), loop, state, report, &responseStageProgress{}, func(bool) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := report.GetRecoveryWait().GetExitReason(); got != contracts.RecoveryWait_EXIT_REASON_NOTHING_APPLIED {
		t.Fatalf("exit = %s, want NOTHING_APPLIED", got)
	}
}

// instantRecoveryClock is a recovery-wait clock that advances by each wait
// instead of sleeping, so a stage test observes horizons without wall time.
// It starts at the wall clock the loop stamps its receipts with.
type instantRecoveryClock struct {
	mu sync.Mutex
	at time.Time
}

func newInstantRecoveryClock() *instantRecoveryClock {
	return &instantRecoveryClock{at: lifecycleio.Now()}
}

func (c *instantRecoveryClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *instantRecoveryClock) wait(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
	return nil
}
