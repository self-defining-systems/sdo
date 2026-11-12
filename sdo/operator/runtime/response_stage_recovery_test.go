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

// The stage clears on Recovered and waits while it does not hold or while a
// program still diagnoses its fault. Strength levels and restored-source
// receipts no longer exist.
func TestStageClearsOnRecovered(t *testing.T) {
	for name, test := range map[string]struct {
		check    recoveryCheck
		matching string
		want     stageDecision
	}{
		"recovered":                  {recoveryCheck{verdict: recoveryVerified}, "", stageCleared},
		"program still matching":     {recoveryCheck{verdict: recoveryVerified}, "program p still diagnoses the fault", stageWait},
		"opening user test not back": {recoveryCheck{verdict: recoveryPending}, "", stageWait},
	} {
		t.Run(name, func(t *testing.T) {
			if got := decideStageRecovery(test.check, test.matching); got != test.want {
				t.Fatalf("decision = %v, want %v", got, test.want)
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
