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

// (b, review C5) Churn that joins the incident on a component no stage write
// touched and clears on its own within its horizon does not send a correct
// playbook's incident to the responder: the stage still clears.
func TestChurnOnAnUntouchedComponentThatClearsDoesNotHandOff(t *testing.T) {
	stage, state, _, _ := settlingStage(t, settlingCase{healAfter: 20 * time.Second, stallWindow: 60 * time.Second, churnFrom: 2 * time.Second, churnUntil: 4 * time.Second})
	agent := &recordingStage{}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	report := deterministicStageReport(state.PriorStages)
	if got := report.GetRecoveryWait().GetExitReason(); got != contracts.RecoveryWait_EXIT_REASON_CLEARED || agent.dispatched {
		t.Fatalf("exit = %s (dispatched %v), want CLEARED without the responder: %v", got, agent.dispatched, report)
	}
}

// (d) A finding on a component no stage write touched that persists past its
// horizon needs another actor: the stage hands off, but not at first sight.
func TestPersistentFindingOnAnUntouchedComponentHandsOffAfterItsHorizon(t *testing.T) {
	stage, state, clock, _ := settlingStage(t, settlingCase{healAfter: 20 * time.Second, stallWindow: 60 * time.Second, churnFrom: 2 * time.Second, churnUntil: time.Hour})
	agent := &recordingStage{}
	started := clock.Now()
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	waited := clock.Now().Sub(started)
	report := deterministicStageReport(state.PriorStages)
	if got := report.GetRecoveryWait().GetExitReason(); got != contracts.RecoveryWait_EXIT_REASON_UNCOVERED || !agent.dispatched {
		t.Fatalf("exit = %s (dispatched %v), want UNCOVERED to the responder", got, agent.dispatched)
	}
	if waited < 4*time.Second || waited > 10*time.Second {
		t.Fatalf("handed off after %s, want once the finding outlived its horizon (4s to 10s)", waited)
	}
}

func stageReportWith(restored ...bool) *contracts.StageReport {
	report := &contracts.StageReport{}
	for _, r := range restored {
		report.Mitigations = append(report.Mitigations, &contracts.StageMitigation{ApplyReceipt: &contracts.PlanApplication{Applied: true, RestoredSource: r}})
	}
	return report
}

// (a, c, review C5) The stage clears on the recovered test: at operation
// strength always; at reachability only when every stage write's receipt
// proves it restored accepted source; never when a component is unobserved
// or a program still diagnoses its fault.
func TestStageClearsOnRecoveredAndReceiptProvenRestores(t *testing.T) {
	operation := recoveryCheck{verdict: recoveryVerified}
	reachability := recoveryCheck{verdict: recoveryWeak, reason: "cache: reachability only"}
	unobserved := recoveryCheck{verdict: recoveryWeak, unobservable: true, reason: "cache: no probe or link covers it"}
	for name, test := range map[string]struct {
		check    recoveryCheck
		report   *contracts.StageReport
		matching string
		want     stageDecision
	}{
		"operation strength, restart":                 {operation, stageReportWith(false), "", stageCleared},
		"reachability, reconcile from source (fge17)": {reachability, stageReportWith(true), "", stageCleared},
		"reachability, restart":                       {reachability, stageReportWith(false), "", stageHandOff},
		"reachability, one write unproven":            {reachability, stageReportWith(true, false), "", stageHandOff},
		"unobserved, restored source":                 {unobserved, stageReportWith(true), "", stageHandOff},
		"operation strength, program still matching":  {operation, stageReportWith(false), "program p still diagnoses the fault", stageWait},
		"members still pending":                       {recoveryCheck{verdict: recoveryPending}, stageReportWith(false), "", stageWait},
	} {
		t.Run(name, func(t *testing.T) {
			if got := decideStageRecovery(test.check, test.matching, test.report); got != test.want {
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
