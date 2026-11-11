package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// restartingStoreDetector is a health detector reading a store the write
// restarts: once the write lands, its read finds the store's container not
// running until until, then the store reads healthy.
type restartingStoreDetector struct {
	world *settlingWorld
	until time.Time
}

func (d restartingStoreDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "store-state", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches:  []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}},
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingCommit: "seed"}
}

func (d restartingStoreDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.world.restarts.Load() > 0 && d.world.clock.Now().Before(d.until) {
		return nil, errors.New("store cache Pod cache-0: container not running")
	}
	return []sdk.Finding{}, nil
}

// A detector that cannot read a store the write is restarting has not observed
// the outcome yet; that is not the write failing. The application and its
// attempt record no error, and the recovery wait keeps observing until the
// store reads again and recovery is proven.
func TestUnavailableReadDuringPostApplyVerificationIsNotAFailedWrite(t *testing.T) {
	stage, state, clock, world := settlingStage(t, settlingCase{healAfter: 20 * time.Second, traffic: &settlingTraffic{}, storeRestart: 5 * time.Second})
	// The write completes within the apply (as a Pod delete the StatefulSet
	// recreates does), so the apply's own verification reads the store.
	world.observed.Store(1)
	started := clock.Now()
	agent := &recordingStage{}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	report := state.PriorStages[0]
	if len(report.Mitigations) != 1 || world.restarts.Load() != 1 {
		t.Fatalf("the stage must apply one restart (writes=%d): %v", world.restarts.Load(), report)
	}
	attempt := report.Mitigations[0]
	if attempt.GetError() != "" || attempt.GetApplyReceipt().GetError() != "" {
		t.Fatalf("an unavailable read was recorded as the write failing: attempt %q, receipt %q", attempt.GetError(), attempt.GetApplyReceipt().GetError())
	}
	if got := report.GetRecoveryWait().GetExitReason(); got != contracts.RecoveryWait_EXIT_REASON_CLEARED || attempt.Result != "applied_cleared" || agent.dispatched {
		t.Fatalf("exit %s after %s, result %s, dispatched %v; want CLEARED, applied_cleared, not dispatched: %v", got, clock.Now().Sub(started), attempt.Result, agent.dispatched, attempt)
	}
}
