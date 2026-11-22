package lifecycle

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/operability"
)

// At onboarding acceptance a map that needs existing records fails the
// designer when no marked step returned a record the probe did not write.
func TestAcceptanceRequiresAnExercisedExistingRecordsStep(t *testing.T) {
	clear := func(required bool, exercised int) HealthVerdict {
		return HealthVerdict{Outcome: VerificationClear, Summary: "clear", ExistingRecords: ExistingRecordsEvidence{Required: required, Exercised: exercised}}
	}
	failing := func(verdict HealthVerdict) *operability.Check {
		for _, check := range verdictChecks(verdict) {
			if check.ID == "O4.existing-records" && check.Status == operability.StatusFail {
				return &check
			}
		}
		return nil
	}
	check := failing(clear(true, 0))
	if check == nil || check.Cause != operability.CauseDetectorFault || len(check.Paths) == 0 ||
		!strings.Contains(check.Detail, "no marked step returned a record the probe did not write") ||
		!strings.Contains(check.Detail, "no_user_records: <why>") {
		t.Fatalf("own-records-only acceptance did not fail with the finding: %+v", check)
	}
	if failing(clear(true, 1)) != nil {
		t.Fatal("an exercised step still failed")
	}
	if failing(clear(false, 0)) != nil {
		t.Fatal("a map not needing records (escape or no data) failed")
	}
}

// A clear verdict whose existing-records step has not run yet is an incomplete
// observation, not a failed map: a controller that has just restarted (every
// head deploy rebuilds it for a judge refresh) evaluates before the probe's
// first existing-records sample, and failing that crash-looped the controller
// ("O4.existing-records ... still firing after 2 designer corrections"), so
// the push after it was never deployed. The window is waited out for the step
// to run; one that never runs still fails at its end.
func TestAnUnexercisedExistingRecordsStepIsWaitedOutBeforeItFails(t *testing.T) {
	verdict := func(exercised int) HealthVerdict {
		return HealthVerdict{Outcome: VerificationClear, Summary: "clear", ExistingRecords: ExistingRecordsEvidence{Required: true, Exercised: exercised}}
	}
	c := Coordinator{Wait: func(context.Context, time.Duration) error { return nil }}
	cfg := Config{PendingEvaluations: 4, PendingInterval: time.Second}

	answers, calls := []HealthVerdict{verdict(0), verdict(0), verdict(2)}, 0
	got, err := c.awaitVerdict(context.Background(), cfg, func() (HealthVerdict, error) {
		answer := answers[min(calls, len(answers)-1)]
		calls++
		return answer, nil
	})
	if err != nil || got.ExistingRecords.Exercised != 2 || calls != 3 {
		t.Fatalf("verdict %+v after %d evaluation(s), err %v: want to wait for the step to run", got, calls, err)
	}

	calls = 0
	got, err = c.awaitVerdict(context.Background(), cfg, func() (HealthVerdict, error) {
		calls++
		return verdict(0), nil
	})
	if err != nil || got.Outcome != VerificationClear || got.ExistingRecords.Exercised != 0 || calls != 4 {
		t.Fatalf("verdict %+v after %d evaluation(s), err %v: a step that never runs must still reach the check after the whole window", got, calls, err)
	}
}
