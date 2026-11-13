package lifecycle

import (
	"strings"
	"testing"

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
