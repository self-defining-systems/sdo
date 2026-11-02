package runtime

import (
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func verifiedAttempt(verdict contracts.Verdict_Result, passed bool) *contracts.StageMitigation {
	return &contracts.StageMitigation{
		ApplyReceipt:      &contracts.PlanApplication{Applied: true, VerificationPassed: passed},
		PostApplyDiagnose: &contracts.ProgramRun{Verdict: &contracts.Verdict{Result: verdict}},
	}
}

// Only a verified write whose program no longer matches may wait for detectors;
// a write the program still matches, or one that did not verify, goes to another actor.
func TestWriteVerifiedForSettling(t *testing.T) {
	if !writeVerifiedForSettling(verifiedAttempt(contracts.Verdict_RESULT_NO_MATCH, true)) {
		t.Fatal("verified write with a cleared match must be allowed to settle")
	}
	if writeVerifiedForSettling(verifiedAttempt(contracts.Verdict_RESULT_MATCH, true)) {
		t.Fatal("a program that still matches is not settling")
	}
	if writeVerifiedForSettling(verifiedAttempt(contracts.Verdict_RESULT_NO_MATCH, false)) {
		t.Fatal("an unverified write is not settling")
	}
	rolledBack := verifiedAttempt(contracts.Verdict_RESULT_NO_MATCH, true)
	rolledBack.ApplyReceipt.RolledBack = true
	if writeVerifiedForSettling(rolledBack) {
		t.Fatal("a rolled-back write is not settling")
	}
}

func TestPromoteSettledAttemptsRecordsTheClearedFindings(t *testing.T) {
	attempt := verifiedAttempt(contracts.Verdict_RESULT_NO_MATCH, true)
	attempt.Result = "applied_still_matching"
	attempt.ClearedFindingFingerprints = []string{"drift"}
	attempt.RemainingFindingFingerprints = []string{"reachability"}
	promoteSettledAttempts([]*contracts.StageMitigation{attempt})
	if attempt.Result != "applied_cleared" || len(attempt.RemainingFindingFingerprints) != 0 || len(attempt.ClearedFindingFingerprints) != 2 {
		t.Fatalf("not promoted: %+v", attempt)
	}
}
