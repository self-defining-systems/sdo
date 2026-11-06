//go:build integration

package operatorcli

import (
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/broker"
)

// TestMain runs this test binary as the broker's validation subprocess when
// invoked as one, exactly as the operator's main does.
func TestMain(m *testing.M) {
	broker.RunValidationChild(onboardingTestRegistry())
	os.Exit(m.Run())
}

// The onboarding invariant behind the live onboarding failures: for every
// built-in telemetry profile, and a profile that provides a source without
// alerts, the health judge's proposal (its detectors and probes, plus Go's
// generic bootstrap) is accepted by the exact broker the lifecycle judge path
// uses, through its real validation subprocess, with no telemetry diagnostic;
// and a valid judge-authored binding is accepted the same way. Before, the
// subprocess ran without the policy and registry: the Go-installed alerts
// detector was unbound and any binding failed "no adapter registry".
// Each case builds and tests the diagnostics module (about 20 s), so the
// test runs in the nightly tier.
func TestOnboardingBrokerAcceptsTheJudgesProposalUnderEveryProfile(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles the diagnostics module once per profile (about 15 s each); nightly CI runs it without -short")
	}
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	runOnboardingCases(t, (broker.SubprocessValidator{OperatorRoot: operatorRoot}).Validator())
}
