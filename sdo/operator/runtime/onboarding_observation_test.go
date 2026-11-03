package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestOnboardingIncompleteHealthIsNotContradiction(t *testing.T) {
	d := controllerDetector("health-objective", time.Second, sdk.Finding{}, sdk.Finding{RuleID: "unhealthy", Status: sdk.FindingActive})
	d.spec.Class = sdk.DetectorClassHealth
	d.errors[0] = errors.New("observation unavailable")
	c := &Controller{onboardingReachability: &onboardingReachabilityState{}, detectors: map[string]sdk.Detector{d.spec.ID: d}}
	if c.checkOnboardingHealth(context.Background(), sdktest.Snapshot{}) {
		t.Fatal("unavailable health authorized onboarding")
	}
	if c.onboardingReachability.invalid {
		t.Fatal("incomplete observation permanently rejected onboarding")
	}
	if !strings.Contains(c.onboardingReachability.waitReason, "observation unavailable") {
		t.Fatal("missing pending evidence")
	}
	if c.checkOnboardingHealth(context.Background(), sdktest.Snapshot{}) {
		t.Fatal("unhealthy observation authorized onboarding")
	}
	if !c.onboardingReachability.invalid || !strings.Contains(c.onboardingReachability.invalidReason, "health-objective") {
		t.Fatal("confirmed contradiction was not retained")
	}
}

type slowWindowDetector struct{ *sequenceDetector }

func (slowWindowDetector) EvaluationWindow(sdk.DetectionContext) time.Duration {
	return 90 * time.Second
}

func TestOnboardingExpiryAllowsConsumerWindowButRemainsBounded(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := slowWindowDetector{controllerDetector("health-objective", time.Second, sdk.Finding{})}
	d.spec.Class = sdk.DetectorClassHealth
	c := &Controller{pendingOnboarding: &contracts.NamespaceOnboarding{}, onboardingReachability: &onboardingReachabilityState{startedAt: start}, detectors: map[string]sdk.Detector{d.spec.ID: d}}
	if c.onboardingConfirmationExpired(start.Add(40*time.Second), sdktest.Snapshot{}) {
		t.Fatal("expired before consumer could observe its window")
	}
	if !c.onboardingConfirmationExpired(start.Add(6*time.Minute), sdktest.Snapshot{}) {
		t.Fatal("incomplete onboarding can wait forever")
	}
}
