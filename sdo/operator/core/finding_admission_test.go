package core

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

type kindlessDetector struct{}

func (kindlessDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "kindless", Class: sdk.DetectorClassLearned, Owner: sdk.DetectorOwnerResponder, Interval: time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}}
}

func (kindlessDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return []sdk.Finding{{RuleID: "bad-members", Fingerprint: "probe-call/call-read", Status: sdk.FindingActive,
		Severity: sdk.SeverityCritical, Summary: "bad members", Evidence: "synthetic"}}, nil
}

// A learned detector whose finding the controller would refuse is named, per
// capture, with the controller's own reason, so memory check rejects it at
// publication instead of the controller rejecting it every cycle.
func TestFindingAdmissionViolationsUseTheControllersValidation(t *testing.T) {
	violations := FindingAdmissionViolations(kindlessDetector{}, []NamedSnapshot{{Name: "open@detect.json", View: nil}})
	if len(violations) != 1 || !strings.Contains(violations[0], "primary resource: kind is required") || !strings.Contains(violations[0], "open@detect.json") {
		t.Fatalf("violations = %q", violations)
	}
}
