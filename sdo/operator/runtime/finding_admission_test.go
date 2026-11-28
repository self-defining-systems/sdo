package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// unadmittedDetector is a learned detector whose finding names no resource
// kind: the controller rejects it, so nothing may target it.
type unadmittedDetector struct{}

func (unadmittedDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "kindless", Class: sdk.DetectorClassLearned, Owner: sdk.DetectorOwnerResponder,
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}, OriginatingCommit: "synthetic"}
}

func (unadmittedDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return []sdk.Finding{{DetectorID: "kindless", RuleID: "kindless", Fingerprint: "probe-call/call-read", Status: sdk.FindingActive,
		Severity: sdk.SeverityCritical, Summary: "bad members", Evidence: "synthetic"}}, nil
}

// Every runtime detector read admits findings as the controller does, so the
// stage and the responder's tools never see, bind to or target a finding the
// controller rejected. The detector shows as failing with the controller's
// reason; the other detectors' findings stay.
func TestDetectorReadsAdmitOnlyWhatTheControllerAdmits(t *testing.T) {
	results := evaluateAdmittedDetectors(context.Background(), []sdk.Detector{unadmittedDetector{}}, sdktest.Snapshot{})
	if got := results["kindless"]; len(got.findings) != 0 || got.err == nil || !strings.Contains(got.err.Error(), "kind is required") {
		t.Fatalf("unadmitted finding read as %+v", got)
	}
	probe := &switchedProbe{id: "traffic-health", scenario: "browse"}
	probe.failing.Store(true)
	request := externalGoldenRequest(t)
	request.Findings, request.DetectorHistory = nil, nil
	loop := NewResponderLoop(request, nil, staticProvider{}, nil, PlanExecutor{}, 2)
	loop.Detectors = []sdk.Detector{unadmittedDetector{}, probe}
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView { return program.ReadFrom(snapshot) }
	for _, command := range []string{"detect", "status", "diagnose"} {
		response := loop.Handle(context.Background(), ToolRequest{Command: command})
		for _, finding := range append(response.Findings, loop.findings...) {
			if finding.DetectorID == "kindless" {
				t.Fatalf("%s kept an unadmitted finding: %+v", command, response.Findings)
			}
		}
		if len(response.Findings) != 1 || response.Findings[0].DetectorID != "traffic-health" {
			t.Fatalf("%s findings %+v, want only the probe's (error %q)", command, response.Findings, response.Error)
		}
	}
}
