package runtime

import (
	"context"
	"fmt"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"time"
)

func unexplainedFinding() sdk.Finding {
	event := sdk.DriftEvent{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Classification: sdk.DriftUnexplained}
	f := sdk.Finding{DetectorID: "drift.unexplained", RuleID: "unexplained", Status: sdk.FindingActive, Severity: sdk.SeverityLatent, PrimaryResource: event.Ref, Metadata: map[string]any{"drift_event": event}}
	f.Fingerprint = FindingFingerprint(f)
	return f
}

// Retained drift is reported next to a clear health verdict; it never decides it.
func TestVerifyReportsDriftWithoutDecidingHealth(t *testing.T) {
	f := unexplainedFinding()
	health := controllerDetector("health", time.Second, sdk.Finding{})
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident", Namespace: "app", Findings: []sdk.Finding{f}}, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{health}, PlanExecutor{}, 1)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: true}}
	}
	for _, command := range []string{"status", "diagnose", "verify"} {
		result := loop.Handle(context.Background(), ToolRequest{Command: command})
		if (command != "diagnose" && !result.Healthy) || len(result.Findings) != 0 || len(result.Drift) != 1 {
			t.Fatalf("%s hid retained drift or let it decide health: %+v", command, result)
		}
	}
}

type failedSourceDriftView struct{ driftReadView }

func (failedSourceDriftView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	return program.Diff{}, fmt.Errorf("source unavailable")
}

// A source that cannot be compared is information about the drift, never a
// verdict on health.
func TestUnexplainedDriftSourceErrorIsInformationOnly(t *testing.T) {
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident", Findings: []sdk.Finding{unexplainedFinding()}}, nil, staticProvider{snapshot: sdktest.Snapshot{}}, []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{})}, PlanExecutor{}, 1)
	loop.ReadView = func(sdk.DetectionContext) program.ReadView { return failedSourceDriftView{} }
	result := loop.Handle(context.Background(), ToolRequest{Command: "verify"})
	if !result.Healthy || len(result.Findings) != 0 || len(result.Drift) != 1 {
		t.Fatalf("drift decided health instead of informing: %+v", result)
	}
	if strings.Contains(result.Summary, "immediately") || !strings.Contains(result.Summary, "1 object(s) still differ from the accepted source") {
		t.Fatalf("verify told the agent to stop while a difference remains: %q", result.Summary)
	}
}

func TestUnexplainedDriftRetainsIndependentFieldSetsOnSameObject(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{})
	event := sdk.DriftEvent{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Classification: sdk.DriftUnexplained, Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort", New: "9999", Source: "8080"}}}
	c.queueUnexplainedFinding(start, event)
	event.Fields = []sdk.DriftField{{Path: "spec.selector.app", New: "wrong", Source: "api"}}
	c.queueUnexplainedFinding(start, event)
	pending := c.batcher.Snapshot().Findings
	if len(pending) != 2 || FindingFingerprint(pending[0]) == FindingFingerprint(pending[1]) {
		t.Fatal("independent retained field sets collapsed into one finding")
	}
}

// stepHealthIncident fires the health finding at start+2s and lets its 500ms
// debounce pass at start+3s, still inside the drift batch window.
func stepHealthIncident(t *testing.T, c *Controller, start time.Time) {
	t.Helper()
	for _, offset := range []time.Duration{2 * time.Second, 3 * time.Second} {
		if err := c.StepEvents(context.Background(), start.Add(offset), nil); err != nil {
			t.Fatal(err)
		}
	}
}

func TestUnexplainedDriftFindingUsesAcceptedSourceRestorationDefault(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
	if err != nil {
		t.Fatal(err)
	}
	event := sdk.DriftEvent{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}, Classification: sdk.DriftUnexplained}
	c.ConfigureDrift(DriftControllerConfig{})
	c.queueUnexplainedFinding(now, event)
	findings := exportState(t, c).PendingBatch.Findings
	if len(findings) != 1 {
		t.Fatalf("lost drift evidence: %+v", findings)
	}
	finding := findings[0]
	if !strings.Contains(finding.Summary, "restore accepted-source differences") || !strings.Contains(finding.Summary, "positive evidence") {
		t.Fatalf("finding still requires impact before restoration: %s", finding.Summary)
	}
	if finding.PrimaryResource.Name != event.Ref.Name || finding.DetectorId != "drift.unexplained" {
		t.Fatalf("changed drift identity: %+v", finding)
	}
}

