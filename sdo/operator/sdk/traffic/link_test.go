package traffic_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

func linkWorkload(t *testing.T, extra string) traffic.Workload {
	t.Helper()
	workload, err := traffic.ParseWorkloadJSON([]byte(`{
	  "apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "links", "purpose": "link-probe",
	  ` + extra + `
	  "links": [{"from": "frontend", "to": "recommendation", "port": 8085}, {"from": "frontend", "to": "user", "port": 8086}]
	}`))
	if err != nil {
		t.Fatalf("parse link workload: %v", err)
	}
	return workload
}

func linkSpec() sdk.DetectorSpec {
	spec := detectorSpec()
	spec.ID = "link-reachability"
	return spec
}

func linkOK(offset time.Duration) traffic.LinkSample {
	return traffic.LinkSample{At: observedAt.Add(-offset), Duration: time.Millisecond, OK: true}
}

func linkFailed(offset time.Duration) traffic.LinkSample {
	return traffic.LinkSample{At: observedAt.Add(-offset), Duration: time.Second, Error: "dial tcp 10.0.0.1:8085: i/o timeout"}
}

func linkWindow(workload traffic.Workload, recommendation []traffic.LinkSample, qualified bool) traffic.Window {
	return traffic.Window{
		Workload: workload, ObservedAt: observedAt,
		Links: []traffic.LinkObservations{
			{Link: workload.Links[0], Samples: recommendation, Qualified: qualified},
			{Link: workload.Links[1], Samples: []traffic.LinkSample{linkOK(3 * time.Second), linkOK(2 * time.Second), linkOK(time.Second)}, Qualified: true},
		},
	}
}

func detectLinks(t *testing.T, window traffic.Window) []sdk.Finding {
	t.Helper()
	snapshot := sdktest.Snapshot{NamespaceName: "hotel-reservation", Traffic: map[string]traffic.Window{"links": window}}
	findings, err := traffic.NewLinkDetector(linkSpec(), "links").Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	return findings
}

func TestLinkWorkloadDefaultsAndValidation(t *testing.T) {
	workload := linkWorkload(t, "")
	if workload.Failures != traffic.DefaultLinkFailures || workload.Interval.Duration() != traffic.DefaultLinkInterval ||
		workload.Timeout.Duration() != traffic.DefaultLinkTimeout {
		t.Fatalf("defaults: failures %d interval %s timeout %s", workload.Failures, workload.Interval.Duration(), workload.Timeout.Duration())
	}
	for name, document := range map[string]string{
		"no links":       `{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "links", "purpose": "link-probe"}`,
		"self link":      `{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "links", "purpose": "link-probe", "links": [{"from": "a", "to": "a", "port": 80}]}`,
		"bad port":       `{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "links", "purpose": "link-probe", "links": [{"from": "a", "to": "b", "port": 0}]}`,
		"duplicate link": `{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "links", "purpose": "link-probe", "links": [{"from": "a", "to": "b", "port": 80}, {"from": "a", "to": "b", "port": 80}]}`,
		"scenarios":      `{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "links", "purpose": "link-probe", "scenarios": [{"id": "x"}], "links": [{"from": "a", "to": "b", "port": 80}]}`,
		"links on probe": `{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "h", "purpose": "health-probe", "scenarios": [{"id": "x"}], "links": [{"from": "a", "to": "b", "port": 80}]}`,
		"too few":        `{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "links", "purpose": "link-probe", "failures": 1, "links": [{"from": "a", "to": "b", "port": 80}]}`,
	} {
		if _, err := traffic.ParseWorkloadJSON([]byte(document)); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

func TestLinkDetectorFiresAfterConsecutiveFailuresAndNamesTheEdge(t *testing.T) {
	workload := linkWorkload(t, "")
	window := linkWindow(workload, []traffic.LinkSample{
		linkOK(6 * time.Second), linkFailed(5 * time.Second), linkFailed(4 * time.Second), linkFailed(3 * time.Second),
		linkFailed(2 * time.Second), linkFailed(time.Second),
	}, true)
	findings := detectLinks(t, window)
	if len(findings) != 1 {
		t.Fatalf("findings: %+v", findings)
	}
	finding := findings[0]
	if finding.RuleID != "link-reachability.frontend.recommendation.8085" || finding.PrimaryResource.Name != "recommendation" ||
		finding.PrimaryResource.Kind != "Service" || finding.PrimaryResource.Namespace != "hotel-reservation" {
		t.Fatalf("finding: %+v", finding)
	}
	if len(finding.RelatedResources) != 1 || finding.RelatedResources[0].Name != "frontend" {
		t.Fatalf("related: %+v", finding.RelatedResources)
	}
	if !strings.Contains(finding.Summary, "frontend") || !strings.Contains(finding.Summary, "recommendation:8085") ||
		!strings.Contains(finding.Evidence, "i/o timeout") {
		t.Fatalf("summary %q evidence %q", finding.Summary, finding.Evidence)
	}
	if finding.Status != sdk.FindingActive {
		t.Fatalf("status %s", finding.Status)
	}
}

func TestLinkDetectorStaysQuietWhenHealthy(t *testing.T) {
	workload := linkWorkload(t, "")
	window := linkWindow(workload, []traffic.LinkSample{linkOK(3 * time.Second), linkOK(2 * time.Second), linkOK(time.Second)}, true)
	if findings := detectLinks(t, window); len(findings) != 0 {
		t.Fatalf("findings on a healthy link: %+v", findings)
	}
}

func TestLinkDetectorHysteresis(t *testing.T) {
	workload := linkWorkload(t, "")
	cases := map[string][]traffic.LinkSample{
		"fewer failures than the threshold": {linkOK(5 * time.Second), linkFailed(4 * time.Second), linkFailed(3 * time.Second), linkFailed(2 * time.Second), linkFailed(time.Second)},
		"a success inside the run":          {linkFailed(5 * time.Second), linkFailed(4 * time.Second), linkOK(3 * time.Second), linkFailed(2 * time.Second), linkFailed(time.Second)},
		"recovered":                         {linkFailed(5 * time.Second), linkFailed(4 * time.Second), linkFailed(3 * time.Second), linkFailed(2 * time.Second), linkOK(time.Second)},
		"no samples yet":                    nil,
	}
	for name, samples := range cases {
		if findings := detectLinks(t, linkWindow(workload, samples, true)); len(findings) != 0 {
			t.Errorf("%s: unexpected findings %+v", name, findings)
		}
	}
}

func TestLinkDetectorIgnoresLinksNeverReachable(t *testing.T) {
	// An edge the prober has never reached is not evidence of a change: the
	// prober's own identity may simply not be admitted to the target.
	workload := linkWorkload(t, "")
	failures := []traffic.LinkSample{linkFailed(5 * time.Second), linkFailed(4 * time.Second), linkFailed(3 * time.Second), linkFailed(2 * time.Second), linkFailed(time.Second)}
	if findings := detectLinks(t, linkWindow(workload, failures, false)); len(findings) != 0 {
		t.Fatalf("findings on a never-qualified link: %+v", findings)
	}
}

func TestLinkDetectorIgnoresStaleSamples(t *testing.T) {
	workload := linkWorkload(t, "")
	stale := []traffic.LinkSample{linkFailed(5 * time.Minute), linkFailed(5*time.Minute + time.Second), linkFailed(5*time.Minute + 2*time.Second),
		linkFailed(5*time.Minute + 3*time.Second), linkFailed(5*time.Minute + 4*time.Second)}
	if findings := detectLinks(t, linkWindow(workload, stale, true)); len(findings) != 0 {
		t.Fatalf("findings from stale samples: %+v", findings)
	}
}

func TestLinkDetectorHonoursConfiguredThreshold(t *testing.T) {
	workload := linkWorkload(t, `"failures": 2,`)
	window := linkWindow(workload, []traffic.LinkSample{linkOK(3 * time.Second), linkFailed(2 * time.Second), linkFailed(time.Second)}, true)
	if findings := detectLinks(t, window); len(findings) != 1 {
		t.Fatalf("findings: %+v", findings)
	}
}

func TestLinkDetectorDoesNotApplyTheScenarioMinDuration(t *testing.T) {
	// The consecutive-failure count already spans several probe intervals.
	spec := linkSpec()
	detector := traffic.NewLinkDetector(spec, "links")
	if detector.Spec().Persistence.MinDuration != 0 {
		t.Fatalf("link detector min duration %s", detector.Spec().Persistence.MinDuration)
	}
	consumer, ok := detector.(traffic.Consumer)
	if !ok || len(consumer.TrafficWorkloads()) != 1 || consumer.TrafficWorkloads()[0] != "links" {
		t.Fatalf("link detector must consume the links workload")
	}
}

func TestLinkDetectorReportsUnloadableWindow(t *testing.T) {
	snapshot := sdktest.Snapshot{NamespaceName: "ns", Traffic: map[string]traffic.Window{"links": {LoadError: "prober unavailable"}}}
	if _, err := traffic.NewLinkDetector(linkSpec(), "links").Detect(context.Background(), snapshot); err == nil {
		t.Fatalf("expected the load error")
	}
}
