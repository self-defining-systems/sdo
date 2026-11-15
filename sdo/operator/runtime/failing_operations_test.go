package runtime

import (
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/traffic"
)

const longRoute = "GET /api/:version/projects/:id/packages/generic/:package_name/*package_version/(*path/):file_name"

// routeFailing is a request-metrics error on a route no user test covers: the
// operation users actually fail on.
func routeFailing(route string, failed, total float64, samples []map[string]string) sdk.Finding {
	return sdk.Finding{
		DetectorID: "request-metrics", RuleID: requestmetrics.RuleErrors, Status: sdk.FindingActive,
		Summary:         "operation undeclared route " + route + ": 12 of 24 requests failed in 30s (client-error-jump)",
		PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"},
		Metadata: map[string]any{"operation": "undeclared route " + route, "route": route, "undeclared_route": true,
			"failed": failed, "total": total, "share": failed / total, "window": "30s", "class": "client-error-jump", "samples": samples},
	}
}

// blipApp has one probe scenario with a single failed sample long ago that
// passes again: the routine-rollout blip.
func blipApp() detectFake {
	view := detectApp()
	window := view.Traffic["health"]
	for i := range window.Scenarios {
		if window.Scenarios[i].Scenario.ID != "list" {
			continue
		}
		s := &window.Scenarios[i].Samples[3]
		s.Outcome, s.Status = traffic.OutcomeError, 502
	}
	view.Traffic["health"] = window
	return view
}

func failingNowRequest(finding sdk.Finding) IncidentRequest {
	request := detectRequest()
	request.Findings = []sdk.Finding{finding}
	request.OpeningKeys = []string{findingKey(finding)}
	return request
}

// What the responder sees is what fails for users now: the failing route with
// its own counts and samples leads, and a blip that passes again is shown as
// cleared, after it.
func TestDetectSummaryLeadsWithTheFailingRouteAndClearsTheBlip(t *testing.T) {
	finding := routeFailing(longRoute, 12, 24, []map[string]string{{"method": "GET", "path": "/api/v4/projects/9/packages/generic/p/1.0.0/a.txt", "status": "404"}})
	request := failingNowRequest(finding)
	text := buildDetectSummary(detectInput{Request: request, Findings: request.Findings, View: blipApp(), Now: detectNow()}).Text
	for _, want := range []string{"12/24", "50%", "file_name", "GET /api/v4/projects/9/packages/generic/p/1.0.0/a.txt -> 404", "cleared: list"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q:\n%s", want, text)
		}
	}
	if route, blip := strings.Index(text, "12/24"), strings.Index(text, "cleared: list"); route > blip {
		t.Fatalf("the failing route must come before the cleared blip:\n%s", text)
	}
	if strings.Contains(text, "opened on: Kubernetes condition") || strings.Contains(text, "kubectl -n app describe service") {
		t.Fatalf("a request error is not a Kubernetes condition:\n%s", text)
	}
	if strings.Contains(text, "(also failing)") {
		t.Fatalf("a blip that passes again is cleared, not failing:\n%s", text)
	}
}

// The first message carries the same view, not only the blip's test.
func TestFirstMessageNamesTheFailingRoute(t *testing.T) {
	finding := routeFailing(longRoute, 12, 24, nil)
	request := failingNowRequest(finding)
	block := strings.Join(incidentCandidateBlock(incidentPageInput{Request: request, Findings: request.Findings, View: blipApp(), Now: detectNow()}), "\n")
	for _, want := range []string{"12/24", "file_name", "cleared: list"} {
		if !strings.Contains(block, want) {
			t.Fatalf("missing %q:\n%s", want, block)
		}
	}
	if len(block) > candidateRenderCap+200 {
		t.Fatalf("block is %d bytes", len(block))
	}
}

// A request error that joined later (not an opening alert) still reaches the
// page, with the opening alert's operation first and the rest by share failing.
func TestJoinedRequestErrorsReachThePageAfterTheOpeningOne(t *testing.T) {
	opening := routeFailing("GET /a", 2, 20, nil)
	opening.Fingerprint = "a"
	joined := routeFailing("GET /b", 18, 20, nil)
	joined.Fingerprint = "b"
	request := detectRequest()
	request.Findings = []sdk.Finding{opening, joined}
	request.OpeningKeys = []string{findingKey(opening)}
	text := buildDetectSummary(detectInput{Request: request, Findings: request.Findings, View: detectApp(), Now: detectNow()}).Text
	b, a := strings.Index(text, "GET /b"), strings.Index(text, "GET /a")
	if b < 0 || a < 0 || a > b {
		t.Fatalf("opening route first, joined route listed (a before b):\n%s", text)
	}
}
