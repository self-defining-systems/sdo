package prober

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/journeycheck"
	"sdo.dev/operator/sdk/traffic"
)

// bodyClient answers every request with the same body.
type bodyClient string

func (b bodyClient) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(string(b))), Request: request}, nil
}

// The installed prober runs the full journey checks of its committed health
// probes with its own transport: three passes, a second prober and altered
// answers. A probe that checks the answer's content is green; one that would
// pass on a wrong answer is red, so the guarantee that probes fail on wrong
// answers holds after the session as it did inside it.
func TestInstalledProberRunsTheFullJourneyChecksOfItsHealthProbes(t *testing.T) {
	read := func(id string, endpoint *traffic.HTTPEndpoint) traffic.Scenario {
		return traffic.Scenario{ID: id, Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead,
			Steps: []traffic.Step{{Name: "get", Endpoint: endpoint}}, Detects: []traffic.FaultClass{traffic.FaultWrongBody}}
	}
	catalog := traffic.Catalog{read("checks-content", traffic.GET("/items", nil).Contains("sdoprobe-ready")), read("checks-status", traffic.GET("/status", nil))}
	workload := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "probe-call", Purpose: traffic.PurposeHealthProbe,
		Scenarios: []traffic.WorkloadScenario{{ID: "checks-content"}, {ID: "checks-status"}}}
	clock := &stepClock{now: time.Date(2026, 11, 28, 9, 0, 0, 0, time.UTC)}
	p, err := New(Config{Namespace: "app", Catalog: catalog, Workloads: []traffic.Workload{workload}, Client: bodyClient(`["sdoprobe-ready"]`),
		BaseURL: func(traffic.Target) string { return "http://web:80" }, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	report := p.JourneyCheck(context.Background())
	status := map[string]string{}
	for _, result := range report.Results {
		status[result.Item+" "+result.Subject] = result.Status
	}
	for key, want := range map[string]string{
		"passes checks-content": journeycheck.Green, "concurrent checks-content": journeycheck.Green,
		"altered-answers checks-content": journeycheck.Green, "altered-answers checks-status": journeycheck.Red,
	} {
		if status[key] != want {
			t.Fatalf("%s = %q, want %q; report %+v", key, status[key], want, report.Results)
		}
	}
}
