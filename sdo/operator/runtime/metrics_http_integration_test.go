//go:build integration

package runtime

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/sdk"
	"strings"
	"testing"
)

func newRealFakePrometheus(t *testing.T) (*fakePrometheus, *httptest.Server) {
	t.Helper()
	fake := &fakePrometheus{queries: map[string]string{}, hang: map[string]bool{}, release: make(chan struct{})}
	server := httptest.NewServer(fake)
	t.Cleanup(func() { close(fake.release); server.Close() })
	return fake, server
}

func TestEvaluateOnceWithMetricsURLReportsAFiringThreshold(t *testing.T) {
	fake, server := newRealFakePrometheus(t)
	fake.queries[errorQuery.Expr] = vector(`{"metric":{"deployment":"checkout","namespace":"shop"},"value":[1790856000,"0.4"]}`)
	output, err := evaluateOnceWithMetrics(t, []sdk.Detector{metricsDetector("thresholds", errorQuery)}, "--metrics-url", server.URL)
	if err != nil {
		t.Fatal(err)
	}
	var evaluation struct {
		Status   string        `json:"status"`
		Findings []sdk.Finding `json:"findings"`
	}
	if err := json.Unmarshal([]byte(output), &evaluation); err != nil {
		t.Fatalf("decode %q: %v", output, err)
	}
	if evaluation.Status != "firing" || len(evaluation.Findings) != 1 || evaluation.Findings[0].PrimaryResource.Name != "checkout" ||
		evaluation.Findings[0].Metadata["query_id"] != "error-rate" {
		t.Fatalf("evaluation: %s", output)
	}
}

func TestEvaluateOnceWithMetricsURLReportsClearOnlyWhenTheQueryAnswered(t *testing.T) {
	fake, server := newRealFakePrometheus(t)
	fake.queries[errorQuery.Expr] = vector(`{"metric":{"deployment":"checkout"},"value":[1790856000,"0.01"]}`)
	output, err := evaluateOnceWithMetrics(t, []sdk.Detector{metricsDetector("thresholds", errorQuery)}, "--metrics-url", server.URL)
	if err != nil || !strings.Contains(output, `"status":"clear"`) {
		t.Fatalf("an answered healthy query is clear: %q %v", output, err)
	}
}

func TestEvaluateOnceFailsClosedWhenTheMetricsSourceIsUnavailable(t *testing.T) {
	fake, server := newRealFakePrometheus(t)
	fake.status = http.StatusServiceUnavailable
	output, err := evaluateOnceWithMetrics(t, []sdk.Detector{metricsDetector("thresholds", errorQuery)}, "--metrics-url", server.URL)
	if err == nil || !strings.Contains(err.Error(), "unavailable") || strings.Contains(output, `"clear"`) {
		t.Fatalf("an unavailable source must fail evaluation, not clear it: %q %v", output, err)
	}
	if _, err := evaluateOnceWithMetrics(t, []sdk.Detector{alertReader{spec: metricsSpec("alerts")}}, "--metrics-url", server.URL); err == nil {
		t.Fatal("unavailable alerts must fail evaluation")
	}
}
