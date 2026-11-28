package appmap

import "testing"

const instrumented = `# HELP http_requests_total Total number of requests by method, status and handler.
# TYPE http_requests_total counter
http_requests_total{handler="/api/items/{id}",method="GET",status="2xx"} 12.0
http_requests_total{handler="/api/items",method="POST",status="5xx"} 1.0
# HELP http_request_duration_seconds Latency.
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_count{handler="/api/items/{id}",method="GET"} 12.0
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 3.1
`

// A Prometheus request counter with route, method and status labels is
// mapped by Go: the counter, its labels, and 5xx statuses as errors.
func TestRequestCounterFindsTheCounterWithRouteMethodAndStatusLabels(t *testing.T) {
	entry, ok := RequestCounter("api", []byte(instrumented))
	want := MetricEntry{Service: "api", Metric: "http_requests_total", Labels: EntryLabels{Route: "handler", Method: "method", Status: "status"}, Errors: "5.."}
	if !ok || entry.Service != want.Service || entry.Metric != want.Metric || entry.Labels != want.Labels || entry.Errors != want.Errors {
		t.Fatalf("entry = %+v, %t; want %+v", entry, ok, want)
	}
}

// OpenMetrics names the counter family without _total; a histogram's count
// is taken only when no counter carries a route.
func TestRequestCounterReadsOpenMetricsAndFallsBackToAHistogramCount(t *testing.T) {
	open := "# TYPE app_requests counter\napp_requests_total{route=\"/x\",code=\"200\"} 1\n"
	if entry, ok := RequestCounter("api", []byte(open)); !ok || entry.Metric != "app_requests_total" || entry.Labels.Status != "code" {
		t.Fatalf("open metrics: %+v %t", entry, ok)
	}
	histogram := "# TYPE http_server_requests_seconds histogram\nhttp_server_requests_seconds_count{uri=\"/x\",method=\"GET\",status=\"200\"} 3\n"
	if entry, ok := RequestCounter("api", []byte(histogram)); !ok || entry.Metric != "http_server_requests_seconds_count" || entry.Labels.Route != "uri" {
		t.Fatalf("histogram: %+v %t", entry, ok)
	}
}

// Without a route label Go cannot tell operations apart: nothing is mapped.
func TestRequestCounterNeedsARouteLabel(t *testing.T) {
	for _, text := range []string{
		"# TYPE http_requests_total counter\nhttp_requests_total{method=\"GET\",status=\"200\"} 1\n",
		"# TYPE jobs_total counter\njobs_total{handler=\"/x\"} 1\n",
		"",
		"not prometheus at all <html>",
	} {
		if entry, ok := RequestCounter("api", []byte(text)); ok {
			t.Fatalf("%q mapped as %+v", text, entry)
		}
	}
}
