package memory

import "testing"

// A service only background work enters (internal operations) is planned as a
// background entry; one a user operation enters is not.
func TestRequestMetricsEntryOfBackgroundWorkIsMarked(t *testing.T) {
	root := t.TempDir()
	writeEntriesMap(t, root, `operations:
  - {id: list-items, entry: {service: web, port: 80, method: GET, path: /items}, path: [web]}
  - {id: send-mail, entry: {service: worker, path: mailers}, path: [worker]}
entries:
  - {service: web, metric: http_requests_total, labels: {route: route, method: method, status: code}, errors: "5.."}
  - {service: worker, metric: jobs_total, labels: {route: queue, status: code}, errors: "5.."}
`)
	cfg, ok := plannedRequestMetrics(root)
	if !ok {
		t.Fatal("not planned")
	}
	got := map[string]bool{}
	for _, e := range cfg.Entries {
		got[e.Service] = e.Background
	}
	if got["web"] || !got["worker"] {
		t.Fatalf("background by service = %v, want only worker", got)
	}
}
