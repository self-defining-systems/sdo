package requestmetrics_test

import (
	"testing"
	"time"

	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/sdktest"
	sdotraffic "sdo.dev/operator/sdk/traffic"
)

// withOwn adds SDO's own probe requests to a snapshot: n answers of status on
// a concrete path of the package route, one per second from start.
func withOwn(s sdktest.Snapshot, start time.Time, status int, n int) sdktest.Snapshot {
	window := s.Traffic["probe-call"]
	for i := range n {
		window.Sent = append(window.Sent, sdotraffic.SentRequest{Service: "web", Method: "GET",
			Path: "/api/v4/projects/7/packages/generic/sdoprobe/1.0.0/sdoprobe-x.txt", Status: status, At: start.Add(time.Duration(i) * time.Second)})
	}
	if s.Traffic == nil {
		s.Traffic = map[string]sdotraffic.Window{}
	}
	s.Traffic["probe-call"] = window
	return s
}

func packageFailures() []metrics.Series {
	return []metrics.Series{
		series(pkgRoute, "200", steady(11, 7)...),
		series(pkgRoute, "404", 0, 0, 0, 5, 8, 8, 8),
		series(webRoute, "200", steady(40, 7)...),
	}
}

// An operator's own traffic is never evidence that users are failing: when
// every failed request the application counted on an operation is one SDO's
// prober sent (its scenarios judge those), request metrics raise nothing.
func TestSDOsOwnFailingProbeRequestsAreNotUserFailures(t *testing.T) {
	s := withOwn(snapshot(packageFailures(), healthyBaseline(), presence()), t0.Add(12*time.Second), 404, 8)
	if got := detect(t, s); len(got) != 0 {
		t.Fatalf("SDO's own requests raised: %+v", got)
	}
}

// Users' failures beside SDO's own still raise, counted without SDO's.
func TestUsersFailuresBesideSDOsOwnStillRaise(t *testing.T) {
	s := withOwn(snapshot(packageFailures(), healthyBaseline(), presence()), t0.Add(12*time.Second), 404, 3)
	got := detect(t, s)
	if len(got) != 1 || got[0].Metadata["operation"] != "package-download" {
		t.Fatalf("findings = %+v", got)
	}
	if got[0].Metadata["failed"].(float64) != 5 || got[0].Metadata[requestmetrics.MetaOwnFailedExcluded] != 3.0 {
		t.Fatalf("want 5 users' failures judged and SDO's 3 named as excluded: %+v", got[0].Metadata)
	}
}

// SDO's requests on another service, method or route take nothing away.
func TestOwnRequestsOfOtherOperationsAreNotSubtracted(t *testing.T) {
	s := withOwn(snapshot(packageFailures(), healthyBaseline(), presence()), t0.Add(12*time.Second), 404, 8)
	window := s.Traffic["probe-call"]
	for i := range window.Sent {
		window.Sent[i].Path = "/api/v4/projects/7/issues/1"
	}
	s.Traffic["probe-call"] = window
	if got := detect(t, s); len(got) != 1 {
		t.Fatalf("findings = %+v", got)
	}
}
