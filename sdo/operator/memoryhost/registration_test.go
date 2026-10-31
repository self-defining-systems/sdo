package memoryhost

import (
	"context"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/usage"
	"testing"
)

type inputDetector struct{ testDetector }

func (inputDetector) MetricQueries() []metrics.Query {
	return []metrics.Query{{ID: "load", Expr: "up"}}
}
func (inputDetector) LogQueries() []logs.Query { return []logs.Query{{ID: "errors"}} }
func (inputDetector) UsageRequirements() usage.Requirements {
	return usage.Requirements{Throttling: true}
}
func (inputDetector) ConsumesAlerts() bool { return true }
func TestRegistrationRetainsOptionalInputs(t *testing.T) {
	s, err := NewServer("c", "d", []sdk.Detector{inputDetector{}})
	if err != nil {
		t.Fatal(err)
	}
	ready, _ := s.Ready(context.Background(), nil)
	r, err := DecodeRegistration(ready.Detectors[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Metrics) != 1 || r.Metrics[0].Expr != "up" || len(r.Logs) != 1 || !r.Usage.Throttling || !r.Alerts {
		t.Fatalf("lost optional declarations: %+v", r)
	}
}
