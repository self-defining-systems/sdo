package runtime

import (
	"context"
	"errors"
	"testing"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type boundedVerifyTraffic struct {
	ProberAPI
	request prober.BurstRequest
	failure error
	healthy bool
}

func (v *boundedVerifyTraffic) Burst(_ context.Context, r prober.BurstRequest) (prober.BurstResult, error) {
	v.request = r
	return prober.BurstResult{Healthy: v.healthy}, v.failure
}
func TestResponderBoundedVerificationUsesIncidentTraffic(t *testing.T) {
	for _, test := range []struct {
		name    string
		healthy bool
		failure error
	}{{"healthy", true, nil}, {"unhealthy", false, nil}, {"unavailable", false, errors.New("host unavailable")}} {
		t.Run(test.name, func(t *testing.T) {
			traffic := &boundedVerifyTraffic{healthy: test.healthy, failure: test.failure}
			loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app"}, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{loopDetector{}}, PlanExecutor{}, 5)
			loop.ReadView = func(s sdk.DetectionContext) program.ReadView {
				return &sdktest.ReplayReadView{Snapshot: s.(sdktest.Snapshot)}
			}
			loop.Traffic = traffic
			response := loop.Handle(context.Background(), ToolRequest{Command: "verify", Workload: "bounded", Scenarios: []string{"read"}})
			if traffic.request.Workload != "bounded" || len(traffic.request.Scenarios) != 1 {
				t.Fatal("selection did not reach host", traffic.request)
			}
			if response.Healthy != test.healthy || (response.Error != "") != (test.failure != nil) {
				t.Fatalf("verification did not fail closed: %+v", response)
			}
		})
	}
}
