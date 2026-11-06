package runtime

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func telemetryOf(id string) *contracts.TelemetryContext {
	return &contracts.TelemetryContext{Mode: "open", Sources: []*contracts.TelemetrySource{{Id: id, Kinds: []string{"metrics"}}}}
}

func TestIncidentRequestsCarryTheActiveRevisionsTelemetry(t *testing.T) {
	config := testControllerConfig()
	config.Telemetry = telemetryOf("launch")
	c, err := NewController(config, nil, staticProvider{}, &recordingDispatcher{}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.incidentRequest(time.Unix(1, 0), nil).Telemetry.GetSources()[0].GetId(); got != "launch" {
		t.Fatalf("incident telemetry source = %q, want launch", got)
	}
	if err := c.ReconfigureMemory(&RemoteMemory{}, staticProvider{}, time.Unix(2, 0)); err != nil {
		t.Fatal(err)
	}
	if got := c.incidentRequest(time.Unix(3, 0), nil).Telemetry.GetSources()[0].GetId(); got != "launch" {
		t.Fatalf("a revision that reports no context must keep the previous one, got %q", got)
	}
	if err := c.ReconfigureMemory(&RemoteMemory{Telemetry: telemetryOf("swapped")}, staticProvider{}, time.Unix(4, 0)); err != nil {
		t.Fatal(err)
	}
	request := c.incidentRequest(time.Unix(5, 0), nil)
	if got := request.Telemetry.GetSources()[0].GetId(); got != "swapped" {
		t.Fatalf("incident telemetry source = %q after the swap, want swapped", got)
	}
	projected, err := incidentRequestContract(request)
	if err != nil || projected.GetTelemetry().GetSources()[0].GetId() != "swapped" {
		t.Fatalf("contract projection lost the telemetry: %v, %v", projected.GetTelemetry(), err)
	}
	cloned := cloneIncidentRequest(&request)
	cloned.Telemetry.Mode = "floor"
	if request.Telemetry.GetMode() != "open" || !proto.Equal(request.Telemetry, c.config.Telemetry) {
		t.Fatal("the stored copy of an incident request must not alias the controller's telemetry context")
	}
}
