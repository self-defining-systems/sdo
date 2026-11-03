package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestDispatchTimestampUsesExecutionClock(t *testing.T) {
	detected := time.Unix(100, 0).UTC()
	started := detected.Add(10 * time.Second)
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	request := IncidentRequest{IncidentID: "synthetic", ResponseDeadline: started.Add(time.Minute)}
	c, err := NewController(testControllerConfig(), nil, staticProvider{}, dispatcher, detected)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return started }
	c.incidentOpen = true
	c.incidentDetectedAt = detected
	c.currentIncidentRequest = &request
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
	if err := c.ExecuteDispatchEffect(context.Background(), DispatchEffect{Request: request}); err != nil {
		t.Fatal(err)
	}
	awaitDispatchCompletionQueued(t, c)
	if !c.incidentDispatchedAt.Equal(started) {
		t.Fatalf("dispatch = %s; want execution time %s", c.incidentDispatchedAt, started)
	}
	stored, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	if !stored.GetIncidentDispatchedAt().AsTime().Equal(started) {
		t.Fatal("dispatch timestamp lost from persisted state")
	}
}

func TestDeterministicStageRecordsAndRestoresStart(t *testing.T) {
	started := time.Unix(110, 0).UTC()
	loop := NewResponderLoop(IncidentRequest{IncidentID: "synthetic"}, nil, staticProvider{}, nil, PlanExecutor{}, 2)
	loop.Now = func() time.Time { return started }
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true}
	state := &IncidentState{Request: loop.Request}
	for i := 0; i < 2; i++ {
		report, err := stage.Run(context.Background(), state)
		if err != nil {
			t.Fatal(err)
		}
		data, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded["started_at"] != started.Format(time.RFC3339) {
			t.Fatalf("stage start not recorded/preserved: %s", data)
		}
		loop.Now = func() time.Time { return started.Add(time.Minute) }
	}
}
