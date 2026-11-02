package runtime

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type waitEntryContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *waitEntryContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func earlyWaitController(t *testing.T, samples ...sdk.Finding) (*Controller, *blockingDispatcher, func(int)) {
	t.Helper()
	health := controllerDetector("health", time.Second, samples...)
	health.spec.Class, health.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	health.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 1}
	health.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	health.spec.OriginatingCommit = "health-objective"
	d := &blockingDispatcher{requests: make(chan IncidentRequest, 1), release: make(chan struct{})}
	t.Cleanup(func() { close(d.release) })
	c, err := NewController(testControllerConfig(), []sdk.Detector{health}, staticProvider{snapshot: sdktest.Snapshot{}}, d, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	step := func(i int) {
		t.Helper()
		if err := c.Step(context.Background(), time.Unix(int64(i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	step(0)
	step(1)
	executePendingEffect(t, c)
	awaitRequest(t, d.requests)
	return c, d, step
}

func TestPostClosureReadPayloadIsNormalPlusWarning(t *testing.T) {
	for _, command := range []string{"detect", "status"} {
		t.Run(command, func(t *testing.T) {
			gate, err := apihistory.PrepareMutationGate(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fixed := time.Unix(42, 0).UTC()
			loop := NewResponderLoop(IncidentRequest{Namespace: "app"}, nil, staticProvider{snapshot: sdktest.Snapshot{}}, []sdk.Detector{loopDetector{}}, PlanExecutor{}, 3)
			loop.Now = func() time.Time { return fixed }
			loop.MutationGate = gate
			server := SessionToolServer{Directory: filepath.Join(t.TempDir(), "tools"), Handle: loop.Handle}
			if err := server.Prepare(); err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			normal := exchangeSelfCheck(t, &server, `{"command":"`+command+`"}`)
			observation, err := gate.TryObserve()
			if err != nil {
				t.Fatal(err)
			}
			if err := observation.Fence(fixed); err != nil {
				t.Fatal(err)
			}
			observation.Close()
			after := exchangeSelfCheck(t, &server, `{"command":"`+command+`"}`)
			if after.IncidentVerified == nil {
				t.Fatal("warning missing")
			}
			after.IncidentVerified = nil
			expected, _ := json.Marshal(normal)
			actual, _ := json.Marshal(after)
			if string(expected) != string(actual) {
				t.Fatalf("normal output changed\n%s\n%s", expected, actual)
			}
			write := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{"never"}})
			if write.FailureReason != "incident_verified" || write.Receipt == nil || write.Receipt.GetFailureReason() != "incident_verified" || write.IncidentVerified == nil {
				t.Fatalf("untyped rejection: %+v", write)
			}
		})
	}
}
