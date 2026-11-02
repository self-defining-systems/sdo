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
func TestResponderVerifyWaitStablePassAndReactivation(t *testing.T) {
	for _, reactivate := range []bool{false, true} {
		name := "stable"
		if reactivate {
			name = "reactivation"
		}
		t.Run(name, func(t *testing.T) {
			samples := []sdk.Finding{stateFinding("health"), stateFinding("health"), {}, {}, {}}
			if reactivate {
				samples = append(samples, stateFinding("health"), stateFinding("health"))
			}
			c, _, step := earlyWaitController(t, samples...)
			step(2)
			id := c.currentIncidentRequest.IncidentID
			entered := &waitEntryContext{Context: context.Background(), entered: make(chan struct{})}
			result := make(chan ToolResponse, 1)
			go func() { result <- c.cachedResponderVerification(entered, id, time.Second) }()
			select {
			case <-entered.entered:
			case <-time.After(time.Second):
				t.Fatal("wait did not start")
			}
			end := 17
			if reactivate {
				end = 6
			}
			for i := 3; i <= end; i++ {
				step(i)
			}
			select {
			case response := <-result:
				if reactivate {
					if response.Healthy || len(response.Findings) != 1 || response.IncidentVerified != nil {
						t.Fatalf("reactivation: %+v", response)
					}
				} else if !response.Healthy || response.IncidentVerified == nil {
					t.Fatalf("stable: %+v", response)
				}
			case <-time.After(time.Second):
				t.Fatal("cached-state waiter did not wake")
			}
		})
	}
}
func TestResponderVerifyWaitTimeoutAndInstantCachedRead(t *testing.T) {
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"))
	step(2)
	id := c.currentIncidentRequest.IncidentID
	// A cached read must work even if a new snapshot would be unavailable.
	c.provider = nil
	before := time.Now()
	plain := c.cachedResponderVerification(context.Background(), id, 0)
	if time.Since(before) > 100*time.Millisecond || plain.Healthy {
		t.Fatalf("plain cached read: %+v", plain)
	}
	before = time.Now()
	response := c.cachedResponderVerification(context.Background(), id, 20*time.Millisecond)
	elapsed := time.Since(before)
	if elapsed < 20*time.Millisecond || elapsed > time.Second || response.Healthy || response.IncidentVerified != nil {
		t.Fatalf("timeout %v: %+v", elapsed, response)
	}
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
