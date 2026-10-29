package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func replayReady(t *testing.T, c *Controller) bool {
	t.Helper()
	state, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := protojson.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	return fields["healthyReplayReady"] == true
}

func TestHealthyReplayReadinessRequiresCurrentProcessCapture(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(100, 0).UTC()
	for _, failCapture := range []bool{false, true} {
		t.Run(map[bool]string{false: "restart", true: "capture-failure"}[failCapture], func(t *testing.T) {
			root := t.TempDir()
			config := testControllerConfig()
			config.CorpusSessionRoot = root
			makeController := func(at time.Time) *Controller {
				c, err := NewController(config, []sdk.Detector{controllerDetector("health", time.Hour, sdk.Finding{}, sdk.Finding{})}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: config.Namespace}}, &recordingDispatcher{}, at)
				if err != nil {
					t.Fatal(err)
				}
				return c
			}
			c := makeController(start)
			if replayReady(t, c) {
				t.Fatal("ready before a healthy capture")
			}
			runtimeRoot := filepath.Join(root, ".sdo-runtime")
			if failCapture {
				if err := os.WriteFile(runtimeRoot, []byte("synthetic obstruction"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.EvaluateAll(ctx, start); err != nil {
				t.Fatal(err)
			}
			if failCapture {
				if replayReady(t, c) {
					t.Fatal("failed capture published readiness")
				}
				if err := os.Remove(runtimeRoot); err != nil {
					t.Fatal(err)
				}
				if err := c.EvaluateAll(ctx, start.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			if !replayReady(t, c) {
				t.Fatal("successful healthy capture did not publish readiness")
			}
			if err := c.bindIncidentHealthyBaseline("synthetic-before"); err != nil {
				t.Fatal(err)
			}
			state, err := c.ExportState()
			if err != nil {
				t.Fatal(err)
			}
			restarted := makeController(start.Add(2 * time.Second))
			if err := restarted.RestoreState(state); err != nil {
				t.Fatal(err)
			}
			if replayReady(t, restarted) {
				t.Fatal("restored readiness without this process's baseline")
			}
			if err := restarted.Step(ctx, start.Add(2*time.Second), nil); err != nil {
				t.Fatal(err)
			}
			if !replayReady(t, restarted) {
				t.Fatal("restarted controller never became ready")
			}
			if err := restarted.bindIncidentHealthyBaseline("synthetic-after"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHealthyReplayReconcilesDriftAfterItsIncidentClosed(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "same-process", true: "restart"}[restart], func(t *testing.T) {
			f := newClosureDriftFixture(t, true, true, "app")
			f.settle(t, 1)
			if len(f.closed) != 1 {
				t.Fatal("fixture did not close its healthy incident")
			}
			if err := f.c.SetIncidentBroker(&recordingIncidentBroker{receipt: ClosureReceipt{OutcomeCommit: "outcome", AckToken: "ack"}}); err != nil {
				t.Fatal(err)
			}
			acknowledgeScenario(t, f.c)
			f.c.config.CorpusSessionRoot = t.TempDir()
			if restart {
				state, err := f.c.ExportState()
				if err != nil {
					t.Fatal(err)
				}
				after, err := NewController(f.c.config, []sdk.Detector{closureImageHealth{}}, fastloopObjects{f.client}, f.c.dispatcher, f.start.Add(20*time.Second))
				if err != nil {
					t.Fatal(err)
				}
				if err := after.RestoreState(state); err != nil {
					t.Fatal(err)
				}
				after.ConfigureDrift(*f.c.drift)
				f.c = after
			}
			f.restoreSource(t)
			f.step(t, 30)
			if !replayReady(t, f.c) {
				t.Fatalf("restored drift stranded baseline: open=%v blocked=%v quiet=%v complete=%v ref=%q history=%+v states=%+v", f.c.incidentOpen, f.c.closureBlocksNextLocked(), f.c.quiet(), f.c.completeHealthyEvaluation(), f.c.healthyMemoryReference, f.c.history, f.c.tracker.Snapshot())
			}
		})
	}
}
