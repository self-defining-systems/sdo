package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// jsonNullPaths lists every path in an encoded document whose value is null.
// Python's strict contract models reject null for a list field, so a Go
// shape that the broker or a responder parses must never encode one: RC1's
// `changes: null` wedge passed both sides' unit tests because each side
// built its values in its own language.
func jsonNullPaths(t *testing.T, encoded []byte) []string {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var paths []string
	var walk func(path string, value any)
	walk = func(path string, value any) {
		switch typed := value.(type) {
		case nil:
			paths = append(paths, path)
		case map[string]any:
			for key, child := range typed {
				walk(path+"."+key, child)
			}
		case []any:
			for index, child := range typed {
				walk(fmt.Sprintf("%s[%d]", path, index), child)
			}
		}
	}
	walk("$", document)
	sort.Strings(paths)
	return paths
}

func assertNoJSONNulls(t *testing.T, name string, value any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %s: %v", name, err)
	}
	if paths := jsonNullPaths(t, encoded); len(paths) > 0 {
		t.Fatalf("%s encodes null at %v:\n%s", name, paths, encoded)
	}
}

// TestContractShapesNeverEncodeNullCollections encodes every Go shape Python
// parses, built with nil and empty collections and passed through the clone,
// export and restore paths the controller really uses.
func TestContractShapesNeverEncodeNullCollections(t *testing.T) {
	at := time.Unix(10, 0).UTC()
	request := &IncidentRequest{
		SchemaVersion: ProtocolSchemaVersion, Application: "demo", Namespace: "demo", IncidentID: "demo-1",
		Findings:        []sdk.Finding{{DetectorID: "cause", RuleID: "r", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: "s", Evidence: "e", PrimaryResource: sdk.ObjectRef{Kind: "Service", Name: "frontend"}}},
		DetectorHistory: []DetectorEvaluation{{DetectorID: "cause", EvaluatedAt: at, Status: DetectorEvaluationClear}},
		SourceCommit:    "abc", DeployedCommit: "abc", RepositoryWorktree: "/repo", RepositoryBaseCommit: "abc",
		ResponseDeadline: at, CancellationToken: "cancel", RepairPolicy: "recorded-actions",
		StateChanges: &StateChanges{BaselineAt: at, ObservedAt: at},
	}
	result := &IncidentResult{
		SchemaVersion: ProtocolSchemaVersion, IncidentID: "demo-1", Status: IncidentCompleted,
		ConfirmedRootCauses: []ConfirmedRootCause{{Summary: "cause"}},
		AppliedPlaybooks:    []AppliedPlaybook{{PlaybookID: "p"}},
		RepairActions:       []RepairActionReceipt{{ActionID: "a", Kind: "kubectl", Target: "Service/frontend", StartedAt: at, CompletedAt: at, Success: true}},
		Timing:              TimingMetrics{StartedAt: at, CompletedAt: at},
	}
	closure := &IncidentClosure{
		Request: *request, Result: result,
		FinalDetectorStates: []DetectorEvaluation{}, FinalStateChanges: &StateChanges{BaselineAt: at, ObservedAt: at},
		ObservedStateChanges: []ObservedStateChange{}, DetectedAt: at, DispatchedAt: at,
		ResponderCompletedAt: at, VerifiedAt: at,
	}
	view := &IncidentView{IncidentID: "demo-1", ObservedAt: at, StateChanges: &StateChanges{BaselineAt: at, ObservedAt: at}}

	assertNoJSONNulls(t, "cloned request", cloneIncidentRequest(request))
	assertNoJSONNulls(t, "cloned result", cloneIncidentResult(result))
	assertNoJSONNulls(t, "cloned closure", cloneIncidentClosure(closure))
	assertNoJSONNulls(t, "cloned incident view", cloneIncidentView(view))
	assertNoJSONNulls(t, "cloned state changes", cloneStateChanges(&StateChanges{BaselineAt: at, ObservedAt: at}))
}

// TestAControllersRealIncidentNeverEncodesNull drives one incident with no
// playbooks, no fingerprints, an empty diff and no helpers through dispatch,
// the incident view, a restart and closure, and checks every shape.
func TestAControllersRealIncidentNeverEncodesNull(t *testing.T) {
	cause := controllerDetector(
		"cause", time.Second, stateFinding("cause"), stateFinding("cause"),
		sdk.Finding{}, sdk.Finding{}, sdk.Finding{}, sdk.Finding{}, sdk.Finding{},
	)
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(
		testControllerConfig(), []sdk.Detector{cause}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	controller.Baseline = &scriptedBaseline{changes: func(time.Time) []StateChange { return nil }}
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }

	for sample := 0; sample < 2; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("firing step: %v", err)
		}
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	assertNoJSONNulls(t, "dispatched request", request)
	awaitDispatchCompletionQueued(t, controller)
	if err := controller.Step(context.Background(), time.Unix(2, 0), nil); err != nil {
		t.Fatalf("step: %v", err)
	}
	state := controller.ExportState()
	if state.IncidentView == nil {
		t.Fatal("an open incident must publish its view")
	}
	assertNoJSONNulls(t, "incident view", state.IncidentView)
	if state.IncidentRequest != nil {
		assertNoJSONNulls(t, "persisted request", state.IncidentRequest)
	}
	var closure IncidentClosure
	for sample := 3; sample < 8 && closure.Request.IncidentID == ""; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("step %d: %v", sample, err)
		}
		select {
		case closure = <-closed:
		default:
		}
	}
	if closure.Request.IncidentID == "" {
		t.Fatal("verified incident did not emit closure")
	}
	assertNoJSONNulls(t, "closure", closure)
	pending, ok := controller.PendingIncidentClosure()
	if !ok {
		t.Fatal("closure must stay pending for the broker")
	}
	assertNoJSONNulls(t, "pending closure", pending)

	restarted, err := NewController(
		testControllerConfig(), []sdk.Detector{cause}, staticProvider{snapshot: sdktest.Snapshot{}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := restarted.RestoreState(controller.ExportState()); err != nil {
		t.Fatalf("restore: %v", err)
	}
	restored, ok := restarted.PendingIncidentClosure()
	if !ok {
		t.Fatal("restored controller lost the pending closure")
	}
	assertNoJSONNulls(t, "restored pending closure", restored)
}
