package runtime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// The controller fills recent_changes from a renderer; what it builds for a
// wide candidate view must pass the same validation state persistence runs.
func TestCandidateBlockWorstCaseSatisfiesTheContract(t *testing.T) {
	fault := classedDetector("workload-health", sdk.DetectorClassHealth, true)
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	config := testControllerConfig()
	var rows []IncidentChange
	for i := 0; i < 80; i++ {
		rows = append(rows, IncidentChange{At: time.Unix(0, 0).UTC(), Source: "app-state", Component: fmt.Sprintf("c%d", i), ReaderOrObject: "s", Key: fmt.Sprintf("k%d", i), Before: "1", After: "2"})
	}
	config.AppStateChanges = &staticAppState{rows: rows}
	controller, err := NewController(config, []sdk.Detector{fault}, staticProvider{snapshot: detectApp("create")}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	var reported []error
	controller.OnError = func(err error) { reported = append(reported, err) }
	controller.now = func() time.Time { return time.Unix(1, 0).UTC() }
	for sample := 0; sample < 2; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	state, err := controller.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeState(state); err != nil {
		t.Fatalf("the state the controller persists after opening is invalid: %v", err)
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if len(request.RecentChanges) < 2 {
		t.Fatalf("no candidate block: %q", request.RecentChanges)
	}
	if !recentChangesWithin(request.RecentChanges) {
		t.Fatalf("the block violates the contract (%d lines)", len(request.RecentChanges))
	}
	for _, err := range reported {
		if strings.Contains(err.Error(), "recent_changes") {
			t.Fatalf("the controller had to degrade a block the renderer produced: %v", err)
		}
	}
}

// The bound derives from the render cap: the shortest lines are one byte plus
// a newline, and no line outgrows the cap.
func TestRecentChangesContractCoversTheRenderCap(t *testing.T) {
	lines := make([]string, candidateRenderCap/2)
	for i := range lines {
		lines[i] = "x"
	}
	if !recentChangesWithin(lines) {
		t.Fatalf("%d one-byte lines (a %d-byte block) exceed the contract", len(lines), candidateRenderCap)
	}
	if !recentChangesWithin([]string{strings.Repeat("x", candidateRenderCap)}) {
		t.Fatal("one cap-sized line exceeds the contract")
	}
}

// A block that exceeds the contract is cut and reported, never sent invalid.
func TestOversizedCandidateBlockDegradesInsteadOfWedging(t *testing.T) {
	controller := &Controller{}
	var reported []error
	controller.OnError = func(err error) { reported = append(reported, err) }
	lines := make([]string, 5000)
	for i := range lines {
		lines[i] = strings.Repeat("y", 3000)
	}
	got := controller.boundRecentChanges(lines)
	if len(got) == 0 || !recentChangesWithin(got) {
		t.Fatalf("degraded block is empty or still invalid (%d lines)", len(got))
	}
	if len(reported) != 1 {
		t.Fatalf("degrade not reported: %v", reported)
	}
}
