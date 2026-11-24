package runtime

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/storestate"
)

func storeReader(t *testing.T, gap bool) sdk.Detector {
	t.Helper()
	d, err := storestate.NewDetector(storestate.Config{
		// The spec Go's generated reader declares (memory.genericSpec).
		Spec: sdk.DetectorSpec{
			ID: "store-state-kv", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
			Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
			Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
			Playbooks:   []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap", Interval: 30 * time.Second,
		},
		Component: "kv", Engine: storestate.EngineRedis,
		Target: podread.Target{Workload: "StatefulSet/kv"}, GapWithoutBaseline: gap,
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// The deployment gate's evaluation has no promoted baseline, and here no pod
// read can run at all. A store reader Go installed is then a coverage note on
// a clear verdict, never an evaluation failure that would stall acceptance;
// a designer's reader is named unavailable.
func TestEvaluateOnceReportsAnUnobservedGoStoreReaderAsACoverageNote(t *testing.T) {
	verdict, err := evaluateOnceClassed(t, []sdk.Detector{storeReader(t, true)})
	if err != nil || verdict.Status != evaluation.Clear || len(verdict.Findings) != 0 {
		t.Fatalf("verdict = %+v, %v", verdict, err)
	}
	var note string
	for _, state := range verdict.DetectorStates {
		if state.DetectorID == "store-state-kv" && state.Status == string(DetectorEvaluationClear) {
			note = state.Reason
		}
	}
	if !strings.Contains(note, "not observed") {
		t.Fatalf("detector states = %+v", verdict.DetectorStates)
	}
	// A designer's reader that could not read is named unavailable with its
	// reason, never cleared; it is not a user test, so it holds nothing
	// (evaluation.UnavailableHolds).
	designer, err := evaluateOnceClassed(t, []sdk.Detector{storeReader(t, false)})
	if err != nil {
		t.Fatalf("designer reader: %v", err)
	}
	if len(designer.Unavailable) != 1 || designer.Unavailable[0].DetectorID != "store-state-kv" || designer.Unavailable[0].Reason == "" {
		t.Fatalf("a designer's store reader cleared a read that never ran: %+v", designer)
	}
}
