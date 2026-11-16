package sdktest

import (
	"encoding/json"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// A recorded filtered log read replays exactly, so a learned Diagnose that uses it is replayable.
func TestReplayServesARecordedLogMatchesRead(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "web-0"}
	want := program.LogMatchResult{Lines: []string{"violates check constraint"}, Scanned: 9000}
	view := &ReplayReadView{Snapshot: Snapshot{NamespaceName: "app"}, Calls: []PrimitiveCall{{
		Method: "LogMatches", Arguments: mustJSON(t, []any{ref, "web", time.Minute, "violates", 5}), Result: mustJSON(t, want)}}}
	got, err := view.LogMatches(ref, "web", time.Minute, "violates", 5)
	if err != nil || len(got.Lines) != 1 || got.Scanned != 9000 {
		t.Fatalf("%+v, %v", got, err)
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
