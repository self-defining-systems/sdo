package runtime

import (
	"context"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// STALE3: a diagnosis keeps running when the scope changes under it.
func TestScopedDiagnosisSurvivesScopeChange(t *testing.T) {
	root := t.TempDir()
	request := IncidentRequest{IncidentID: "generation", Namespace: "app", ObservationGeneration: "S1"}
	phase := "open@detect"
	snapshot := sdktest.Snapshot{NamespaceName: "app", ScopedSnapshot: sdk.ScopedSnapshot{Version: 1, Generation: "S1"}}
	loop := ResponderLoop{Request: request, CorpusSessionRoot: root, capturePhase: phase, Provider: staticProvider{snapshot: snapshot}, ReadView: func(s sdk.DetectionContext) program.ReadView { return LiveReadView{Carrier: inputs.New(s)} }}
	if _, err := loop.snapshotView(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot.ScopedSnapshot.Generation = "S2"
	loop.Provider = staticProvider{snapshot: snapshot}
	if _, err := loop.snapshotView(context.Background()); err != nil {
		t.Fatalf("scope change failed the diagnosis: %v", err)
	}
}
