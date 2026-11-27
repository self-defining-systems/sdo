package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"sdo.dev/operator/runtime/internal/lifecycleio"

	"sdo.dev/operator/sdk"
)

// offPathStage is an incident opened by user test browse-items (path front,
// api, db in verifyArch) while a learned playbook's detector hangs until the
// read's deadline. credits are the playbook's earned keys.
func offPathStage(t *testing.T, credits ProgramCredits, read gitRead) (time.Duration, int32) {
	t.Helper()
	calls := &atomic.Int32{}
	_, loop, state, _ := playbookDetectorLoop(t, []restartProgram{{id: "aware-restart"}},
		stateDetector{playbooks: []string{"aware-restart"}, hang: true, calls: calls})
	alert := browseTest()
	alert.Summary, alert.Evidence = "browse-items fails", "synthetic user test"
	state.Request.Findings = []sdk.Finding{alert}
	state.Request.OpeningKeys = []string{findingKey(alert)}
	state.Findings = state.Request.Findings
	loop.Request = state.Request
	loop.findings = state.Findings
	loop.GitRead = read
	loop.setCredits(credits)
	loop.Now = nil
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true}
	started := lifecycleio.Now()
	if _, err := stage.Run(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	return lifecycleio.Since(started), calls.Load()
}

func creditedFor(kind, name string) ProgramCredits {
	return ProgramCredits{"aware-restart": {{Symptom: "earlier-symptom", Component: sdk.ObjectRef{Kind: kind, Namespace: "app", Name: name}}}}
}

// A learned playbook credited only for a component off every opening user
// test's path cannot bind for this incident: the stage does not wait for its
// detector's open-time read. Simulated stage time: 5 s (half the 10 s
// diagnosis deadline) before, 0 s after.
func TestDeterministicStageSkipsTheReadOfDetectorsOffTheFailingPaths(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		took, calls := offPathStage(t, creditedFor("Deployment", "cache"), fakeArchitecture(verifyArch))
		if calls != 0 || took > time.Second {
			t.Fatalf("the stage read the off-path detector %d times and took %s, want no read", calls, took)
		}
		t.Logf("stage time with the detector off the failing paths: %s", took)
	})
}

// Whatever could still bind on the path, or is unknown, is read as before:
// a playbook credited on the path, one with no credit, and an application
// with no map.
func TestDeterministicStageReadsDetectorsThatMayBindOnThePath(t *testing.T) {
	for name, tc := range map[string]struct {
		credits ProgramCredits
		read    gitRead
	}{
		"credited on the path": {creditedFor("StatefulSet", "db"), fakeArchitecture(verifyArch)},
		"no credit":            {nil, fakeArchitecture(verifyArch)},
		"no map":               {creditedFor("Deployment", "cache"), fakeArchitecture("")},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				took, calls := offPathStage(t, tc.credits, tc.read)
				if calls == 0 || took < DefaultDiagnosisDeadline/2 {
					t.Fatalf("the detector was read %d times and the stage took %s, want the read to its bound", calls, took)
				}
				t.Logf("stage time with the read: %s", took)
			})
		})
	}
}
