package runtime

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"

	"sdo.dev/operator/sdk"
)

// offPathStage is an incident opened by user test browse-items (path front,
// api, db in verifyArch) while the learned playbook aware-restart's detector
// hangs until the read's deadline. credits are the playbook's earned keys;
// fixed makes the program declare no object parameter.
func offPathStage(t *testing.T, credits ProgramCredits, read gitRead, fixed bool) (time.Duration, int32) {
	t.Helper()
	run := offPathRun(t, restartProgram{id: "aware-restart", fixed: fixed}, true, credits, read)
	return run.took, run.calls
}

type offPathResult struct {
	loop   *ResponderLoop
	state  *IncidentState
	took   time.Duration
	calls  int32
	report *contracts.StageReport
}

// offPathRun runs the stage on the browse-items incident with one program and
// its detector, which hangs or reports every Deployment (api in the world).
func offPathRun(t *testing.T, p restartProgram, hang bool, credits ProgramCredits, read gitRead) offPathResult {
	t.Helper()
	calls := &atomic.Int32{}
	_, loop, state, _ := playbookDetectorLoop(t, []restartProgram{p},
		stateDetector{playbooks: []string{p.id}, hang: hang, calls: calls})
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
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	return offPathResult{loop: loop, state: state, took: lifecycleio.Since(started), calls: calls.Load(), report: report}
}

func creditedFor(kind, name string) ProgramCredits {
	return ProgramCredits{"aware-restart": {{Symptom: "earlier-symptom", Component: sdk.ObjectRef{Kind: kind, Namespace: "app", Name: name}}}}
}

// A learned playbook with fixed targets credited only for a component off
// every opening user test's path cannot bind for this incident: the stage does not wait for its
// detector's open-time read. Simulated stage time: 5 s (half the 10 s
// diagnosis deadline) before, 0 s after.
func TestDeterministicStageSkipsTheReadOfDetectorsOffTheFailingPaths(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		took, calls := offPathStage(t, creditedFor("Deployment", "cache"), fakeArchitecture(verifyArch), true)
		if calls != 0 || took > time.Second {
			t.Fatalf("the stage read the off-path detector %d times and took %s, want no read", calls, took)
		}
		t.Logf("stage time with the detector off the failing paths: %s", took)
	})
}

// Whatever could still bind on the path, or is unknown, is read as before:
// a playbook credited on the path, one with no credit, one whose program
// takes a component parameter (its credits record past fixes, not its reach),
// and an application with no map.
func TestDeterministicStageReadsDetectorsThatMayBindOnThePath(t *testing.T) {
	for name, tc := range map[string]struct {
		credits ProgramCredits
		read    gitRead
		fixed   bool
	}{
		"credited on the path":          {creditedFor("StatefulSet", "db"), fakeArchitecture(verifyArch), true},
		"no credit":                     {nil, fakeArchitecture(verifyArch), true},
		"component parameter, off path": {creditedFor("Deployment", "cache"), fakeArchitecture(verifyArch), false},
		"no map":                        {creditedFor("Deployment", "cache"), fakeArchitecture(""), true},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				took, calls := offPathStage(t, tc.credits, tc.read, tc.fixed)
				if calls == 0 || took < DefaultDiagnosisDeadline/2 {
					t.Fatalf("the detector was read %d times and the stage took %s, want the read to its bound", calls, took)
				}
				t.Logf("stage time with the read: %s", took)
			})
		})
	}
}

// A playbook whose program takes a component parameter, credited only for
// cache, still binds a new fault on api, a component of the failing test's
// path: its detector is read at open, its finding on api joins the incident,
// and the stage applies the playbook bound to api. Credits rank bindings and
// never filter them.
func TestDeterministicStageReadsParameterizedPlaybookCreditedElsewhere(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		run := offPathRun(t, restartProgram{id: "aware-restart"}, false, creditedFor("Deployment", "cache"), fakeArchitecture(verifyArch))
		if run.calls == 0 {
			t.Fatal("the parameterized playbook's detector was not read")
		}
		if !slices.ContainsFunc(run.state.Findings, func(f sdk.Finding) bool { return f.DetectorID == "store-state" && f.PrimaryResource.Name == "api" }) {
			t.Fatalf("the detector's finding on api must join the incident: %v", run.state.Findings)
		}
		if !slices.ContainsFunc(run.report.GetMitigations(), func(m *contracts.StageMitigation) bool {
			return m.GetPlaybookId() == "aware-restart" && m.GetApplyReceipt().GetApplied()
		}) {
			t.Fatalf("the stage must apply the playbook credited elsewhere to api: %v", run.report)
		}
	})
}

// Two stores with the same role: a playbook credited for the opening alert on
// store-a, whose program binds the component role, meets a sibling fault on
// another store (api in this world). The stage binds it through the finding,
// its Diagnose matches there, and it applies: credits rank, never filter.
func TestDeterministicStageAppliesACreditedPlaybookToASiblingStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := &atomic.Int32{}
		p := restartProgram{id: "aware-restart", role: ComponentRole}
		_, loop, state, instant := playbookDetectorLoop(t, []restartProgram{p}, stateDetector{playbooks: []string{p.id}, calls: calls})
		loop.setCredits(ProgramCredits{p.id: {{Symptom: state.Request.OpeningKeys[0], Component: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "store-a"}}}})
		stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true, diagnoseWait: instant.wait}
		report, err := stage.Run(context.Background(), state)
		if err != nil {
			t.Fatal(err)
		}
		applied := slices.ContainsFunc(report.GetMitigations(), func(m *contracts.StageMitigation) bool {
			if m.GetPlaybookId() != p.id || !m.GetApplyReceipt().GetApplied() {
				return false
			}
			return slices.ContainsFunc(m.GetBindings(), func(b *contracts.Binding) bool {
				return b.GetName() == ComponentRole && b.GetObject().GetName() == "api"
			})
		})
		if !applied {
			t.Fatalf("the stage must apply the playbook credited on store-a to the sibling api: %v", report)
		}
	})
}
