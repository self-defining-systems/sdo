package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// liveProgram is a new playbook whose Mitigate plans one EnsureExec.
type liveProgram struct{ diagnoseErr error }

func (liveProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "merge-renamed"} }
func (p liveProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.NoMatch}, p.diagnoseErr
}
func (liveProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{slowExecAction()}}, nil
}

type liveRegistry struct{ p liveProgram }

func (r liveRegistry) List() []program.ProgramSpec                { return []program.ProgramSpec{r.p.Spec()} }
func (r liveRegistry) Diagnoser(string) (program.Diagnoser, bool) { return r.p, true }
func (r liveRegistry) Mitigator(string) (program.Mitigator, bool) { return r.p, true }

type fakeLiveLoop struct {
	registry liveRegistry
	client   *codedExecClient
}

func (f *fakeLiveLoop) loadWorkingPrograms(context.Context, string) error { return nil }
func (f *fakeLiveLoop) snapshotView(context.Context) (program.ReadView, error) {
	return &sdktest.ReplayReadView{}, nil
}
func (f *fakeLiveLoop) programs() program.ProgramRegistry       { return f.registry }
func (f *fakeLiveLoop) operations() (PlanOperationClient, bool) { return f.client, true }

// At the closing turn a new playbook's checks run read only on the recovered
// cluster: an observation that answers neither "holds" nor "needs the change",
// or a Diagnose that errors, fails memory check there instead of on first
// reuse. No Ensure ever runs.
func TestNewPlaybookChecksRunLiveAtTheClosingTurn(t *testing.T) {
	for _, c := range []struct {
		name    string
		observe int32
		diagErr error
		fails   string
	}{
		{"state holds", 0, nil, ""},
		{"needs the change", 1, nil, ""},
		{"script error", 252, nil, "exited 252"},
		{"diagnose error", 0, errors.New("exec: mongo: not found"), "Diagnose failed live"},
	} {
		t.Run(c.name, func(t *testing.T) {
			loop := &fakeLiveLoop{registry: liveRegistry{p: liveProgram{diagnoseErr: c.diagErr}}, client: &codedExecClient{observe: c.observe}}
			diagnostics := checkPlaybooksLive(context.Background(), loop, []string{"merge-renamed"})
			for _, call := range loop.client.calls {
				if call == "ensure" {
					t.Fatalf("the live check ran an Ensure: %v", loop.client.calls)
				}
			}
			if c.fails == "" {
				if len(diagnostics) != 0 {
					t.Fatalf("diagnostics %+v, want none", diagnostics)
				}
				return
			}
			if len(diagnostics) != 1 || diagnostics[0].Rule != LivePlaybookCheckRule || !strings.Contains(diagnostics[0].Message, c.fails) {
				t.Fatalf("diagnostics %+v, want one %s naming %q", diagnostics, LivePlaybookCheckRule, c.fails)
			}
		})
	}
}
