package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// liveProgram is a changed playbook whose Mitigate plans one EnsureExec, or,
// with quietWhenHealthy, plans it only on the fault-time capture.
type liveProgram struct {
	diagnoseErrs     int
	quietWhenHealthy bool
	mitigations      int
	seen             *[]program.Bindings
}

func (*liveProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "merge-renamed", Params: []program.ParamSpec{{Name: "store", Kind: "Pod"}, {Name: "collection", ValueKind: program.StringValue}}}
}
func (p *liveProgram) Diagnose(_ context.Context, _ program.ReadView, b program.Bindings) (program.Verdict, error) {
	if p.seen != nil {
		*p.seen = append(*p.seen, b)
	}
	if p.diagnoseErrs > 0 {
		p.diagnoseErrs--
		return program.Verdict{}, errors.New("exec: mongo: not found")
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

// Mitigate plans first on the recovered cluster, then (if asked) on the capture.
func (p *liveProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	p.mitigations++
	if p.mitigations == 1 && p.quietWhenHealthy {
		return program.Plan{}, nil
	}
	return program.Plan{Actions: []program.Action{slowExecAction()}}, nil
}

type liveRegistry struct{ p *liveProgram }

func (r liveRegistry) List() []program.ProgramSpec                { return []program.ProgramSpec{r.p.Spec()} }
func (r liveRegistry) Diagnoser(string) (program.Diagnoser, bool) { return r.p, true }
func (r liveRegistry) Mitigator(string) (program.Mitigator, bool) { return r.p, true }

// flakyExecClient fails its first execErrs observations with a transport
// error, as when the target Pod is replaced during the check.
type flakyExecClient struct {
	codedExecClient
	execErrs int
}

func (c *flakyExecClient) RunOperation(ctx context.Context, op program.ExternalOperation) (int32, error) {
	if c.execErrs > 0 {
		c.execErrs--
		c.calls = append(c.calls, "observe")
		return 0, errors.New("pods \"toolbox\" not found")
	}
	return c.codedExecClient.RunOperation(ctx, op)
}

type fakeLiveLoop struct {
	registry liveRegistry
	client   *flakyExecClient
}

func (f *fakeLiveLoop) loadWorkingPrograms(context.Context, string) error { return nil }
func (f *fakeLiveLoop) snapshotView(context.Context) (program.ReadView, error) {
	return &sdktest.ReplayReadView{}, nil
}
func (f *fakeLiveLoop) programs() program.ProgramRegistry       { return f.registry }
func (f *fakeLiveLoop) operations() (PlanOperationClient, bool) { return f.client, true }

// At the closing turn a changed playbook's checks run read only on the
// recovered cluster. Only an observation's own exit code, answering neither
// "holds" nor "needs the change", blocks memory check. A check that cannot run
// (an exec error, a Diagnose error) is retried once and then is a note, so a
// transient hiccup never costs the playbook. No Ensure ever runs.
func TestNewPlaybookChecksRunLiveAtTheClosingTurn(t *testing.T) {
	for _, c := range []struct {
		name               string
		observe            int32
		diagErrs, execErrs int
		blocks, notes      string
	}{
		{name: "state holds", observe: 0},
		{name: "needs the change", observe: 1},
		{name: "script error", observe: 252, blocks: "exited 252"},
		{name: "one diagnose error is retried", diagErrs: 1},
		{name: "diagnose errors twice", diagErrs: 2, notes: "Diagnose did not complete"},
		{name: "one exec error is retried", execErrs: 1},
		{name: "exec errors twice", execErrs: 2, notes: "could not run live twice"},
	} {
		t.Run(c.name, func(t *testing.T) {
			loop := &fakeLiveLoop{registry: liveRegistry{p: &liveProgram{diagnoseErrs: c.diagErrs}}, client: &flakyExecClient{codedExecClient: codedExecClient{observe: c.observe}, execErrs: c.execErrs}}
			blocking, notes := checkPlaybooksLive(context.Background(), loop, []liveCheckTarget{{ID: "merge-renamed"}})
			for _, call := range loop.client.calls {
				if call == "ensure" {
					t.Fatalf("the live check ran an Ensure: %v", loop.client.calls)
				}
			}
			for _, want := range []struct {
				got  []string
				rule string
				text string
			}{{messages(blocking, LivePlaybookCheckRule), LivePlaybookCheckRule, c.blocks}, {messages(notes, LivePlaybookCheckNote), LivePlaybookCheckNote, c.notes}} {
				if want.text == "" && len(want.got) != 0 || want.text != "" && (len(want.got) != 1 || !strings.Contains(want.got[0], want.text)) {
					t.Fatalf("%s %v, want %q (blocking %+v notes %+v)", want.rule, want.got, want.text, blocking, notes)
				}
			}
		})
	}
}

// A changed playbook is checked with this incident's captured bindings, and a
// Mitigate that plans nothing on the recovered cluster has the plan it made on
// the incident's fault-time capture checked instead. A playbook with roles and
// no captured case is a note, never silently passed.
func TestLiveCheckUsesTheIncidentsCapturedCase(t *testing.T) {
	bound := program.Bindings{Objects: program.ObjectBindings{"store": sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "store-0"}}, Values: map[string]any{"collection": "items"}}
	var seen []program.Bindings
	loop := &fakeLiveLoop{registry: liveRegistry{p: &liveProgram{quietWhenHealthy: true, seen: &seen}}, client: &flakyExecClient{codedExecClient: codedExecClient{observe: 252}}}
	blocking, notes := checkPlaybooksLive(context.Background(), loop, []liveCheckTarget{{ID: "merge-renamed", Roles: []string{"store"}, Captured: true, Bindings: bound, Open: &sdktest.ReplayReadView{}}})
	if len(seen) != 1 || !seen[0].Equal(bound) {
		t.Fatalf("Diagnose bindings %+v, want the captured %+v", seen, bound)
	}
	if len(blocking) != 1 || !strings.Contains(blocking[0].Message, "exited 252") || len(notes) != 0 {
		t.Fatalf("blocking %+v notes %+v, want the fault-time plan's failed check", blocking, notes)
	}

	loop = &fakeLiveLoop{registry: liveRegistry{p: &liveProgram{}}, client: &flakyExecClient{codedExecClient: codedExecClient{observe: 252}}}
	blocking, notes = checkPlaybooksLive(context.Background(), loop, []liveCheckTarget{{ID: "merge-renamed", Roles: []string{"store"}}})
	if len(blocking) != 0 || len(notes) != 1 || !strings.Contains(notes[0].Message, "roles store") || len(loop.client.calls) != 0 {
		t.Fatalf("blocking %+v notes %+v calls %v, want one roles note and no run", blocking, notes, loop.client.calls)
	}
}

func messages(diagnostics []validate.Diagnostic, rule string) []string {
	var out []string
	for _, d := range diagnostics {
		if d.Rule == rule {
			out = append(out, d.Message)
		}
	}
	return out
}

// The captured case is the one Go wrote for this incident and this playbook;
// another incident's case binds nothing.
func TestLiveCheckTargetsComeFromThisIncidentsCase(t *testing.T) {
	root := t.TempDir()
	write := func(name, labels string) {
		dir := filepath.Join(root, ".sdo", "corpus", name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "labels.json"), []byte(labels), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("ours", `{"root_cause_program_ids":["merge-renamed"],"bindings":{"values":{"collection":"items"}},"provenance":{"incident_id":"inc-1","open_sha256":"x","fixed_sha256":"y"}}`)
	write("other", `{"root_cause_program_ids":["fresh"],"bindings":{"values":{"collection":"other"}},"provenance":{"incident_id":"inc-0","open_sha256":"x","fixed_sha256":"y"}}`)
	targets := liveCheckTargets(root, "inc-1", []string{"fresh", "merge-renamed"})
	if len(targets) != 2 || targets[0].Captured || !targets[1].Captured || targets[1].Bindings.Values["collection"] != "items" {
		t.Fatalf("targets %+v, want only merge-renamed bound from inc-1's case", targets)
	}
}
