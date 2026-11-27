package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// joinedFault matches only the state in which the later fault shows.
type joinedFault struct{}

func (joinedFault) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "later"} }
func (joinedFault) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	if view.Namespace() == "joined" {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

// joinCapture is an incident capture whose opening state lacks the later fault
// and whose join capture shows it.
func joinCapture(t *testing.T) string {
	t.Helper()
	capture := t.TempDir()
	for name, namespace := range map[string]string{"fixed.json": "fixed", "open@detect.json": "opening", "open@join-1.json": "joined"} {
		data, _ := json.Marshal(sdktest.Snapshot{NamespaceName: namespace})
		writeSynthetic(t, capture, name, string(data))
	}
	for _, phase := range []string{"open@detect", "open@join-1"} {
		writeSynthetic(t, capture, phase+"-recordings.json", "[]")
	}
	return capture
}

// The match key of a playbook that ran in the incident comes from the capture
// in which its Diagnose matches, a join capture included.
func TestAMatchedPlaybooksKeyComesFromTheJoinCaptureItMatchesIn(t *testing.T) {
	registry, err := program.NewRegistry([]program.Diagnoser{joinedFault{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	outcome := &contracts.OutcomeRecord{ProgramRuns: []*contracts.ProgramRun{{ProgramId: "later", Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_MATCH}}}}
	origins := map[string]string{}
	fixedCaptureVerdicts(context.Background(), outcome, registry, joinCapture(t), origins)
	if got := origins[matchedViewKey("later", program.Bindings{})]; got != "join-1" {
		t.Fatalf("origin = %q, want join-1 (origins %v)", got, origins)
	}
}

// A new playbook's first case comes from the capture its own-incident replay
// matched: for a later fault, the join capture, not the opening state.
func TestANewPlaybooksFirstCaseComesFromTheCaptureItsOwnIncidentReplayMatched(t *testing.T) {
	worktree := t.TempDir()
	writeSynthetic(t, worktree, ".sdo/playbooks/later/diagnose.go", "package later\n\ntype P struct{}\n\nfunc (P) Spec() program.ProgramSpec { return program.ProgramSpec{ID: \"later\"} }\n")
	writeSynthetic(t, worktree, ".sdo/playbooks/first/diagnose.go", "package first\n\ntype P struct{}\n\nfunc (P) Spec() program.ProgramSpec { return program.ProgramSpec{ID: \"first\"} }\n")
	if err := os.MkdirAll(filepath.Join(worktree, ".sdo", "corpus"), 0o755); err != nil {
		t.Fatal(err)
	}
	notes := []*contracts.MemoryDiagnostic{
		validate.ProtoDiagnostics([]validate.Diagnostic{validate.OwnIncidentNote(sdktest.OwnIncidentResult{ProgramID: "later", Outcome: sdktest.OwnIncidentMatch, Capture: "open@join-1.json"})})[0],
		validate.ProtoDiagnostics([]validate.Diagnostic{validate.OwnIncidentNote(sdktest.OwnIncidentResult{ProgramID: "first", Outcome: sdktest.OwnIncidentMatch, Capture: "open@detect.json"})})[0],
	}
	origins := map[string]string{}
	addOwnIncidentOrigins(origins, []string{"later", "first"}, notes)

	capture := joinCapture(t)
	recordFirstCases(worktree, []string{"later", "first"}, capture, "incident-1", memory.CaseContext{}, origins)

	labels, _ := filepath.Glob(filepath.Join(worktree, ".sdo", "corpus", "*", "labels.json"))
	got := map[string]string{}
	for _, path := range labels {
		loaded, err := sdktest.LoadCorpusMetadata(filepath.Dir(path))
		if err != nil {
			t.Fatal(err)
		}
		got[loaded.RootCauseProgramIDs[0]] = loaded.Provenance.OpenAt
	}
	if got["later"] != "join-1" || got["first"] != "detect" {
		t.Fatalf("case captures = %v, want later from join-1 and first from detect", got)
	}
	var changed []string
	for _, path := range labels {
		rel, _ := filepath.Rel(worktree, filepath.Dir(path))
		entries, _ := os.ReadDir(filepath.Dir(path))
		for _, entry := range entries {
			changed = append(changed, filepath.ToSlash(filepath.Join(rel, entry.Name())))
		}
	}
	if err := memory.ValidateCapturedCorpusDirectory(capture, worktree, "incident-1", changed); err != nil {
		t.Fatalf("provenance: %v", err)
	}
}

// A join capture taken after SDO or the agent wrote says so: a Diagnose that
// matches there could be matching state a write made.
func TestAJoinCaptureRecordsWhetherKnownWritesPrecededIt(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	for name, wrote := range map[string]bool{"no write": false, "after a stage write": true} {
		t.Run(name, func(t *testing.T) {
			opening := &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}
			later := &switchedProbe{id: memory.TrafficDetectorPrefix + "later", scenario: "clone"}
			opening.failing.Store(true)
			c, _ := alertSetController(t, now, opening, later)
			root := t.TempDir()
			c.config.CorpusSessionRoot = root
			c.OnError = func(err error) { t.Log(err) }
			c.responderDone = false
			c.provider = staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}
			c.currentIncidentRequest.Namespace = "demo"
			c.currentIncidentResult.RepairActions = nil
			if wrote {
				c.currentIncidentRequest.PriorStages = []*contracts.StageReport{{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{{ApplyReceipt: &contracts.PlanApplication{ObjectEffects: []*contracts.PlanObjectEffect{{State: "applied"}}}}}}}
			}
			stepFor(t, c, now, 2)
			later.failing.Store(true)
			stepFor(t, c, now.Add(2*time.Second), 2)
			dir, err := memory.IncidentCorpusDir(root, "synthetic")
			if err != nil {
				t.Fatal(err)
			}
			joinContext, err := memory.ReadJoinCaptureContext(dir, 1)
			if err != nil {
				t.Fatal(err)
			}
			if joinContext.KnownWritesBefore != wrote || len(joinContext.JoinedKeys) == 0 {
				t.Fatalf("join context = %+v, want known_writes_before=%v and the joined keys", joinContext, wrote)
			}
		})
	}
}
