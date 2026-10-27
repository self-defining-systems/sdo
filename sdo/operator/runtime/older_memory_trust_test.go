package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

// Producer to consumer: outcome files on disk, as an earlier build and a
// damaged write left them, through the real repository reader into dispatch
// preparation. Neither may stop the controller from preparing a responder.
func TestDispatchPreparationSurvivesOlderAndUnreadableOutcomes(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	write := func(incidentID, document string) {
		path := filepath.Join(root, layout.OutcomePath(incidentID))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("older", `{"schema_version":1,"incident_id":"older","classification":"success","reflection_eligibility":{"version":1,"unassigned":[{"program_id":"p","disposition":"question","reason":"evidence_gap"}]}}`)
	write("damaged", `{"schema_version":1,"incident_id":"damaged","classification":"nonsense"}`)
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "outcomes from earlier builds")
	d := KubernetesJobDispatcher{Outcomes: memory.OpenReader(root)}
	request := IncidentRequest{RepositoryWorktree: root, SourceCommit: nativeGit(t, root, "rev-parse", "HEAD")}
	if _, err := d.programTrust(ctx, request); err != nil {
		t.Fatalf("stored outcomes blocked dispatch preparation: %v", err)
	}
}

// notedOutcomes is a reader whose scan skipped some documents.
type notedOutcomes struct {
	outcomes []memory.Outcome
	notes    []memory.UnreadableOutcome
}

func (r notedOutcomes) Get(context.Context, string) (memory.Outcome, error) {
	return memory.Outcome{}, memory.ErrNotFound
}
func (r notedOutcomes) List(context.Context) ([]memory.Outcome, error) { return r.outcomes, nil }
func (r notedOutcomes) ListWithNotes(context.Context) ([]memory.Outcome, []memory.UnreadableOutcome, error) {
	return r.outcomes, r.notes, nil
}

// An outcome that cannot be read may have been a failure, so while one exists
// no program may look more trusted than it does with every outcome readable.
func TestUnreadableOutcomeNeverRaisesProgramTrust(t *testing.T) {
	ctx := context.Background()
	good := verifiedTrustOutcome(t, "targetport", "revision-a")
	good.IncidentId = "good"
	stored := []memory.Outcome{{Record: good, IncidentID: "good"}}
	trust := func(notes ...memory.UnreadableOutcome) map[string]ProgramTrust {
		t.Helper()
		d := KubernetesJobDispatcher{Outcomes: notedOutcomes{outcomes: stored, notes: notes}}
		stats, err := d.programTrust(ctx, IncidentRequest{})
		if err != nil {
			t.Fatal(err)
		}
		return stats
	}
	readable := trust()
	if readable["targetport"].VerifiedSuccesses != 1 || readable["targetport"].Failures != 0 {
		t.Fatalf("fixture earns no trust, so the test proves nothing: %+v", readable["targetport"])
	}
	withGap := trust(memory.UnreadableOutcome{Name: "damaged.json", Reason: "invalid outcome"})
	for id, before := range readable {
		after := withGap[id]
		if after.VerifiedSuccesses > before.VerifiedSuccesses || after.Failures < before.Failures {
			t.Errorf("program %s gained trust from an unreadable outcome: %+v -> %+v", id, before, after)
		}
	}
	if got := withGap["targetport"]; got.VerifiedSuccesses != 0 || got.Failures != 1 {
		t.Fatalf("unreadable outcome not counted against trust: %+v", got)
	}
}
