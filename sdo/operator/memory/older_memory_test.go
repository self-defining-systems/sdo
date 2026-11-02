package memory

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// Synthetic outcome as an earlier SDO version wrote it: the retired
// reflection reason evidence_gap and the retired count field.
const retiredReasonOutcome = `{"schema_version":1,"incident_id":"old-incident","classification":"success","source_commit":"abc",
"reflection_eligibility":{"version":1,"evidence_gap_count":1,"unassigned":[{"program_id":"p","disposition":"question","reason":"evidence_gap"}]}}`

func writeOutcomeFile(t *testing.T, root, incidentID, document string) string {
	t.Helper()
	path := filepath.Join(root, layout.OutcomePath(incidentID))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestOutcomeWrittenByAnEarlierVersionStaysReadable(t *testing.T) {
	root := t.TempDir()
	writeOutcomeFile(t, root, "old-incident", retiredReasonOutcome)
	store := NewJSONStore(root)
	got, err := store.Get(context.Background(), "old-incident")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	item := got.Record.GetReflectionEligibility().GetUnassigned()[0]
	if item.GetReason() != "no_recorded_repair" {
		t.Fatalf("retired reason not mapped to its successor: %q", item.GetReason())
	}
	all, err := store.List(context.Background())
	if err != nil || len(all) != 1 {
		t.Fatalf("List = %d outcomes, %v", len(all), err)
	}
}

func TestOneUnreadableOutcomeNeverHidesTheRest(t *testing.T) {
	root := t.TempDir()
	store := NewJSONStore(root)
	if _, err := store.Put(context.Background(), outcome("good", "success")); err != nil {
		t.Fatal(err)
	}
	bad := writeOutcomeFile(t, root, "bad", `{"schema_version":1,"incident_id":"bad","classification":"not-a-class"}`)
	mismatch := writeOutcomeFile(t, root, "named-other", `{"schema_version":1,"incident_id":"other","classification":"success"}`)
	all, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("one invalid outcome failed the whole list: %v", err)
	}
	if len(all) != 1 || all[0].IncidentID != "good" {
		t.Fatalf("readable outcomes = %+v", all)
	}
	_, notes, err := store.ListWithNotes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(notes) != 2 {
		t.Fatalf("notes = %+v", notes)
	}
	for _, note := range notes {
		if note.Name == "" || note.Reason == "" {
			t.Fatalf("note lacks context: %+v", note)
		}
	}
	for _, path := range []string{bad, mismatch} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("reader modified accepted memory: %v", err)
		}
	}
	if !strings.Contains(notes[0].Name+notes[1].Name, ".json") {
		t.Fatalf("notes do not name the files: %+v", notes)
	}
}

func TestListOutcomesLogsEverySkippedOutcomeAndIgnoresNonDocuments(t *testing.T) {
	root := t.TempDir()
	store := NewJSONStore(root)
	if _, err := store.Put(context.Background(), outcome("good", "success")); err != nil {
		t.Fatal(err)
	}
	writeOutcomeFile(t, root, "bad", `{"schema_version":1,"incident_id":"bad","classification":"not-a-class"}`)
	leftover := filepath.Join(root, layout.OutcomesDir, ".outcome-123")
	if err := os.WriteFile(leftover, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	var logged strings.Builder
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(previous)

	outcomes, notes, err := ListOutcomes(context.Background(), store, "test reading")
	if err != nil || len(outcomes) != 1 || len(notes) != 1 {
		t.Fatalf("ListOutcomes = %d outcomes, %d notes, %v", len(outcomes), len(notes), err)
	}
	if !strings.Contains(logged.String(), "skipped unreadable outcome") || !strings.Contains(logged.String(), "test reading") || !strings.Contains(logged.String(), notes[0].Name) {
		t.Fatalf("skipped outcome not logged with its reader and file: %q", logged.String())
	}
}
