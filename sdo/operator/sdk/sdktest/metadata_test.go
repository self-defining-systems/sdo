package sdktest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCorpusMetadataDoesNotRequireReplayInputs(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "labels.json"), []byte(`{"root_cause_program_ids":["repair"],"script_origins":{"digest":"receipt"},"provenance":{"incident_id":"incident"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	metadata, err := LoadCorpusMetadata(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata.RootCauseProgramIDs) != 1 || metadata.ScriptOrigins["digest"] != "receipt" || metadata.Provenance.IncidentID != "incident" {
		t.Fatalf("metadata was lost: %+v", metadata)
	}
	if _, err := LoadCorpusCase(dir); err == nil {
		t.Fatal("analysis replay silently invented missing snapshots")
	}
}
