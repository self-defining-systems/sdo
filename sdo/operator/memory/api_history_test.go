package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
)

func TestAPIHistoryRecoveryMarkerIsControllerTimeOutsideMemory(t *testing.T) {
	root := t.TempDir()
	sub, err := PrepareIncidentSession(root, "", "incident")
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, sub, "api-history")
	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := MarkIncidentAPIRecovery(root, "incident", at); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "recovery.json"))
	if err != nil {
		t.Fatal(err)
	}
	var marker struct {
		VerifiedAt time.Time `json:"verified_at"`
	}
	if json.Unmarshal(data, &marker) != nil || !marker.VerifiedAt.Equal(at) {
		t.Fatalf("marker: %s", data)
	}
	if err := MarkIncidentAPIRecovery(root, "incident", time.Time{}); err == nil {
		t.Fatal("zero verification time accepted")
	}
}

func TestAPIHistoryAttemptInvalidatesPreviousClosedRecorder(t *testing.T) {
	root := t.TempDir()
	sub, err := PrepareIncidentSession(root, "", "incident")
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, sub, "api-history")
	store, err := apihistory.Open(dir, "incident", apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	if _, err := PrepareIncidentAPIAttempt(root, "", "incident"); err != nil {
		t.Fatal(err)
	}
	history, err := apihistory.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if history.Manifest.Complete || history.Manifest.Dropped == 0 {
		t.Fatal("attempt reused old history")
	}
}

func TestAPIHistoryAttemptRejectsUnwritableGenerationBoundary(t *testing.T) {
	root := t.TempDir()
	sub, err := PrepareIncidentSession(root, "", "incident")
	if err != nil {
		t.Fatal(err)
	}
	marker := apihistory.GenerationPath(filepath.Join(root, sub, "api-history"))
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(marker, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareIncidentAPIAttempt(root, "", "incident"); err == nil {
		t.Fatal("launch boundary must fail closed")
	}
}
