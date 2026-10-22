package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
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
