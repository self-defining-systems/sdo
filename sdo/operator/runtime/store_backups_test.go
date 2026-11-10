package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/program"
)

func syntheticBackup(id string, dump []byte) program.StoreBackupRecord {
	sum := sha256.Sum256(dump)
	return program.StoreBackupRecord{ID: id, IncidentID: "synthetic-incident", Component: "db", Engine: "redis", Items: 1, Bytes: int64(len(dump)), SHA256: hex.EncodeToString(sum[:])}
}

// Backups are kept per incident on the controller volume, immutable, and
// checked against their digest on every read.
func TestFileBackupStoreKeepsImmutableCheckedBackupsPerIncident(t *testing.T) {
	root := t.TempDir()
	store := FileBackupStore{Root: root}
	dump := []byte("6b -1 76\n#sdo-end 1\n")
	record, err := store.Put(syntheticBackup("before", dump), dump)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(record.Path, "synthetic-incident") || filepath.IsAbs(record.Path) {
		t.Fatalf("the path is relative and names the incident only by digest: %s", record.Path)
	}
	got, gotDump, err := store.Get("synthetic-incident", "before")
	if err != nil || string(gotDump) != string(dump) || got.SHA256 != record.SHA256 {
		t.Fatalf("Get = %+v %q %v", got, gotDump, err)
	}
	if _, err := store.Put(syntheticBackup("before", dump), dump); err == nil {
		t.Fatal("a backup ID is immutable within its incident")
	}
	if _, _, err := store.Get("synthetic-other", "before"); err == nil {
		t.Fatal("another incident cannot read this incident's backup")
	}
	if _, _, err := store.Get("synthetic-incident", "../before"); err == nil {
		t.Fatal("an ID never leaves the incident directory")
	}
	if err := os.WriteFile(filepath.Join(root, record.Path), []byte("tampered\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Get("synthetic-incident", "before"); err == nil || !strings.Contains(err.Error(), "cannot be trusted") {
		t.Fatalf("a changed dump is refused: %v", err)
	}
	entries, _ := os.ReadDir(filepath.Join(root, incidentDirectory("synthetic-incident")))
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".staging-") {
			t.Fatalf("staging directory left behind: %s", entry.Name())
		}
	}
}
