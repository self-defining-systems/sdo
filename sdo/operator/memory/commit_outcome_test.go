package memory

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/layout"
)

func TestOutcomeAtIgnoresDirtyAndNewerOutcomeBytes(t *testing.T) {
	root := t.TempDir()
	testutil.InitGit(t, root)
	catalogGit(t, root, "config", "user.name", "Test")
	catalogGit(t, root, "config", "user.email", "test@example.invalid")
	store := NewJSONStore(root)
	original := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "synthetic", Classification: "success"}
	if _, err := store.Put(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	catalogGit(t, root, "add", ".")
	catalogGit(t, root, "commit", "-qm", "accepted outcome")
	before := catalogGit(t, root, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(root, layout.OutcomePath(original.IncidentId)), []byte(`{"schema_version":1,"incident_id":"synthetic","classification":"failed"}`), 0644); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetAt(context.Background(), before, "synthetic")
	if err != nil || got.Classification != "success" {
		t.Fatalf("dirty outcome: %v %v", got, err)
	}
	catalogGit(t, root, "add", ".")
	catalogGit(t, root, "commit", "-qm", "newer accepted outcome")
	got, err = store.GetAt(context.Background(), before, "synthetic")
	if err != nil || got.Classification != "success" {
		t.Fatalf("newer outcome: %v %v", got, err)
	}
	if _, err = store.GetAt(context.Background(), "HEAD", "synthetic"); err == nil {
		t.Fatal("floating revision accepted")
	}
}
