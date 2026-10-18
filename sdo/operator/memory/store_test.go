package memory

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func outcome(id, classification string) []byte {
	return []byte(`{"schema_version":1,"incident_id":"` + id + `","classification":"` + classification + `","source_commit":"abc"}`)
}

func TestOutcomeStoreIsImmutableAndLayoutIsPrivate(t *testing.T) {
	root := t.TempDir()
	var store Store = NewJSONStore(root)
	got, err := store.Put(context.Background(), outcome("incident/one", "success"))
	if err != nil {
		t.Fatal(err)
	}
	if got.IncidentID != "incident/one" || got.Classification != "success" {
		t.Fatalf("wrong outcome: %+v", got)
	}
	if _, err := store.Put(context.Background(), outcome("incident/one", "failed")); err == nil {
		t.Fatal("mutating an outcome succeeded")
	}
	read, err := store.Get(context.Background(), "incident/one")
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(read.Document) {
		t.Fatalf("invalid document: %s", read.Document)
	}
	all, err := store.List(context.Background())
	if err != nil || len(all) != 1 {
		t.Fatalf("list = %+v, %v", all, err)
	}
	if _, err := os.Stat(filepath.Join(root, ".sdo", "outcomes.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("legacy layout persisted: %v", err)
	}
}

func TestImportLegacyIsIdempotentAndRejectsConflicts(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, ".sdo", "outcomes.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(outcome("old", "success"), '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	store := NewJSONStore(root)
	if err := store.ImportLegacy(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.ImportLegacy(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("legacy file remains: %v", err)
	}
	if _, err := store.Put(context.Background(), outcome("old", "failed")); err == nil {
		t.Fatal("conflicting replacement accepted")
	}
}

func TestOutcomeValidation(t *testing.T) {
	store := NewJSONStore(t.TempDir())
	for _, document := range [][]byte{[]byte(`[]`), []byte(`{"schema_version":2,"incident_id":"x","classification":"success"}`), []byte(`{"schema_version":1,"incident_id":"","classification":"success"}`), []byte(`{"schema_version":1,"incident_id":"x","classification":"unknown"}`)} {
		if _, err := store.Put(context.Background(), document); err == nil {
			t.Fatalf("accepted %s", document)
		}
	}
	if _, err := store.Put(context.Background(), outcome(strings.Repeat("x", 512), "success")); err != nil {
		t.Fatal(err)
	}
}
