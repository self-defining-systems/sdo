package memory

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func outcome(id, classification string) *contracts.OutcomeRecord {
	return &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: id, Classification: classification, SourceCommit: "abc"}
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
	if !json.Valid(read.document) {
		t.Fatalf("invalid document: %s", read.document)
	}
	all, err := store.List(context.Background())
	if err != nil || len(all) != 1 {
		t.Fatalf("list = %+v, %v", all, err)
	}
}

func TestOutcomeValidation(t *testing.T) {
	store := NewJSONStore(t.TempDir())
	for _, record := range []*contracts.OutcomeRecord{nil, {SchemaVersion: 2, IncidentId: "x", Classification: "success"}, {SchemaVersion: 1, IncidentId: "", Classification: "success"}, {SchemaVersion: 1, IncidentId: "x", Classification: "unknown"}} {
		if _, err := store.Put(context.Background(), record); err == nil {
			t.Fatalf("accepted %+v", record)
		}
	}
	if _, err := store.Put(context.Background(), outcome(strings.Repeat("x", 512), "success")); err != nil {
		t.Fatal(err)
	}
}
