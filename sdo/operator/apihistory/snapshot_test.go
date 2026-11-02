//go:build integration

package apihistory

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Real writer, asynchronous projections and read-only reader: a live prefix
// can certify closure without waiting for the sidecar/process to terminate.
func TestLiveSnapshotDrainsProjectionAndRetainsAnImmutablePrefix(t *testing.T) {
	dir := t.TempDir()
	lease, err := BeginWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	store, err := Open(dir, "synthetic", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	started := time.Now()
	store.Record(Event{RequestID: "synthetic", Phase: "start", Verb: "patch", Started: started}, nil)
	store.Record(Event{RequestID: "synthetic", Phase: "end", Verb: "patch", Started: started, Finished: time.Now(), Status: 200}, []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"synthetic","namespace":"app"},"data":{"key":"safe"}}`))
	until := time.Now()
	history, err := ReadThrough(context.Background(), dir, until)
	if err != nil {
		t.Fatal(err)
	}
	if !history.Manifest.Open || !history.Manifest.Complete || history.Manifest.SnapshotThrough.Before(until) || history.Manifest.Events != 2 || len(history.Events) != 2 {
		t.Fatalf("invalid live checkpoint: %+v", history)
	}
	var body map[string]any
	for _, event := range history.Events {
		if event.Phase == "end" && json.Unmarshal(history.Body(event), &body) != nil {
			t.Fatal("checkpoint preceded projected blob")
		}
	}
	if body == nil {
		t.Fatal("checkpoint omitted asynchronous projection")
	}
	// Later append/drop cannot mutate the already returned value.
	store.Record(Event{Phase: "start", Verb: "get", Started: time.Now()}, nil)
	store.Gap()
	if !history.Manifest.Complete || history.Manifest.Dropped != 0 || len(history.Events) != 2 {
		t.Fatal("returned snapshot changed")
	}
	if _, err := ReadThrough(context.Background(), dir, time.Now()); err == nil {
		t.Fatal("real drop certified")
	}
}

func TestClosureSnapshotRejectsEveryUnknownProducerBoundary(t *testing.T) {
	for _, fault := range []string{"drop", "disabled", "generation", "restarted", "closed-live-writer", "truncated", "unpublished-open"} {
		t.Run(fault, func(t *testing.T) {
			dir := t.TempDir()
			lease, err := BeginWriter(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			store, err := Open(dir, "synthetic", Limits{})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			store.Record(Event{Phase: "end", Verb: "get", Started: time.Now()}, nil)
			if _, err := ReadThrough(context.Background(), dir, time.Now()); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "drop":
				store.Gap()
			case "disabled":
				if err := os.WriteFile(filepath.Join(dir, "recording-disabled"), []byte("synthetic"), 0600); err != nil {
					t.Fatal(err)
				}
			case "generation":
				if err := BeginGeneration(dir); err != nil {
					t.Fatal(err)
				}
			case "restarted":
				store.Close()
				restarted, err := Open(dir, "synthetic", Limits{})
				if err != nil {
					t.Fatal(err)
				}
				defer restarted.Close()
			case "closed-live-writer":
				store.Close()
			case "truncated":
				store.Close()
				if err := os.Truncate(filepath.Join(dir, "events.jsonl"), 0); err != nil {
					t.Fatal(err)
				}
			case "unpublished-open":
				store.Close()
				b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
				if err != nil {
					t.Fatal(err)
				}
				var m Manifest
				if err := json.Unmarshal(b, &m); err != nil {
					t.Fatal(err)
				}
				m.Open = true
				m.SnapshotThrough = time.Time{}
				b, _ = json.Marshal(m)
				if err := os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0600); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			if _, err := ReadThrough(ctx, dir, time.Now()); err == nil {
				t.Fatal("unknown history granted closure authority")
			}
		})
	}
}
