//go:build integration

package apihistory

import (
	"context"
	"testing"
	"time"
)

func TestOnlyLostWriteRecordsAreWriteGaps(t *testing.T) {
	store, dir := openTestStore(t, Limits{Events: 1})
	read := Event{Phase: "end", Verb: "get", Resource: "pods", Started: time.Now(), Finished: time.Now(), Status: 200}
	store.Record(read, nil)
	// The cap is spent: a lost read and a lost body are gaps, not write gaps.
	store.Record(read, nil)
	store.Gap()
	store.Close()
	history, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if history.Manifest.WriteGaps != 0 || history.Manifest.Dropped == 0 {
		t.Fatalf("write gaps %d dropped %d: lost reads must not be write gaps", history.Manifest.WriteGaps, history.Manifest.Dropped)
	}
	store, dir = openTestStore(t, Limits{Events: 1})
	store.Record(read, nil)
	write := Event{Phase: "end", Verb: "patch", Resource: "deployments", Name: "api", Started: time.Now(), Finished: time.Now(), Status: 200}
	store.Record(write, nil)
	store.Close()
	history, err = Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if history.Manifest.WriteGaps != 1 || history.Manifest.WriteGapAt.IsZero() {
		t.Fatalf("a lost write record is a write gap at its time: %+v", history.Manifest)
	}
}

func TestRecorderRestartIsAWriteGapAtTheRestart(t *testing.T) {
	store, dir := openTestStore(t, Limits{})
	store.Close()
	before := time.Now().UTC().Add(-time.Second)
	reopened, err := Open(dir, "incident", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
	history, err := ReadThrough(context.Background(), dir, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if history.Manifest.WriteGaps != 1 || history.Manifest.WriteGapAt.Before(before) {
		t.Fatalf("a restart must restart settlement at the restart: %+v", history.Manifest)
	}
}

func TestReadThroughToleratesLostBodiesAndReportsAStaleGeneration(t *testing.T) {
	store, dir := openTestStore(t, Limits{})
	store.Record(Event{Phase: "end", Verb: "get", Resource: "pods", Started: time.Now(), Finished: time.Now(), Status: 200}, make([]byte, MaxProjectionBytes+1))
	store.Close()
	history, err := ReadThrough(context.Background(), dir, time.Now().UTC())
	if err != nil {
		t.Fatalf("a lost body must not fail the read: %v", err)
	}
	if history.Manifest.WriteGaps != 0 {
		t.Fatalf("an oversized read response is not a write gap: %+v", history.Manifest)
	}
	if err := BeginGeneration(dir); err != nil {
		t.Fatal(err)
	}
	history, err = ReadThrough(context.Background(), dir, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if history.Manifest.WriteGaps == 0 {
		t.Fatalf("a stale generation lost its write records: %+v", history.Manifest)
	}
}
