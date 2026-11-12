//go:build integration

package apihistory

import (
	"testing"
	"time"
)

// An admitted write's start and end envelopes are stored even when blobs have used the byte budget and the
// ordinary queue is full: the reservation is separate from both.
func TestAdmittedEnvelopesSurviveFullBlobBudgetAndQueue(t *testing.T) {
	store, dir := openTestStore(t, Limits{Bytes: 600, Events: 4, Queue: 2})
	for i := 0; i < 4; i++ {
		store.Record(Event{Started: time.Now(), Phase: "end", Verb: "get", Resource: "pods", Status: 200}, nil)
	}
	time.Sleep(300 * time.Millisecond)
	if err := store.Admit(); err != nil {
		t.Fatal(err)
	}
	store.Record(Event{Started: time.Now(), Phase: "start", RequestID: "w1", Verb: "patch", Resource: "deployments", Name: "api", Reserved: true}, nil)
	store.Record(Event{Started: time.Now(), Phase: "end", RequestID: "w1", Verb: "patch", Resource: "deployments", Name: "api", Status: 200, Reserved: true}, nil)
	store.Close()
	history, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	var starts, ends int
	for _, e := range history.Events {
		if e.RequestID == "w1" && e.Phase == "start" {
			starts++
		}
		if e.RequestID == "w1" && e.Phase == "end" {
			ends++
		}
	}
	if starts != 1 || ends != 1 {
		t.Fatalf("admitted write stored %d start and %d end envelopes", starts, ends)
	}
}
