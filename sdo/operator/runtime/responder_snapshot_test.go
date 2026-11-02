package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Exercise the same result projection used by controller closure while real
// responder commands and restart journal restoration change the live session.
func TestControllerClosureSnapshotConcurrentResponderUpdates(t *testing.T) {
	loop, _ := menuB3Loop(t)
	request := loop.Request
	start := make(chan struct{})
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		<-start
		for i := 0; i < 100; i++ {
			response := loop.Handle(context.Background(), ToolRequest{Command: "status"})
			if response.Error != "" {
				t.Errorf("status: %s", response.Error)
				return
			}
			state := fmt.Sprintf(`{"Version":2,"IncidentID":%q,"SourceCommit":%q,"SessionID":"session","MenuSession":{"session_id":"session","incident_id":%q,"provider_session_id":"provider-%d"}}`, request.IncidentID, request.SourceCommit, request.IncidentID, i)
			if err := loop.restoreToolSnapshot([]byte(state)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	close(start)
	for i := 0; i < 100; i++ {
		snapshot := earlyResultSnapshot(loop, request, time.Now().UTC())
		if snapshot.IncidentID != request.IncidentID || snapshot.Status != IncidentCompleted {
			t.Errorf("wrong closure identity: %+v", snapshot)
		}
	}
	writers.Wait()
	snapshot := earlyResultSnapshot(loop, request, time.Now().UTC())
	if len(snapshot.ProgramToolCalls) != 100 || snapshot.ResponderSessionID != "provider-99" {
		t.Fatalf("lost responder state: calls=%d session=%q", len(snapshot.ProgramToolCalls), snapshot.ResponderSessionID)
	}
}

// Dispatch setup is a writer too, even before a worker can make its first call.
func TestControllerClosureSnapshotConcurrentDispatchInitialization(t *testing.T) {
	loop, _ := menuB3Loop(t)
	request := loop.Request
	start := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		<-start
		for i := 0; i < 100; i++ {
			loop.initializeDispatch(nil, i+1, []ProgramRun{{ProgramID: fmt.Sprintf("program-%d", i)}})
		}
	}()
	close(start)
	for i := 0; i < 100; i++ {
		snapshot := earlyResultSnapshot(loop, request, time.Now().UTC())
		if len(snapshot.ProgramRuns) > 1 {
			t.Errorf("dispatch setup mixed separate run inventories: %+v", snapshot.ProgramRuns)
		}
	}
	writer.Wait()
	snapshot := earlyResultSnapshot(loop, request, time.Now().UTC())
	if len(snapshot.ProgramRuns) != 1 || snapshot.ProgramRuns[0].ProgramID != "program-99" {
		t.Fatalf("lost initialized program runs: %+v", snapshot.ProgramRuns)
	}
}
