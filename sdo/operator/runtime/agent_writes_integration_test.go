//go:build integration

package runtime

import (
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

// openRecording is the live recording of one incident, as the sidecar holds it.
func openRecording(t *testing.T, incident string) (root string, store *apihistory.Store) {
	t.Helper()
	root = t.TempDir()
	relative, err := memory.PrepareIncidentSession(root, "", incident)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, relative, "api-history")
	lock, err := apihistory.BeginWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lock.Close() })
	store, err = apihistory.Open(dir, incident, apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return root, store
}

func recordWrite(store *apihistory.Store, id string, at time.Time, event apihistory.Event, finish bool) {
	event.RequestID, event.Phase, event.Started, event.Reserved = id, "start", at, true
	store.Record(event, nil)
	if finish {
		event.Phase, event.Finished = "end", at.Add(time.Second)
		if event.Status == 0 {
			event.Status = 200
		}
		store.Record(event, nil)
	}
}

// The agent's own kubectl patch, with no `sdo act` journal anywhere, is a write: it moves Recovered's since.
func TestARawAgentPatchMovesRecoveredSince(t *testing.T) {
	root, store := openRecording(t, "inc")
	wrote := verifyT0.Add(90 * time.Second)
	recordWrite(store, "rawpatch01", wrote, apihistory.Event{Verb: "patch", Group: "apps", Version: "v1", Resource: "deployments", Namespace: "app", Name: "api"}, true)
	store.Close()
	receipts, pending, _, err := agentWriteReceipts(root, "inc", time.Now().UTC())
	if err != nil || pending || len(receipts) != 1 {
		t.Fatalf("receipts %v pending %v err %v", receipts, pending, err)
	}
	now := verifyT0.Add(2 * time.Minute)
	check := runVerify(t, verifyWorld{now: now}, []sdk.Finding{browseTest()}, 1, &IncidentResult{RepairActions: receipts})
	if got := check.record.GetSince().AsTime(); !got.Equal(wrote.Add(time.Second)) {
		t.Fatalf("since = %s, want the write's completion %s", got, wrote.Add(time.Second))
	}
}

// An exec session that is still open keeps the closure guard from reading the recording as settled.
func TestAnOpenExecSessionIsPending(t *testing.T) {
	root, store := openRecording(t, "inc")
	t.Cleanup(store.Close)
	recordWrite(store, "openexec01", verifyT0, apihistory.Event{Verb: "get", Version: "v1", Resource: "pods", Namespace: "app", Name: "db-0", Subresource: "exec"}, false)
	_, pending, _, err := agentWriteReceipts(root, "inc", time.Now().UTC())
	if err != nil || !pending {
		t.Fatalf("pending %v err %v", pending, err)
	}
}
