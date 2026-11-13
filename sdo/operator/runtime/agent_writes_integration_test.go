//go:build integration

package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
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

// The agent's own kubectl patch is a write: it moves Recovered's since.
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

func recordedExecClosure(t *testing.T, incident string) IncidentClosure {
	t.Helper()
	result := incidentResultState(&contractsv1alpha1.IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentId: incident, Status: string(IncidentCompleted)})
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	return IncidentClosure{
		Request: IncidentRequest{
			SchemaVersion: ProtocolSchemaVersion, Application: "app", Namespace: "default", IncidentID: incident,
			SourceCommit: "base", DeployedCommit: "base", RepositoryWorktree: "/proposal", RepositoryBaseCommit: "base",
			ResponseDeadline: now.Add(time.Minute), CancellationToken: "token", RepairPolicy: "commit",
			Findings: []sdk.Finding{{DetectorID: "health", RuleID: "health", Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
				Summary: "fault", Evidence: "evidence", PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Name: "health"}, Fingerprint: "health"}},
		},
		Result:     &result,
		DetectedAt: now, DispatchedAt: now, ResponderCompletedAt: now.Add(time.Second),
		Frozen: true,
	}
}

// With agent.recording off the outcome deriver never reads a recording, so an exec the agent ran moves no
// clock and becomes no repair record; with it on the same history is read (the control).
func TestOutcomeDeriverReadsNoRecordingWhenRecordingIsOff(t *testing.T) {
	root, store := openRecording(t, "inc-off")
	recordWrite(store, "exec000001", verifyT0, apihistory.Event{Verb: "get", Version: "v1", Resource: "pods", Namespace: "app", Name: "db-0", Subresource: "exec", Status: 101}, true)
	store.Close()
	closure := recordedExecClosure(t, "inc-off")
	writes := func(recording bool) int {
		derived, err := (ProtoOutcomeDeriver{SessionRoot: root, Recording: recording}).Derive(context.Background(), closure, "", "test", "test")
		if err != nil {
			t.Fatal(err)
		}
		return len(derived.Record.GetRepairActions())
	}
	if got := writes(true); got != 1 {
		t.Fatalf("recording on: %d repair actions, want the recorded exec", got)
	}
	if got := writes(false); got != 0 {
		t.Fatalf("recording off: %d repair actions, want none", got)
	}
}

// A responder that only ran an exec leaves no receipt with the recording off: the stability window is timed from
// the alerts clearing, and the incident still reaches Recovered.
func TestAResponderWithNoKnownWritesStillReachesRecovered(t *testing.T) {
	now := verifyT0.Add(2 * time.Minute)
	check := runVerify(t, verifyWorld{now: now}, []sdk.Finding{browseTest()}, 1, &IncidentResult{})
	if check.verdict != recoveryVerified {
		t.Fatalf("verdict %v: %s", check.verdict, check.reason)
	}
}
