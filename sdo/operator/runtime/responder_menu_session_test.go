package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

func TestSelectionSessionUsesProtoJSONAndRejectsOpaqueUnknownFields(t *testing.T) {
	var request ToolRequest
	if err := json.Unmarshal([]byte(`{"command":"report","report_query":{"version":1,"incident_id":"i","session_id":"s","dispatch_epoch":"18446744073709551615","attempt_id":"a"}}`), &request); err != nil {
		t.Fatal(err)
	}
	if request.ReportQuery.DispatchEpoch != ^uint64(0) {
		t.Fatal("uint64 epoch lost precision")
	}
	for _, payload := range []string{
		`{"command":"report","opaque":"SYNTHETIC-CANARY"}`,
		`{"command":"report","report_query":{"opaque":"SYNTHETIC-CANARY"}}`,
		`{"command":"mitigate","selection":null}`,
	} {
		if err := json.Unmarshal([]byte(payload), &request); err == nil || strings.Contains(err.Error(), "SYNTHETIC-CANARY") {
			t.Fatalf("invalid request accepted or echoed content: %v", err)
		}
	}
}
func TestSelectionSessionReportDeliverySurvivesLostPublication(t *testing.T) {
	ctx := context.Background()
	journal, store, menu, selection := selectionLedgerFixture(t)
	attempt, _, err := journal.accept(ctx, menu, selection)
	if err != nil {
		t.Fatal(err)
	}
	report, err := journal.settle(ctx, attempt.AttemptId, &contracts.MitigationReport{Version: 1, IncidentId: "incident", SessionId: "session", RequestId: "request", AttemptId: attempt.AttemptId, AcceptanceEpoch: 1, Status: contracts.MitigationReport_STATUS_INTERRUPTED}, 0)
	if err != nil {
		t.Fatal(err)
	}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident"}, nil, nil, nil, PlanExecutor{}, 5)
	loop.selection = journal
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	first := SessionToolServer{Directory: directory, Handle: loop.Handle}
	if err := first.Prepare(); err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	name := "lost-reply.json"
	if err := os.WriteFile(filepath.Join(directory, "requests", name), []byte(`{"command":"session-open"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := first.responses.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.process(ctx); err == nil {
		t.Fatal("publication failure not injected")
	}
	// A new controller adopts the incident. Terminal reports must not be
	// re-settled or require a health policy which may already be superseded.
	state, _, _ := store.Load(ctx)
	replacement := NewResponderLoop(IncidentRequest{IncidentID: "incident", SourceCommit: "source-b"}, nil, nil, nil, PlanExecutor{}, 50)
	if err := replacement.attachSelectionJournal(ctx, replacement.Request, store, state, true); err != nil {
		t.Fatal(err)
	}
	replacement.CheckHealth = func(context.Context) error { return ErrStaleHealthInvestigation }
	second := SessionToolServer{Directory: directory, Handle: replacement.Handle}
	if err := second.Prepare(); err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.process(ctx); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(directory, "responses", name))
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Session json.RawMessage `json:"session"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatal(err)
	}
	var session contracts.MitigationSession
	if err := protojson.Unmarshal(envelope.Session, &session); err != nil {
		t.Fatal(err)
	}
	if session.SessionId != "session" || session.ProviderSessionId != "provider-session" || session.DispatchEpoch != 2 || len(session.PendingReports) != 1 || !proto.Equal(session.PendingReports[0], report) {
		t.Fatalf("lost reply was not redelivered identically: %s", data)
	}
	if len(replacement.selection.pendingReports()) != 1 {
		t.Fatal("publication acknowledged without provider delivery")
	}
	if err := replacement.selection.acknowledge(ctx, attempt.AttemptId, "provider-session", false); err != nil {
		t.Fatal(err)
	}
	if len(replacement.selection.pendingReports()) != 0 {
		t.Fatal("provider acknowledgment did not drain outbox")
	}
}
func TestSelectionSessionDispatcherPersistsJournalAlongsideLegacyFacts(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "control"}
	_, _, menu, selection := selectionLedgerFixture(t)
	request := IncidentRequest{IncidentID: "incident", SourceCommit: "source-a", ResponseDeadline: menu.OriginalDeadline.AsTime()}
	loop := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 5)
	if err := dispatcher.restoreToolState(ctx, request, loop); err != nil {
		t.Fatal(err)
	}
	opened := loop.Handle(ctx, ToolRequest{Command: "session-open"})
	if opened.Error != "" {
		t.Fatal(opened.Error)
	}
	if err := loop.selection.checkpoint(ctx, "codex", "provider-session", strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	menu.SessionId = opened.Session.SessionId
	selection.SessionId = opened.Session.SessionId
	attempt, _, err := loop.selection.accept(ctx, menu, selection)
	if err != nil {
		t.Fatal(err)
	}
	// Ordinary tool-state saves must not erase accepted selections.
	if err := loop.persistToolState(ctx); err != nil {
		t.Fatal(err)
	}
	store := lifecycleio.NewConfigMapStateStore(client, "control", IncidentJobName("incident")+"-tools-state")
	state, _, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.GetSelectionLedger().GetAttempts()) != 1 || len(state.ResponderToolStateJson) == 0 {
		t.Fatal("legacy save clobbered selection journal")
	}
	request.SourceCommit = "source-b"
	replacement := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 50)
	if err := dispatcher.restoreToolState(ctx, request, replacement); err != nil {
		t.Fatal(err)
	}
	state = replacement.selection.state
	if replacement.sessionID != loop.sessionID || replacement.MaxBatches != 5 || state.SelectionLedger.Attempts[0].Report.Status != contracts.MitigationReport_STATUS_INTERRUPTED || !state.SelectionLedger.WritesFrozen {
		t.Fatal("dispatcher adoption lost history or allowed an unproven worker")
	}
	response := replacement.Handle(ctx, ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{Version: 1, IncidentId: "incident", SessionId: loop.sessionID, DispatchEpoch: 2, AttemptId: attempt.AttemptId}})
	if response.Error != "" || response.Report == nil || response.Report.AcceptanceEpoch != 1 {
		t.Fatalf("report query failed after adoption: %+v", response)
	}
}
