//go:build integration

package runtime

import (
	"context"
	"google.golang.org/protobuf/types/known/anypb"
	"path/filepath"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

// Exercise the native broker with hand-constructed worker responses, not a recording.
func TestSyntheticWorkerResultReachesNativeBroker(t *testing.T) {
	t.Parallel()
	responderPayload, err := anypb.New(syntheticIncidentResult())
	if err != nil {
		t.Fatal(err)
	}
	responder := &contracts.AgentResult{Response: responderPayload}
	typed := &contracts.IncidentResult{}
	if err := responder.GetResponse().UnmarshalTo(typed); err != nil {
		t.Fatal(err)
	}
	result := incidentResultState(typed)
	result.RepairActions = []RepairActionReceipt{{ActionID: "synthetic-repair", Kind: "patch", Target: "Service/app/reservation", Summary: "synthetic repair", Details: "test-only patch", Success: true, StartedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), CompletedAt: time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)}}
	result.Timing = TimingMetrics{StartedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), CompletedAt: time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)}
	accepted := nativeRepository(t)
	repository, err := broker.New(accepted, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "synthetic", ResponderModel: "none"}
	workspace, err := native.PrepareIncident(context.Background(), result.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.Namespace = "app"
	closure.Request.RepairPolicy = "recorded-actions"
	closure.Request.Findings = []sdk.Finding{{DetectorID: "health-objective", RuleID: "availability", Summary: "service unavailable", Evidence: "service has no ready endpoint", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "reservation"}}}
	closure.Request.DetectorHistory = []DetectorEvaluation{{DetectorID: "health-objective", EvaluatedAt: result.Timing.StartedAt, Status: DetectorEvaluationFiring}}
	closure.Result = &result
	closure.DetectedAt = result.Timing.StartedAt.Add(-time.Second)
	closure.DispatchedAt = result.Timing.StartedAt
	closure.ResponderCompletedAt = result.Timing.CompletedAt
	closure.VerifiedAt = result.Timing.CompletedAt.Add(time.Second)
	cleared := result.Timing.CompletedAt
	closure.HealthClearedAt = &cleared
	closure.FinalDetectorStates = []DetectorEvaluation{{DetectorID: "health-objective", EvaluatedAt: closure.VerifiedAt, Status: DetectorEvaluationClear}}
	receipt, err := native.ProcessClosure(context.Background(), closure)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := memory.OpenReader(accepted).Get(context.Background(), result.IncidentID)
	if err != nil || receipt.OutcomeCommit == "" || stored.Record.GetClassification() != "false_positive" {
		t.Fatalf("synthetic results did not reach accepted outcome: %+v %v", stored.Record, err)
	}
}
