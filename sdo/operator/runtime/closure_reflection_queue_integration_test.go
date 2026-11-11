//go:build integration

package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/telemetry"
)

// A verified repair's reflection runs after the closure is acknowledged, like a
// failed attempt's: the controller handles one incident at a time, and a
// reflection inside the closure held it pending for the agent's whole run.
func TestSuccessReflectionRunsAfterTheClosureIsAcknowledged(t *testing.T) {
	root := t.TempDir()
	accepted := filepath.Join(root, "accepted")
	if err := os.MkdirAll(accepted, 0o755); err != nil {
		t.Fatal(err)
	}
	queuedReflectionGit(t, accepted, "init", "-q")
	queuedReflectionGit(t, accepted, "config", "user.name", "Synthetic Test")
	queuedReflectionGit(t, accepted, "config", "user.email", "test@example.invalid")
	if err := os.WriteFile(filepath.Join(accepted, "app.txt"), []byte("synthetic\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	queuedReflectionGit(t, accepted, "add", ".")
	queuedReflectionGit(t, accepted, "commit", "-qm", "synthetic base")
	repository, err := broker.New(accepted, filepath.Join(root, "proposals"), nil, broker.ObservationPolicy{Telemetry: broker.Telemetry{Registry: telemetry.NewRegistry()}})
	if err != nil {
		t.Fatal(err)
	}
	reflections := 0
	native := NativeIncidentBroker{
		Repository: repository, LedgerRoot: filepath.Join(root, "ledger"), Deriver: ProtoOutcomeDeriver{},
		ResponderBackend: "test", ResponderModel: "test",
		Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
			reflections++
			return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "synthetic"}, nil
		}),
	}
	ctx := context.Background()
	workspace, err := native.PrepareIncident(ctx, "incident-1")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	app := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "app"}
	closure := IncidentClosure{
		Request: IncidentRequest{
			SchemaVersion: ProtocolSchemaVersion, Application: "synthetic", Namespace: "demo", IncidentID: workspace.IncidentID,
			SourceCommit: workspace.BaseCommit, DeployedCommit: workspace.BaseCommit,
			Findings:           []sdk.Finding{{DetectorID: "health", RuleID: "health", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "synthetic fault", Evidence: "synthetic evidence", PrimaryResource: app, Fingerprint: "health"}},
			DetectorHistory:    []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now, Status: DetectorEvaluationFiring}},
			RepositoryWorktree: workspace.Worktree, RepositoryBaseCommit: workspace.BaseCommit,
			ResponseDeadline: now.Add(time.Minute), CancellationToken: "synthetic", RepairPolicy: "recorded-actions",
		},
		Result: &IncidentResult{
			SchemaVersion: ProtocolSchemaVersion, IncidentID: workspace.IncidentID,
			Status: IncidentCompleted, ResponderSessionID: "synthetic-session",
			RepairActions:       []RepairActionReceipt{{ActionID: "sdo-act-1", Kind: "kubectl-rollout-restart", Target: "Deployment/demo/app", Resources: []sdk.ObjectRef{app}, Summary: "restart", Details: "synthetic receipt", Success: true, StartedAt: now, CompletedAt: now}},
			FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now.Add(time.Second), Status: DetectorEvaluationClear}},
			Timing:              TimingMetrics{StartedAt: now, CompletedAt: now.Add(time.Second)},
		},
		FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now.Add(time.Second), Status: DetectorEvaluationClear}},
		DetectedAt:          now, DispatchedAt: now, ResponderCompletedAt: now.Add(time.Second), VerifiedAt: now.Add(time.Second),
	}
	receipt, err := native.ProcessClosure(ctx, closure)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger(workspace.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if ledger.GetOutcomeDocument().GetClassification() != "success" {
		t.Fatalf("classification = %q, want a success fixture", ledger.GetOutcomeDocument().GetClassification())
	}
	if reflections != 0 || !ledger.GetReflectionQueued() {
		t.Fatalf("reflections during closure = %d, queued = %v; want none during closure and the reflection queued", reflections, ledger.GetReflectionQueued())
	}
	if err := native.AcknowledgeClosure(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if err := native.ProcessQueuedReflection(ctx, workspace.IncidentID); err != nil {
		t.Fatal(err)
	}
	if reflections == 0 {
		t.Fatal("the queued reflection did not run after the acknowledgment")
	}
}
