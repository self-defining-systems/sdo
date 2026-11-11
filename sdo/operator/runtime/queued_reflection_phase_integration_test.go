//go:build integration

package runtime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/telemetry"
)

func queuedReflectionGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	output, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return string(output)
}

// queuedReflectionLedger closes one synthetic failed attempt, whose reflection
// is queued behind the acknowledgment, runs the queued reflection with the
// given worker and returns the ledger it leaves.
func queuedReflectionLedger(t *testing.T, root string, worker ReflectionWorkerFunc) *contracts.BrokerLedger {
	t.Helper()
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
	native := NativeIncidentBroker{
		Repository: repository, LedgerRoot: filepath.Join(root, "ledger"), Deriver: ProtoOutcomeDeriver{},
		ResponderBackend: "test", ResponderModel: "test", Reflector: worker,
	}
	ctx := context.Background()
	workspace, err := native.PrepareIncident(ctx, "incident-1")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	closure := IncidentClosure{
		Request: IncidentRequest{
			SchemaVersion: ProtocolSchemaVersion, Application: "synthetic", Namespace: "demo", IncidentID: workspace.IncidentID,
			SourceCommit: workspace.BaseCommit, DeployedCommit: workspace.BaseCommit,
			Findings:           []sdk.Finding{{DetectorID: "health", RuleID: "health", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "synthetic fault", Evidence: "synthetic evidence", PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Name: "app"}, Fingerprint: "health"}},
			DetectorHistory:    []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now, Status: DetectorEvaluationFiring}},
			RepositoryWorktree: workspace.Worktree, RepositoryBaseCommit: workspace.BaseCommit,
			ResponseDeadline: now.Add(time.Minute), CancellationToken: "synthetic", RepairPolicy: "recorded-actions",
		},
		Result: &IncidentResult{
			SchemaVersion: ProtocolSchemaVersion, IncidentID: workspace.IncidentID,
			Status: IncidentFailed, ResponderSessionID: "synthetic-session",
			Timing: TimingMetrics{StartedAt: now, CompletedAt: now.Add(time.Second)},
		},
		DetectedAt: now, DispatchedAt: now, ResponderCompletedAt: now.Add(time.Second), VerifiedAt: now.Add(time.Second),
	}
	receipt, err := native.ProcessClosure(ctx, closure)
	if err != nil {
		t.Fatal(err)
	}
	if err := native.AcknowledgeClosure(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if err := native.ProcessQueuedReflection(ctx, workspace.IncidentID); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger(workspace.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	return ledger
}

// A queued reflection that published nothing (a no-op, or a decision Go
// turned into a failure) must not report a commit; only a reflection whose
// commit the accepted branch contains is "reflection_committed".
func TestQueuedReflectionPhaseNamesWhatReachedTheAcceptedBranch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		decision ReflectionDecision
		phase    string
	}{
		{"no change", ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "no independently verified repair"}, "reflection_without_commit"},
		// R3b: a README-only "updated" on a closure without a verified repaired
		// cause becomes a failed decision, and nothing is published.
		{"updated without a verified cause", ReflectionDecision{LearningDecision: "updated"}, "reflection_failed"},
		{"worker failure", ReflectionDecision{LearningDecision: "failed", Error: "synthetic worker failure", FailureReason: "agent_failed"}, "reflection_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ledger := queuedReflectionLedger(t, t.TempDir(), func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
				return tc.decision, nil
			})
			if ledger.GetReflectionCommit() != "" {
				t.Fatalf("reflection commit = %q, want none", ledger.GetReflectionCommit())
			}
			phases := ledger.GetClosurePhaseTimestamps()
			if _, ok := phases["reflection_committed"]; ok {
				t.Fatalf("reflection_committed recorded without a reflection commit: %v", phases)
			}
			if _, ok := phases[tc.phase]; !ok {
				t.Fatalf("phase %q not recorded: %v", tc.phase, phases)
			}
		})
	}
}

func TestQueuedReflectionCommittedOnlyWhenAcceptedBranchHasTheCommit(t *testing.T) {
	root := t.TempDir()
	accepted := filepath.Join(root, "accepted")
	ledger := queuedReflectionLedger(t, root, func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
		// Stand in for a broker publication that already reached the branch.
		queuedReflectionGit(t, accepted, "commit", "-q", "--allow-empty", "-m", "sdo(incident-1): validated reflection\n\nSDO-Incident: incident-1\nSDO-Phase: reflection")
		return ReflectionDecision{LearningDecision: "updated"}, nil
	})
	head := queuedReflectionGit(t, accepted, "rev-parse", "HEAD")
	if ledger.GetReflectionCommit() == "" || ledger.GetReflectionCommit()+"\n" != head {
		t.Fatalf("reflection commit = %q, accepted head %q", ledger.GetReflectionCommit(), head)
	}
	if _, ok := ledger.GetClosurePhaseTimestamps()["reflection_committed"]; !ok {
		t.Fatalf("reflection_committed not recorded: %v", ledger.GetClosurePhaseTimestamps())
	}
}

// The host must serve the accepted HEAD once a queued reflection committed:
// HEAD contains the reflection and whatever was accepted after it (another
// incident's outcome closing while the reflection ran). Activating the
// reflection commit itself after a newer publication can never stage (the
// accepted revision moved) and wedged the controller with that closure
// committed but never acknowledged.
func TestQueuedReflectionServesTheAcceptedHeadAfterALaterPublication(t *testing.T) {
	root := t.TempDir()
	accepted := filepath.Join(root, "accepted")
	ledger := queuedReflectionLedger(t, root, func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
		queuedReflectionGit(t, accepted, "commit", "-q", "--allow-empty", "-m", "sdo(incident-1): validated reflection\n\nSDO-Incident: incident-1\nSDO-Phase: reflection")
		return ReflectionDecision{LearningDecision: "updated"}, nil
	})
	reflection := ledger.GetReflectionCommit()
	queuedReflectionGit(t, accepted, "commit", "-q", "--allow-empty", "-m", "sdo(incident-2): validated outcome")
	head := strings.TrimSpace(queuedReflectionGit(t, accepted, "rev-parse", "HEAD"))
	repository, err := broker.New(accepted, filepath.Join(root, "proposals"), nil, broker.ObservationPolicy{Telemetry: broker.Telemetry{Registry: telemetry.NewRegistry()}})
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(root, "ledger")}
	got, err := native.QueuedReflectionServingCommit(context.Background(), "incident-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != head || got == reflection {
		t.Fatalf("serving commit = %q, want the accepted head %q (reflection %q)", got, head, reflection)
	}
}
