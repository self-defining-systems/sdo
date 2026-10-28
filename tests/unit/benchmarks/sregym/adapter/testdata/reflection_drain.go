// Synthetic closure through the real native broker, exported for Python consumers.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	root := os.Args[1]
	decision := os.Args[2]
	accepted := filepath.Join(root, "accepted")
	must(os.MkdirAll(accepted, 0755))
	for _, args := range [][]string{
		{"init", "-q"}, {"config", "user.name", "Synthetic Test"},
		{"config", "user.email", "test@example.invalid"},
	} {
		cmd := exec.Command("git", append([]string{"-C", accepted}, args...)...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			panic(fmt.Sprintf("%s: %v", output, err))
		}
	}
	must(os.WriteFile(filepath.Join(accepted, "app.txt"), []byte("synthetic\n"), 0644))
	for _, args := range [][]string{{"add", "."}, {"commit", "-qm", "synthetic base"}} {
		cmd := exec.Command("git", append([]string{"-C", accepted}, args...)...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			panic(fmt.Sprintf("%s: %v", output, err))
		}
	}
	repository, err := broker.New(accepted, filepath.Join(root, "proposals"), nil)
	must(err)
	ledgerRoot := filepath.Join(root, "ledger")
	native := runtime.NativeIncidentBroker{
		Repository: repository, LedgerRoot: ledgerRoot, Deriver: runtime.ProtoOutcomeDeriver{},
		ResponderBackend: "test", ResponderModel: "test",
		Reflector: runtime.ReflectionWorkerFunc(func(context.Context, runtime.IncidentClosure, runtime.IncidentWorkspace, *contracts.OutcomeRecord, string, string, runtime.ReflectionFeedback) (runtime.ReflectionDecision, error) {
			if decision == "failed" {
				return runtime.ReflectionDecision{LearningDecision: "failed", Error: "synthetic worker failure", FailureReason: "agent_failed"}, nil
			}
			return runtime.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "no independently verified repair"}, nil
		}),
	}
	ctx := context.Background()
	workspace, err := native.PrepareIncident(ctx, "incident-1")
	must(err)
	now := time.Now().UTC()
	closure := runtime.IncidentClosure{
		Request: runtime.IncidentRequest{
			SchemaVersion: runtime.ProtocolSchemaVersion, Application: "synthetic", Namespace: "demo", IncidentID: workspace.IncidentID,
			SourceCommit: workspace.BaseCommit, DeployedCommit: workspace.BaseCommit,
			Findings:           []sdk.Finding{{DetectorID: "health", RuleID: "health", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "synthetic fault", Evidence: "synthetic evidence", PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Name: "app"}, Fingerprint: "health"}},
			DetectorHistory:    []runtime.DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now, Status: runtime.DetectorEvaluationFiring}},
			RepositoryWorktree: workspace.Worktree, RepositoryBaseCommit: workspace.BaseCommit,
			ResponseDeadline: now.Add(time.Minute), CancellationToken: "synthetic", RepairPolicy: "recorded-actions",
		},
		Result: &runtime.IncidentResult{
			SchemaVersion: runtime.ProtocolSchemaVersion, IncidentID: workspace.IncidentID,
			Status: runtime.IncidentFailed, ResponderSessionID: "synthetic-session",
			Timing: runtime.TimingMetrics{StartedAt: now, CompletedAt: now.Add(time.Second)},
		},
		DetectedAt: now, DispatchedAt: now, ResponderCompletedAt: now.Add(time.Second), VerifiedAt: now.Add(time.Second),
	}
	receipt, err := native.ProcessClosure(ctx, closure)
	must(err)
	must(native.AcknowledgeClosure(ctx, receipt))
	sample := func() map[string]any {
		queued, err := native.QueuedReflections()
		must(err)
		entries, err := os.ReadDir(ledgerRoot)
		must(err)
		var ledger map[string]any
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) != ".json" {
				continue
			}
			data, err := os.ReadFile(filepath.Join(ledgerRoot, entry.Name()))
			must(err)
			must(json.Unmarshal(data, &ledger))
		}
		return map[string]any{
			"state":   map[string]any{"last_acknowledged_incident_id": workspace.IncidentID, "queued_reflection_incident_ids": queued},
			"receipt": map[string]any{"schema_version": "sdo.production-receipt/v2", "incident_id": workspace.IncidentID, "native_ledger": ledger},
		}
	}
	before := sample()
	must(native.ProcessQueuedReflection(ctx, workspace.IncidentID))
	must(json.NewEncoder(os.Stdout).Encode([]map[string]any{before, sample()}))
}
