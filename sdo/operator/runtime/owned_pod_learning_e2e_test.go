package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
)

// The first encounter of a hostPort conflict: the responder patches the
// Deployment, whose rollout replaces the Pending Pod, and names both in its
// cause. The closure runs through the real broker, outcome record and scripted
// reflection; publication runs the real compiler and the detector -> binding
// -> repair -> NoMatch corpus replay that a repeat relies on.
func TestFirstEncounterNamingReplacedPodPublishesWorkloadPlaybook(t *testing.T) {
	ctx := context.Background()
	const incidentID = "workload-symptom-learning"
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	fixture := "../../../tests/fixtures/sdo/workload_symptom_learning/.sdo"
	target := nativeRepository(t)
	copyFastloopTree(t, "../memory/validate/testdata/good/.sdo", filepath.Join(target, ".sdo"))
	module, err := os.ReadFile("../../../tests/fixtures/sdo/playbook_program_app/.sdo/diagnostics/go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, ".sdo/diagnostics/go.mod"), module, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(target, ".sdo/playbooks/base/scripts/fix.sh"), 0o755); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, target, "add", "-A")
	nativeGit(t, target, "commit", "-qm", "memory without a host-port playbook")

	sessions := t.TempDir()
	capture, err := memory.IncidentCorpusDir(sessions, incidentID)
	if err != nil {
		t.Fatal(err)
	}
	healthyBaseline := filepath.Join(capture, "healthy")
	if err := os.MkdirAll(healthyBaseline, 0o700); err != nil {
		t.Fatal(err)
	}
	healthy, err := os.ReadFile(filepath.Join(fixture, "corpus/workload-host-port/healthy.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(healthyBaseline, "healthy.json"), healthy, 0o400); err != nil {
		t.Fatal(err)
	}
	opened := time.Now().UTC().Add(-time.Minute)
	mutation := opened.Add(20 * time.Second)
	verified := mutation.Add(10 * time.Second)
	// Controller captures: the opening Pod belongs to the old ReplicaSet of the
	// Deployment; after the rollout only a new Pod of a new ReplicaSet exists.
	for phase, at := range map[string]time.Time{"open@detect": opened, "fixed": verified} {
		source := map[string]string{"open@detect": "open", "fixed": "fixed"}[phase]
		raw, err := os.ReadFile(filepath.Join(fixture, "corpus/workload-host-port", source+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var snapshot map[string]any
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		set, pod := "search-old", "search-old-abcde"
		if phase == "fixed" {
			set, pod = "search-new", "search-new-fghij"
		}
		controlled := func(kind, name string) []any {
			return []any{map[string]any{"apiVersion": "apps/v1", "kind": kind, "name": name, "controller": true}}
		}
		snapshot["replicaSets"] = []any{map[string]any{"metadata": map[string]any{"namespace": "demo", "name": set, "ownerReferences": controlled("Deployment", "search")}}}
		snapshot["pods"] = []any{map[string]any{"metadata": map[string]any{"namespace": "demo", "name": pod, "ownerReferences": controlled("ReplicaSet", set)}}}
		snapshot["observedAt"] = at
		raw, err = json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(capture, phase+".json"), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("SDO_CAPTURED_CORPUS_DIR", capture)
	t.Setenv("SDO_INCIDENT_ID", incidentID)
	ctx = validate.WithCaptureSource(ctx, capture, incidentID)
	compile := func(ctx context.Context, root string, _ []string) error {
		ds, err := validate.CompileCandidate(ctx, validate.Request{Root: root, OperatorRoot: operatorRoot, HealthyBaseline: healthyBaseline})
		if err != nil {
			return err
		}
		return validate.Err(ds)
	}
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), compile)
	if err != nil {
		t.Fatal(err)
	}

	deployment := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "search"}
	replaced := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Pod", Namespace: "demo", Name: "search-old-abcde"}
	calls := 0
	worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(target), Catalog: memory.OpenCatalogReader(target), Timeout: 5 * time.Minute, SessionMode: "fresh"}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		calls++
		checkout := filepath.Join(task.Workspace.CheckoutPath, ".sdo")
		for _, relative := range []string{"playbooks/workload-host-port", "diagnostics/detectors/incidents/host-port-conflict", "corpus/workload-host-port"} {
			copyFastloopTree(t, filepath.Join(fixture, relative), filepath.Join(checkout, relative))
		}
		index := filepath.Join(checkout, "playbooks/README.md")
		indexBytes, err := os.ReadFile(index)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(index, append(indexBytes, []byte("\n- [workload-host-port](workload-host-port/README.md)\n")...), 0o644); err != nil {
			return nil, err
		}
		decision := &contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"workload-host-port"}}
		writeReflectionCheckDecision(t, task.Workspace.CheckoutPath, decision)
		packed, err := anypb.New(decision)
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: packed}, err
	})
	native := NativeIncidentBroker{
		Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), SessionRoot: sessions, Reflector: worker,
		ResponderBackend: "scripted", ResponderModel: "none",
		Deriver: OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			return DerivedOutcome{RequiresReflection: true, Record: &contracts.OutcomeRecord{
				SchemaVersion: 1, IncidentId: closure.Request.IncidentID, Classification: "success",
				Timestamps:          &contracts.OutcomeTimestamps{DetectedAt: timestamppb.New(opened), MitigatedAt: timestamppb.New(mutation), VerifiedAt: timestamppb.New(verified), CompletedAt: timestamppb.New(verified)},
				ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "An injected hostPort left the search Pod Pending", Resources: []*contracts.ObjectRef{deployment, replaced}, ExplainedDetectors: []string{"host-port-conflict"}}},
				Findings:            []*contracts.Finding{{DetectorId: "host-port-conflict", RuleId: "host-port-conflict", Fingerprint: "host-port-conflict/search", Status: "active", Severity: "warn", Summary: "Unexpected workload listener", Evidence: "Pending Pod: hostPort conflict", PrimaryResource: deployment}},
				RepairActions:       []*contracts.RepairActionReceipt{{ActionId: "sdo-act-manual-search", Kind: "patch", Target: "Deployment/search", Details: "controller-recorded Deployment patch", Summary: "removed the conflicting hostPort", Success: true, Resources: []*contracts.ObjectRef{deployment}, StartedAt: timestamppb.New(mutation.Add(-time.Second)), CompletedAt: timestamppb.New(mutation)}},
				DetectorHistory:     []*contracts.DetectorEvaluation{{DetectorId: "host-port-conflict", Status: "clear", EvaluatedAt: timestamppb.New(verified)}},
			}}, nil
		}),
	}
	workspace, err := native.PrepareIncident(ctx, incidentID)
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.RepairPolicy = "recorded-actions"
	receipt, err := native.ProcessClosure(ctx, closure)
	if err != nil {
		t.Fatal(err)
	}
	if calls == 0 || receipt.ReflectionCommit == "" {
		ledger, _ := native.readLedger(incidentID)
		t.Fatalf("workload playbook not published (reflections=%d): %v", calls, ledger.GetReflectionDecision())
	}
	for _, path := range []string{".sdo/playbooks/workload-host-port/diagnose.go", ".sdo/playbooks/workload-host-port/mitigate.go", ".sdo/diagnostics/detectors/incidents/host-port-conflict/detector.go"} {
		if content := nativeGit(t, target, "show", receipt.ReflectionCommit+":"+path); content == "" {
			t.Fatalf("accepted reflection omitted %s", path)
		}
	}
}
