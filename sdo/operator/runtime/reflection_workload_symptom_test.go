package runtime

import (
	"context"
	"errors"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
)

// The scripted worker learns one manual Deployment repair while its accepted
// peer playbook covers the second. Each cause includes an unwritten Service
// whose opening endpoint finding clears after the Deployment patch. CLI and
// broker run the real compiler and detector -> binding -> repair -> NoMatch chain.
func TestReflectionLearnsDeploymentRepairWithUnwrittenServiceSymptoms(t *testing.T) {
	ctx := context.Background()
	const incidentID = "workload-symptom-learning"
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "sdo-operator")
	build := exec.Command("go", "build", "-o", binary, "./cmd/sdo-operator")
	build.Dir = operatorRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v: %s", err, output)
	}
	fixture := "../../../tests/fixtures/sdo/workload_symptom_learning/.sdo"
	target := nativeRepository(t)
	copyFastloopTree(t, "../memory/validate/testdata/good/.sdo", filepath.Join(target, ".sdo"))
	module, err := os.ReadFile("../../../tests/fixtures/sdo/playbook_program_app/.sdo/diagnostics/go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, ".sdo/diagnostics/go.mod"), module, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(target, ".sdo/playbooks/base/scripts/fix.sh"), 0755); err != nil {
		t.Fatal(err)
	}
	for _, relative := range []string{"playbooks/workload-host-port", "diagnostics/detectors/incidents/host-port-conflict", "corpus/workload-host-port"} {
		copyFastloopTree(t, filepath.Join(fixture, relative), filepath.Join(target, ".sdo", relative))
	}
	book := ".sdo/playbooks/extra-workload-listener"
	if err := os.MkdirAll(filepath.Join(target, book), 0755); err != nil {
		t.Fatal(err)
	}
	readme, err := os.ReadFile(filepath.Join(fixture, "playbooks/extra-workload-listener/README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, book, "README.md"), readme, 0644); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(target, ".sdo/playbooks/README.md")
	indexBytes, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(index, append(indexBytes, []byte("\n- [workload-host-port](workload-host-port/README.md)\n- [extra-workload-listener](extra-workload-listener/README.md)\n")...), 0644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, target, "add", "-A")
	nativeGit(t, target, "commit", "-qm", "accepted host-port repair and listener guidance")
	capture := t.TempDir()
	healthyBaseline := filepath.Join(capture, "healthy")
	if err := os.MkdirAll(healthyBaseline, 0700); err != nil {
		t.Fatal(err)
	}
	healthy, err := os.ReadFile(filepath.Join(fixture, "corpus/extra-workload-listener/healthy.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(healthyBaseline, "healthy.json"), healthy, 0400); err != nil {
		t.Fatal(err)
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
	opened := time.Unix(100, 0)
	mutation := opened.Add(20 * time.Second)
	verified := mutation.Add(10 * time.Second)
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: incidentID, Classification: "success", Timestamps: &contracts.OutcomeTimestamps{DetectedAt: timestamppb.New(opened), MitigatedAt: timestamppb.New(mutation), VerifiedAt: timestamppb.New(verified), CompletedAt: timestamppb.New(verified)}}
	for i, name := range []string{"recommendation", "search"} {
		detector := []string{"listener-conflict", "host-port-conflict"}[i]
		deployment := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: name}
		service := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "demo", Name: name}
		outcome.ConfirmedRootCauses = append(outcome.ConfirmedRootCauses, &contracts.ConfirmedRootCause{Summary: "Workload listener conflict; Service endpoints recovered after manual Deployment patch", Resources: []*contracts.ObjectRef{deployment, service}, ExplainedDetectors: []string{detector, "service-endpoints"}})
		outcome.Findings = append(outcome.Findings, &contracts.Finding{DetectorId: detector, RuleId: detector, Fingerprint: detector + "/" + name, Status: "active", Severity: "warn", Summary: "Observed workload symptom", Evidence: "Synthetic opening observation", PrimaryResource: deployment}, &contracts.Finding{DetectorId: "service-endpoints", RuleId: "no-endpoints", Fingerprint: "endpoints/" + name, Status: "active", Severity: "warn", Summary: "Observed workload symptom", Evidence: "Synthetic opening observation", PrimaryResource: service})
		outcome.RepairActions = append(outcome.RepairActions, &contracts.RepairActionReceipt{ActionId: "sdo-act-manual-" + name, Kind: "patch", Target: "Deployment/" + name, Details: "Synthetic controller-recorded Deployment patch", Success: true, Resources: []*contracts.ObjectRef{deployment}, StartedAt: timestamppb.New(mutation.Add(-time.Second)), CompletedAt: timestamppb.New(mutation), Summary: "Patched only the Deployment; observed Service recovery"})
		outcome.DetectorHistory = append(outcome.DetectorHistory, &contracts.DetectorEvaluation{DetectorId: detector, Status: "clear", EvaluatedAt: timestamppb.New(verified)})
	}
	outcome.DetectorHistory = append(outcome.DetectorHistory, &contracts.DetectorEvaluation{DetectorId: "service-endpoints", Status: "clear", EvaluatedAt: timestamppb.New(verified)})
	if _, err := repository.RecordOutcome(ctx, incidentID, outcome); err != nil {
		t.Fatal(err)
	}
	proposal, err := repository.CreateProposalForPhase(ctx, incidentID, broker.ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(target), Catalog: memory.OpenCatalogReader(target), Timeout: 5 * time.Minute, SessionMode: "fresh"}
	worker.Executor = reflectionTaskExecutorFunc(func(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		calls++
		request := new(contracts.ReflectionRequest)
		if err := task.Request.UnmarshalTo(request); err != nil {
			return nil, err
		}
		if len(request.Outcome.ConfirmedRootCauses) != 2 || len(request.Outcome.RepairActions) != 2 {
			t.Fatalf("lost composite hand repair: %v", request.Outcome)
		}
		for _, a := range request.Outcome.RepairActions {
			for _, r := range a.Resources {
				if r.Kind != "Deployment" {
					t.Fatalf("invented symptom write: %v", a)
				}
			}
		}
		checkout := task.Workspace.CheckoutPath
		decision := &contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"extra-workload-listener"}}
		publication := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: decision.ProposedArtifactIds}
		reject := func(rule string, publish bool) {
			t.Helper()
			writeReflectionCheckDecision(t, checkout, decision)
			report, code := runReflectionMemoryCheck(t, binary, checkout, proposal.BaseCommit, operatorRoot)
			found := false
			for _, d := range report.Diagnostics {
				found = found || d.Rule == rule
			}
			if code != 1 || !found {
				t.Fatalf("wanted CLI rejection %s, code%d diagnostics%v", rule, code, report.Diagnostics)
			}
			if !publish {
				return
			}
			_, publishErr := repository.PublishReflection(ctx, incidentID, publication)
			var failure *validate.Failure
			if !errors.As(publishErr, &failure) {
				t.Fatalf("unsafe fixture accepted by broker: %v", publishErr)
			}
			if !proto.Equal(&contracts.MemoryCheckReport{Diagnostics: report.Diagnostics}, &contracts.MemoryCheckReport{Diagnostics: validate.ProtoDiagnostics(failure.Diagnostics)}) {
				t.Fatalf("CLI/broker diagnostics differ: %v / %v", report.Diagnostics, failure.Diagnostics)
			}
		}
		// README-only guidance still cannot represent an executable repair.
		if err := os.WriteFile(filepath.Join(checkout, book, "README.md"), append(readme, []byte("\nObserved a successful manual workload repair.\n")...), 0644); err != nil {
			return nil, err
		}
		reject(validate.RuleReflectionProgramRequired, true)
		copyFastloopTree(t, fixture, filepath.Join(checkout, ".sdo"))
		// Negative controls exercise actual replay and production finding selection.
		// Compile each once through the shared checker; publication below proves
		// the same final bytes through the broker without duplicate failing builds.
		mitigation := filepath.Join(checkout, book, "mitigate.go")
		original, err := os.ReadFile(mitigation)
		if err != nil {
			return nil, err
		}
		noop := `package repair
import("context";"sdo.dev/operator/sdk/program")
func(Program) Mitigate(context.Context,program.ReadView,program.Bindings)(program.Plan,error){return program.Plan{},nil}
`
		formatted, err := format.Source([]byte(noop))
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(mitigation, formatted, 0644); err != nil {
			return nil, err
		}
		reject(validate.RuleProgramMitigationRepair, false)
		if err := os.WriteFile(mitigation, original, 0644); err != nil {
			return nil, err
		}
		detectorPath := filepath.Join(checkout, ".sdo/diagnostics/detectors/incidents/listener-conflict/detector.go")
		detector, err := os.ReadFile(detectorPath)
		if err != nil {
			return nil, err
		}
		unbound := strings.Replace(string(detector), "PrimaryResource: ref", "PrimaryResource: sdk.ObjectRef{APIVersion: \"v1\", Kind: \"Service\", Namespace: ref.Namespace, Name: ref.Name}", 1)
		if unbound == string(detector) {
			t.Fatal("finding-binding negative control did not alter detector")
		}
		formatted, err = format.Source([]byte(unbound))
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(detectorPath, formatted, 0644); err != nil {
			return nil, err
		}
		reject(validate.RuleProgramRepeatReachable, false)
		if err := os.WriteFile(detectorPath, detector, 0644); err != nil {
			return nil, err
		}
		writeReflectionCheckDecision(t, checkout, decision)
		report, code := runReflectionMemoryCheck(t, binary, checkout, proposal.BaseCommit, operatorRoot)
		if code != 0 {
			t.Fatalf("observed unwritten symptoms blocked executable repair: CLI %d %v", code, report.Diagnostics)
		}
		receipt, err := repository.PublishReflection(ctx, incidentID, publication)
		if err != nil {
			return nil, err
		}
		dispatcher := KubernetesJobDispatcher{Outcomes: memory.OpenReader(target)}
		stats, err := dispatcher.programTrust(ctx, IncidentRequest{RepositoryWorktree: target, SourceCommit: receipt.Commit})
		if err != nil || stats["extra-workload-listener"].VerifiedSuccesses != 1 {
			t.Fatalf("accepted symptom-aware repair lost origin credit: %v %v", stats, err)
		}
		for _, p := range []string{book + "/diagnose.go", book + "/mitigate.go", ".sdo/diagnostics/detectors/incidents/listener-conflict/detector.go"} {
			if content := nativeGit(t, target, "show", receipt.Commit+":"+p); strings.TrimSpace(content) == "" {
				t.Fatalf("missing accepted executable: %s", p)
			}
		}
		packed, err := anypb.New(decision)
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: packed}, err
	})
	workspace := IncidentWorkspace{IncidentID: incidentID, Worktree: proposal.Path, BaseCommit: proposal.BaseCommit}
	decision, err := worker.Reflect(ctx, nativeClosure(workspace), workspace, outcome, "manual-repair", proposal.BaseCommit, ReflectionFeedback{})
	if err != nil || calls != 1 || decision.LearningDecision != "updated" {
		t.Fatalf("scripted reflection failed: calls=%d decision=%+v err=%v", calls, decision, err)
	}
}
