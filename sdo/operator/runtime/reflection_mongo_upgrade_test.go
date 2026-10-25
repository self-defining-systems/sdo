package runtime

import (
	"context"
	"encoding/json"
	"errors"
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

// A scripted reflector upgrades an accepted README after a hand repair. The
// real CLI and broker both require the executable, compile the current SDK and
// prove detector -> finding binding -> diagnosis -> catalog repair -> NoMatch.
// No model, MongoDB server, credentials or cluster is involved.
func TestReflectionMongoAuthUpgradesReadmeThroughCLIAndBroker(t *testing.T) {
	ctx := context.Background()
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "sdo-operator")
	build := exec.Command("go", "build", "-p", "8", "-o", binary, "./cmd/sdo-operator")
	build.Dir = operatorRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v: %s", err, output)
	}
	target := nativeRepository(t)
	copyFastloopTree(t, "../memory/validate/testdata/good/.sdo", filepath.Join(target, ".sdo"))
	fixture := "../../../tests/fixtures/sdo/mongo_auth_upgrade/.sdo"
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
	book := ".sdo/playbooks/mongo-auth-bootstrap"
	if err := os.MkdirAll(filepath.Join(target, book), 0755); err != nil {
		t.Fatal(err)
	}
	readme, err := os.ReadFile(filepath.Join(fixture, "playbooks/mongo-auth-bootstrap/README.md"))
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
	if err := os.WriteFile(index, append(indexBytes, []byte("\n- [mongo-auth-bootstrap](mongo-auth-bootstrap/README.md)\n")...), 0644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, target, "add", "-A")
	nativeGit(t, target, "commit", "-qm", "accepted README-only Mongo guidance")
	compile := func(ctx context.Context, root string, _ []string) error {
		diagnostics, err := validate.CompileCandidate(ctx, validate.Request{Root: root, OperatorRoot: operatorRoot})
		if err != nil {
			return err
		}
		return validate.Err(diagnostics)
	}
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), compile)
	if err != nil {
		t.Fatal(err)
	}
	const incidentID = "mongo-auth-upgrade"
	outcome := &contracts.OutcomeRecord{
		SchemaVersion: 1, IncidentId: incidentID, Classification: "success",
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "Application MongoDB account absent; hand-created account recovered application", Resources: []*contracts.ObjectRef{{ApiVersion: "v1", Kind: "Pod", Namespace: "demo", Name: "mongo-app"}}}},
		RepairActions:       []*contracts.RepairActionReceipt{{ActionId: "manual-mongo-user", Details: "synthetic user creation receipt; credential values omitted", StartedAt: timestamppb.Now(), CompletedAt: timestamppb.Now(), Kind: "exec", Target: "Pod/mongo-app", Summary: "Created missing application user with readWrite role; independently verified application recovery", Success: true}},
	}
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
		if len(request.Outcome.RepairActions) != 1 || !request.Outcome.RepairActions[0].Success {
			t.Fatal("hand-repair evidence missing from scripted reflection")
		}
		checkout := task.Workspace.CheckoutPath
		decision := &contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"mongo-auth-bootstrap"}}
		publication := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: decision.ProposedArtifactIds}
		// First reproduce the failure: more prose is not executable learning.
		if err := os.WriteFile(filepath.Join(checkout, book, "README.md"), append(readme, []byte("\nThe user was restored by hand.\n")...), 0644); err != nil {
			return nil, err
		}
		writeReflectionCheckDecision(t, checkout, decision)
		report, code := runReflectionMemoryCheck(t, binary, checkout, proposal.BaseCommit, operatorRoot)
		_, publishErr := repository.PublishReflection(ctx, incidentID, publication)
		var failure *validate.Failure
		if code != 1 || !errors.As(publishErr, &failure) {
			t.Fatalf("README-only repair accepted: CLI %d %v broker %v", code, report.Diagnostics, publishErr)
		}
		if !proto.Equal(&contracts.MemoryCheckReport{Diagnostics: report.Diagnostics}, &contracts.MemoryCheckReport{Diagnostics: validate.ProtoDiagnostics(failure.Diagnostics)}) {
			t.Fatalf("README diagnostics differ: CLI %v broker %v", report.Diagnostics, failure.Diagnostics)
		}
		if len(report.Diagnostics) != 1 || report.Diagnostics[0].Rule != validate.RuleReflectionProgramRequired {
			t.Fatalf("unexpected README rejection: %v", report.Diagnostics)
		}
		// Correct that same proposal by learning the hand repair with current
		// DatabaseUsers / EnsureDatabaseUser / CredentialRef APIs and a detector.
		copyFastloopTree(t, fixture, filepath.Join(checkout, ".sdo"))
		writeReflectionCheckDecision(t, checkout, decision)
		// Fixed/healthy catalogs cannot substitute for the observed open catalog.
		// This guards against a simulator that merely trusts the expected result.
		recordingsPath := filepath.Join(checkout, ".sdo/corpus/mongo-auth/recordings.json")
		recorded, err := os.ReadFile(recordingsPath)
		if err != nil {
			return nil, err
		}
		var readings []map[string]any
		if err := json.Unmarshal(recorded, &readings); err != nil {
			return nil, err
		}
		withoutOpen, err := json.Marshal(readings[1:])
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(recordingsPath, withoutOpen, 0644); err != nil {
			return nil, err
		}
		report, code = runReflectionMemoryCheck(t, binary, checkout, proposal.BaseCommit, operatorRoot)
		_, publishErr = repository.PublishReflection(ctx, incidentID, publication)
		failure = nil
		if code != 1 || !errors.As(publishErr, &failure) {
			t.Fatalf("fixed-only catalog accepted: CLI %d %v broker %v", code, report.Diagnostics, publishErr)
		}
		if !proto.Equal(&contracts.MemoryCheckReport{Diagnostics: report.Diagnostics}, &contracts.MemoryCheckReport{Diagnostics: validate.ProtoDiagnostics(failure.Diagnostics)}) {
			t.Fatalf("catalog replay diagnostics differ: CLI %v broker %v", report.Diagnostics, failure.Diagnostics)
		}
		if len(report.Diagnostics) != 1 || report.Diagnostics[0].Rule != validate.RuleProgramRepeatReachable {
			t.Fatalf("unobserved open catalog did not break the repeat chain: %v", report.Diagnostics)
		}
		if err := os.WriteFile(recordingsPath, recorded, 0644); err != nil {
			return nil, err
		}
		report, code = runReflectionMemoryCheck(t, binary, checkout, proposal.BaseCommit, operatorRoot)
		if code != 0 {
			t.Fatalf("Mongo executable upgrade failed: CLI %d %v", code, report.Diagnostics)
		}
		receipt, err := repository.PublishReflection(ctx, incidentID, publication)
		if err != nil {
			t.Fatalf("Mongo executable upgrade rejected by broker: %v", err)
		}
		nativeGit(t, checkout, "add", "-f", ".sdo")
		if authored, accepted := nativeGit(t, checkout, "write-tree"), nativeGit(t, target, "rev-parse", receipt.Commit+"^{tree}"); authored != accepted {
			t.Fatalf("broker changed authored bytes: %s != %s", authored, accepted)
		}
		for _, relative := range []string{book + "/diagnose.go", book + "/mitigate.go", ".sdo/diagnostics/detectors/incidents/mongo-authentication/detector.go"} {
			if content := nativeGit(t, target, "show", receipt.Commit+":"+relative); strings.TrimSpace(content) == "" {
				t.Fatalf("accepted executable absent: %s", relative)
			}
		}
		packed, err := anypb.New(decision)
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: packed}, err
	})
	workspace := IncidentWorkspace{IncidentID: incidentID, Worktree: proposal.Path, BaseCommit: proposal.BaseCommit}
	decision, err := worker.Reflect(ctx, nativeClosure(workspace), workspace, outcome, "manual-repair", proposal.BaseCommit, ReflectionFeedback{})
	if err != nil || calls != 1 || decision.LearningDecision != "updated" || len(decision.SDKGaps) != 0 {
		t.Fatalf("scripted executable upgrade did not complete: calls=%d decision=%+v error=%v", calls, decision, err)
	}
}
