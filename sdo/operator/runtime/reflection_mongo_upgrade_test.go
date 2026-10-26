package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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
	testReflectionMongoAuthUpgrade(t, "mongo_auth_upgrade")
}
func TestReflectionHotelMongoAuthUpgradesReadmeThroughCLIAndBroker(t *testing.T) {
	testReflectionMongoAuthUpgrade(t, "hotel_mongo_auth_upgrade")
}
func TestReflectionHotelRunScriptUpgradesReadmeThroughCLIAndBroker(t *testing.T) {
	testReflectionMongoAuthUpgrade(t, "hotel_run_script_upgrade")
}
func testReflectionMongoAuthUpgrade(t *testing.T, fixtureName string) {
	ctx := context.Background()
	const incidentID = "mongo-auth-upgrade"
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
	target := nativeRepository(t)
	copyFastloopTree(t, "../memory/validate/testdata/good/.sdo", filepath.Join(target, ".sdo"))
	fixture := filepath.Join("../../../tests/fixtures/sdo", fixtureName, ".sdo")
	if fixtureName == "hotel_mongo_auth_upgrade" {
		raw, err := os.ReadFile(filepath.Join(fixture, "corpus/mongo-auth/open.json"))
		if err != nil {
			t.Fatal(err)
		}
		var snapshot struct {
			Pods []struct {
				Spec struct{ Containers []struct{ Env []any } }
			}
		}
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		for _, pod := range snapshot.Pods {
			for _, container := range pod.Spec.Containers {
				if len(container.Env) != 0 {
					t.Fatal("hotel credential fixture must have no container env")
				}
			}
		}
	}
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
	// Frozen controller-side healthy evidence lives outside authored memory.
	capture := t.TempDir()
	healthyBaseline := filepath.Join(capture, "healthy")
	if err := os.MkdirAll(healthyBaseline, 0700); err != nil {
		t.Fatal(err)
	}
	healthy, err := os.ReadFile(filepath.Join(fixture, "corpus/mongo-auth/healthy.json"))
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
		diagnostics, err := validate.CompileCandidate(ctx, validate.Request{Root: root, OperatorRoot: operatorRoot, HealthyBaseline: healthyBaseline})
		if err != nil {
			return err
		}
		return validate.Err(diagnostics)
	}
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), compile)
	if err != nil {
		t.Fatal(err)
	}
	podName := "mongo-app"
	if fixtureName == "hotel_mongo_auth_upgrade" || fixtureName == "hotel_run_script_upgrade" {
		podName = "mongodb-geo"
	}
	ref := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Pod", Namespace: "demo", Name: podName}
	opened := time.Now().Add(-time.Minute)
	if fixtureName == "hotel_run_script_upgrade" {
		opened = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	mutation := opened.Add(20 * time.Second)
	verified := mutation.Add(10 * time.Second)
	fingerprint := "mongo-authentication/mongo-authentication/demo/Pod/" + podName
	outcome := &contracts.OutcomeRecord{
		SchemaVersion: 1, IncidentId: incidentID, Classification: "success",
		Findings:            []*contracts.Finding{{DetectorId: "mongo-authentication", RuleId: "mongo-authentication", Fingerprint: fingerprint, Status: "active", Severity: "warn", PrimaryResource: ref, Summary: "MongoDB application account absent", Evidence: "Synthetic open catalog lacks app user", PlaybookIds: []string{"mongo-auth-bootstrap"}}},
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "Application MongoDB account absent; hand-created account recovered application", Resources: []*contracts.ObjectRef{ref}, ExplainedDetectors: []string{"mongo-authentication"}}},
		RepairActions:       []*contracts.RepairActionReceipt{{ActionId: "manual-mongo-user", Resources: []*contracts.ObjectRef{ref}, Details: "synthetic user creation receipt; credential values omitted", StartedAt: timestamppb.New(mutation.Add(-time.Second)), CompletedAt: timestamppb.New(mutation), Kind: "exec", Target: "Pod/" + podName, Summary: "Created missing application user with readWrite role; independently verified application recovery", Success: true}},
		DetectorHistory:     []*contracts.DetectorEvaluation{{DetectorId: "mongo-authentication", Status: "firing", Fingerprints: []string{fingerprint}, EvaluatedAt: timestamppb.New(opened)}, {DetectorId: "mongo-authentication", Status: "clear", EvaluatedAt: timestamppb.New(verified)}},
		Timestamps:          &contracts.OutcomeTimestamps{DetectedAt: timestamppb.New(opened), MitigatedAt: timestamppb.New(mutation), VerifiedAt: timestamppb.New(verified), CompletedAt: timestamppb.New(verified)},
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
		expectedRules := []string{validate.RuleProgramRepeatReachable}
		if fixtureName == "hotel_mongo_auth_upgrade" || fixtureName == "hotel_run_script_upgrade" {
			expectedRules = append(expectedRules, "detector.test")
		}
		gotRules := make([]string, 0, len(report.Diagnostics))
		for _, diagnostic := range report.Diagnostics {
			gotRules = append(gotRules, diagnostic.Rule)
		}
		slices.Sort(expectedRules)
		slices.Sort(gotRules)
		if !slices.Equal(expectedRules, gotRules) {
			t.Fatalf("unobserved open catalog did not fail program/detector evidence checks: %v", report.Diagnostics)
		}
		if err := os.WriteFile(recordingsPath, recorded, 0644); err != nil {
			return nil, err
		}
		if fixtureName == "hotel_run_script_upgrade" {
			labelsPath := filepath.Join(checkout, ".sdo/corpus/mongo-auth/labels.json")
			originalLabels, err := os.ReadFile(labelsPath)
			if err != nil {
				return nil, err
			}
			invalid := bytes.ReplaceAll(originalLabels, []byte("manual-mongo-user"), []byte("unrelated-or-unsuccessful-receipt"))
			if err = os.WriteFile(labelsPath, invalid, 0644); err != nil {
				return nil, err
			}
			rejected, exit := runReflectionMemoryCheck(t, binary, checkout, proposal.BaseCommit, operatorRoot)
			_, publishErr := repository.PublishReflection(ctx, incidentID, publication)
			var originFailure *validate.Failure
			if exit != 1 || !errors.As(publishErr, &originFailure) || len(rejected.Diagnostics) != 1 || rejected.Diagnostics[0].Rule != validate.RuleProgramMitigationRepair {
				t.Fatalf("unlinked script accepted: CLI=%d %v broker=%v", exit, rejected.Diagnostics, publishErr)
			}
			if err = os.WriteFile(labelsPath, originalLabels, 0644); err != nil {
				return nil, err
			}
		}
		report, code = runReflectionMemoryCheck(t, binary, checkout, proposal.BaseCommit, operatorRoot)
		if fixtureName == "hotel_run_script_upgrade" && code == 0 {
			observed := false
			for _, note := range report.MitigationReplayNotes {
				observed = observed || note.Status == contracts.MitigationReplayNote_STATUS_OBSERVED_TRANSITION
			}
			if !observed {
				t.Fatal("script learning lost its observed-transition evidence basis")
			}
		}
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
		if fixtureName == "hotel_mongo_auth_upgrade" {
			// Publication's repeat-reachability check is followed by the full
			// runtime loop: linked detector must also clear after real actions.
			replayCommand := exec.Command("go", "run", "./cmd/sdo-replay", "-sdo", filepath.Join(target, ".sdo"), "-operator", operatorRoot, "-fixtures", filepath.Join(target, ".sdo/corpus"), "-matrix=false")
			replayCommand.Dir = operatorRoot
			output, err := replayCommand.Output()
			if err != nil {
				var failed *exec.ExitError
				if errors.As(err, &failed) {
					t.Fatalf("hotel closed-loop replay failed: %v: %s", err, failed.Stderr)
				}
				t.Fatal(err)
			}
			var row struct {
				Status    string
				Post      struct{ Result string }
				Effects   []string
				Detectors map[string][]struct {
					Active int
					Error  string
				}
			}
			if err := json.Unmarshal(bytes.TrimSpace(output), &row); err != nil {
				t.Fatal(err)
			}
			if row.Status != "replayed" || row.Post.Result != "NoMatch" || len(row.Detectors["post"]) != 1 || row.Detectors["post"][0].Active != 0 || row.Detectors["post"][0].Error != "" {
				t.Fatalf("hotel replay did not clear diagnosis and detector: %s", output)
			}
			for _, effect := range []string{"real:program.EnsureDatabaseUser:action:0", "real:program.RestartRollout:action:1"} {
				if !slices.Contains(row.Effects, effect) {
					t.Fatalf("hotel replay omitted %s", effect)
				}
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
