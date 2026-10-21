package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
)

func TestReflectionDirectFilesCLIAndBroker(t *testing.T) {
	ctx := context.Background()
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "sdo-operator")
	build := exec.Command("go", "build", "-p", "8", "-o", binary, "./cmd/sdo-operator")
	build.Dir = operatorRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v: %s", err, out)
	}
	for _, scenario := range []string{"detector and README", "malformed then fixed", "no change", "SDK gap"} {
		t.Run(scenario, func(t *testing.T) {
			target := nativeRepository(t)
			if err := os.CopyFS(target, os.DirFS("../memory/validate/testdata/good")); err != nil {
				t.Fatal(err)
			}
			nativeGit(t, target, "add", "-A")
			nativeGit(t, target, "commit", "-qm", "memory")
			compile := func(ctx context.Context, root string, _ []string) error {
				ds, err := validate.CompileCandidate(ctx, validate.Request{Root: root, OperatorRoot: operatorRoot})
				if err != nil {
					return err
				}
				return validate.Err(ds)
			}
			repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), compile)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			var authoredREADME, authoredGo []byte
			worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(target), Catalog: memory.OpenCatalogReader(target), Timeout: time.Minute}
			worker.Executor = reflectionTaskExecutorFunc(func(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
				calls++
				checkout, base := task.Workspace.CheckoutPath, task.Workspace.BaseCommit
				readme := filepath.Join(checkout, ".sdo/playbooks/base/README.md")
				detector := filepath.Join(checkout, ".sdo/diagnostics/detectors/incidents/cache/detector.go")
				response := &contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "already covered"}
				if scenario == "SDK gap" {
					response.SdkGaps = []*contracts.SdkGap{{FaultClass: "fault", Primitive: "ProbeUDP", Reason: "UDP", ScratchScript: "probe"}}
					response.LearningDecision = "sdk_gap"
					response.NoChangeReason = "No deterministic SDK observation for this cause"
				}
				if scenario != "no change" && scenario != "SDK gap" {
					original, err := os.ReadFile(readme)
					if err != nil {
						return nil, err
					}
					authoredREADME = append(original, []byte("\n## Verification\nThe controller confirmed recovery.\n")...)
					source, err := os.ReadFile(detector)
					if err != nil {
						return nil, err
					}
					authoredGo = append(source, []byte("\n// Confirmed signature retained after reflection.\n")...)
					if err := os.WriteFile(detector, authoredGo, 0o644); err != nil {
						return nil, err
					}
					if err := os.WriteFile(readme, authoredREADME, 0o644); err != nil {
						return nil, err
					}
					response = &contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"base"}}
					if scenario == "malformed then fixed" {
						if err := os.WriteFile(readme, []byte("malformed README\n"), 0o644); err != nil {
							return nil, err
						}
						writeReflectionCheckDecision(t, checkout, response)
						report, code := runReflectionMemoryCheck(t, binary, checkout, base, operatorRoot)
						if code != 1 || len(report.Diagnostics) == 0 {
							t.Fatalf("malformed README passed CLI: %d %v", code, report)
						}
						_, rejected := repository.Publish(ctx, "direct-files", broker.Responder, broker.ReflectionPhase)
						var failure *validate.Failure
						if !errors.As(rejected, &failure) {
							t.Fatalf("broker did not return named diagnostics: %v", rejected)
						}
						ds := failure.Diagnostics
						if !proto.Equal(report, &contracts.MemoryCheckReport{Root: checkout, Actor: "responder", Phase: "reflection", Base: base, Diagnostics: validate.ProtoDiagnostics(ds)}) {
							t.Fatalf("CLI/broker diagnostics differ: CLI %v broker %v", report, ds)
						}
						if err := os.WriteFile(readme, authoredREADME, 0o644); err != nil {
							return nil, err
						}
					}
				}
				writeReflectionCheckDecision(t, checkout, response)
				report, code := runReflectionMemoryCheck(t, binary, checkout, base, operatorRoot)
				if code != 0 {
					t.Fatalf("final memory check failed: %d %v", code, report)
				}
				packed, err := anypb.New(response)
				return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: packed}, err
			})
			native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Reflector: worker, ResponderBackend: "fake", ResponderModel: "fake", Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
				return testDerivedOutcome(c.Request.IncidentID, "success", true), nil
			})}
			workspace, err := native.PrepareIncident(ctx, "direct-files")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repaired\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			receipt, err := native.ProcessClosure(ctx, nativeClosure(workspace))
			if err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("worker dispatched %d times", calls)
			}
			if scenario == "no change" || scenario == "SDK gap" {
				if receipt.ReflectionCommit != "" {
					t.Fatal("no-change created a commit")
				}
			} else {
				if receipt.ReflectionCommit == "" {
					t.Fatal("update was not committed")
				}
				ledger, err := native.readLedger("direct-files")
				if err != nil || len(ledger.GetReflectionDecision().GetPlaybookIds()) != 1 || ledger.GetReflectionDecision().GetPlaybookIds()[0] != "base" {
					t.Fatalf("accepted playbook IDs not durable: %v %v", ledger, err)
				}
				for rel, want := range map[string][]byte{".sdo/playbooks/base/README.md": authoredREADME, ".sdo/diagnostics/detectors/incidents/cache/detector.go": authoredGo} {
					got, err := os.ReadFile(filepath.Join(target, rel))
					if err != nil || string(got) != string(want) {
						t.Fatalf("authored bytes changed in %s: %v", rel, err)
					}
				}
			}
		})
	}
}

func runReflectionMemoryCheck(t *testing.T, binary, root, base, operatorRoot string) (*contracts.MemoryCheckReport, int) {
	t.Helper()
	command := exec.Command(binary, "memory", "check", "--json")
	command.Dir = root
	command.Env = append(os.Environ(), "SDO_ACTOR=responder", "SDO_PHASE=reflection", "SDO_BASE_COMMIT="+base, "SDO_OPERATOR_ROOT="+operatorRoot, "SDO_HEALTHY_BASELINE=")
	out, err := command.Output()
	code := 0
	if err != nil {
		var failed *exec.ExitError
		if !errors.As(err, &failed) {
			t.Fatal(err)
		}
		code = failed.ExitCode()
		if code > 1 {
			t.Fatalf("CLI infrastructure failure: %s", failed.Stderr)
		}
	}
	report := &contracts.MemoryCheckReport{}
	if err := protojson.Unmarshal(out, report); err != nil {
		t.Fatalf("CLI report: %v: %s", err, out)
	}
	return report, code
}

func TestReflectionBrokerDiagnosticRetryIsDurableAndBounded(t *testing.T) {
	ctx := context.Background()
	target := nativeRepository(t)
	nativeMemoryFixture(t, target)
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(target), Catalog: memory.OpenCatalogReader(target), Timeout: time.Minute}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		request := &contracts.ReflectionRequest{}
		if err := task.Request.UnmarshalTo(request); err != nil {
			return nil, err
		}
		if int(request.ValidationAttempt) != calls+1 {
			t.Fatalf("correction %d, want %d", request.ValidationAttempt, calls)
		}
		if calls > 0 && len(request.PreviousDiagnostics) == 0 {
			t.Fatal("durable diagnostic feedback missing")
		}
		calls++
		if err := os.WriteFile(filepath.Join(task.Workspace.CheckoutPath, ".sdo/playbooks/base/README.md"), []byte("bad README\n"), 0o644); err != nil {
			return nil, err
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"base"}})
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
	})
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Reflector: worker, ResponderBackend: "fake", ResponderModel: "fake", Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
		return testDerivedOutcome(c.Request.IncidentID, "success", true), nil
	})}
	workspace, err := native.PrepareIncident(ctx, "retry")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	if _, err := native.ProcessClosure(ctx, closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger("retry")
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.ReflectionRejections) != 3 || !ledger.ReflectionRejected || !ledger.ReflectionProcessed || ledger.ReflectionCommit != "" {
		t.Fatalf("terminal rejection not durable: %v", ledger)
	}
	for i, rejection := range ledger.ReflectionRejections {
		if rejection.Attempt != uint32(i+1) || len(rejection.Diagnostics) == 0 {
			t.Fatalf("missing diagnostic feedback: %v", rejection)
		}
	}
	if _, err := repository.ProposalInfo("retry", broker.ReflectionPhase); err == nil {
		t.Fatal("rejected checkout retained")
	}
	native = NativeIncidentBroker{Repository: repository, LedgerRoot: native.LedgerRoot, Reflector: worker, ResponderBackend: "fake", ResponderModel: "fake", Deriver: native.Deriver}
	if _, err := native.ProcessClosure(ctx, closure); err != nil {
		t.Fatal(err)
	}

	if calls != 3 {
		t.Fatalf("dispatched %d times, want 3", calls)
	}
}

func TestReflectionInfrastructureFailureDoesNotSpendLearningRetries(t *testing.T) {
	ctx := context.Background()
	target := nativeRepository(t)
	nativeMemoryFixture(t, target)
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(_ context.Context, _ string, paths []string) error {
		for _, path := range paths {
			if strings.HasPrefix(path, ".sdo/playbooks/") {
				return errors.New("validator infrastructure unavailable")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "fake", ResponderModel: "fake", Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
		return testDerivedOutcome(c.Request.IncidentID, "success", true), nil
	}), Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, w IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
		calls++
		path := filepath.Join(w.Worktree, ".sdo/playbooks/base/README.md")
		content, err := os.ReadFile(path)
		if err != nil {
			return ReflectionDecision{}, err
		}
		err = os.WriteFile(path, append(content, []byte("\nConfirmed recovery.\n")...), 0o644)
		return ReflectionDecision{LearningDecision: "updated", PlaybookIDs: []string{"base"}}, err
	})}
	workspace, err := native.PrepareIncident(ctx, "infrastructure")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	for range 2 {
		if _, err := native.ProcessClosure(ctx, closure); err == nil || !strings.Contains(err.Error(), "infrastructure unavailable") {
			t.Fatalf("infrastructure failure lost: %v", err)
		}
		ledger, err := native.readLedger(workspace.IncidentID)
		if err != nil {
			t.Fatal(err)
		}
		if len(ledger.ReflectionRejections) != 0 || ledger.ReflectionRejected {
			t.Fatalf("infrastructure consumed learning budget: %v", ledger)
		}
	}
	if calls != 2 {
		t.Fatalf("worker dispatched %d times", calls)
	}
}

func writeReflectionCheckDecision(t *testing.T, root string, response *contracts.ReflectionResult) {
	t.Helper()
	data, err := protojson.Marshal(&contracts.ReflectionDecision{LearningDecision: response.LearningDecision, NoChangeReason: response.NoChangeReason, PlaybookIds: response.ProposedArtifactIds})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".git", validate.DecisionFile), data, 0600); err != nil {
		t.Fatal(err)
	}
}

// writeCapturedProgramCorpus copies the immutable evidence while authoring only
// logical labels and bindings, as a direct-edit reflection worker does.
func writeCapturedProgramCorpus(t *testing.T, root, capture, incidentID, programID, parameter, kind, namespace, match, peer, phase string) {
	t.Helper()
	dir := filepath.Join(root, ".sdo/corpus", programID+"-case")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	opened, err := os.ReadFile(filepath.Join(capture, "open@"+phase+".json"))
	if err != nil {
		t.Fatal(err)
	}
	fixed, err := os.ReadFile(filepath.Join(capture, "fixed.json"))
	if err != nil {
		t.Fatal(err)
	}
	recordings, err := os.ReadFile(filepath.Join(capture, "open@"+phase+"-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	apiVersion := "v1"
	if kind == "NetworkPolicy" {
		apiVersion = "networking.k8s.io/v1"
	}
	labels, err := json.Marshal(map[string]any{
		"root_cause_program_ids": []string{programID},
		"bindings":               map[string]any{parameter: map[string]string{"api_version": apiVersion, "kind": kind, "namespace": namespace, "name": match}},
		"peer_bindings":          map[string]any{parameter: map[string]string{"api_version": apiVersion, "kind": kind, "namespace": namespace, "name": peer}},
		"provenance":             map[string]string{"incident_id": incidentID, "open_at": phase, "open_sha256": fmt.Sprintf("%x", sha256.Sum256(opened)), "fixed_sha256": fmt.Sprintf("%x", sha256.Sum256(fixed))},
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"open.json": opened, "fixed.json": fixed, "healthy.json": fixed, "recordings.json": recordings, "labels.json": labels} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
}

// Hand-written program and toy snapshots exercise direct-edit publication without run outputs.
func TestReflectionLearnedProgramCLIAndBroker(t *testing.T) {
	ctx := context.Background()
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "sdo-operator")
	build := exec.Command("go", "build", "-p", "8", "-o", binary, "./cmd/sdo-operator")
	build.Dir = operatorRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v: %s", err, out)
	}
	for _, physical := range []bool{false, true} {
		t.Run(fmt.Sprintf("physical_path_%v", physical), func(t *testing.T) {
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
			nativeGit(t, target, "add", "-A")
			nativeGit(t, target, "commit", "-qm", "memory")
			const incidentID = "learned-program"
			capture := t.TempDir()
			fixture := "../../../tests/fixtures/sdo/playbook_program_app/.sdo/corpus/targetport-mismatch"
			for destination, source := range map[string]string{"open@detect.json": "open.json", "fixed.json": "fixed.json", "open@detect-recordings.json": "recordings.json"} {
				data, err := os.ReadFile(filepath.Join(fixture, source))
				if err != nil {
					t.Fatal(err)
				}
				if destination == "fixed.json" {
					healthy, err := os.ReadFile(filepath.Join(fixture, "healthy.json"))
					if err != nil {
						t.Fatal(err)
					}
					var fixedView, healthyView map[string]any
					if err := json.Unmarshal(data, &fixedView); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(healthy, &healthyView); err != nil {
						t.Fatal(err)
					}
					for key, value := range healthyView {
						if items, ok := value.([]any); ok {
							prior, _ := fixedView[key].([]any)
							fixedView[key] = append(prior, items...)
						}
					}
					data, err = json.Marshal(fixedView)
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(filepath.Join(capture, destination), data, 0644); err != nil {
					t.Fatal(err)
				}
			}
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
			if _, err := repository.RecordOutcome(ctx, incidentID, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: incidentID, Classification: "success"}); err != nil {
				t.Fatal(err)
			}
			proposal, err := repository.CreateProposalForPhase(ctx, incidentID, broker.ReflectionPhase)
			if err != nil {
				t.Fatal(err)
			}
			var authoredTree string
			worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute}
			worker.Executor = reflectionTaskExecutorFunc(func(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
				checkout := task.Workspace.CheckoutPath
				testutil.CopyPortProgram(t, checkout, "../../../tests/fixtures/sdo/playbook_program_app/.sdo")
				writeCapturedProgramCorpus(t, checkout, capture, incidentID, "service-targetport-mismatch", "service", "Service", "demo", "front", "peer", "detect")
				index := filepath.Join(checkout, ".sdo/playbooks/README.md")
				data, err := os.ReadFile(index)
				if err != nil {
					return nil, err
				}
				data = append(data, []byte("\n- [service-targetport-mismatch](service-targetport-mismatch/README.md)\n")...)
				if err := os.WriteFile(index, data, 0644); err != nil {
					return nil, err
				}
				if physical {
					path := filepath.Join(checkout, ".sdo/playbooks/service-targetport-mismatch/README.md")
					data, err := os.ReadFile(path)
					if err != nil {
						return nil, err
					}
					if err := os.WriteFile(path, append(data, []byte("\nRead .sdo/goal.md for context.\n")...), 0644); err != nil {
						return nil, err
					}
				}
				decision := &contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"service-targetport-mismatch"}}
				writeReflectionCheckDecision(t, checkout, decision)
				command := exec.Command(binary, "memory", "check", "--json")
				command.Dir = checkout
				command.Env = append(os.Environ(), "SDO_ACTOR=responder", "SDO_PHASE=reflection", "SDO_BASE_COMMIT="+proposal.BaseCommit, "SDO_OPERATOR_ROOT="+operatorRoot, "SDO_HEALTHY_BASELINE=", "SDO_CAPTURED_CORPUS_DIR="+capture, "SDO_INCIDENT_ID="+incidentID)
				output, checkErr := command.Output()
				report := new(contracts.MemoryCheckReport)
				if err := protojson.Unmarshal(output, report); err != nil {
					t.Fatalf("CLI report: %v: %s", err, output)
				}
				receipt, publishErr := repository.PublishReflection(validate.WithCaptureSource(ctx, capture, incidentID), incidentID, &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}})
				if physical {
					var failure *validate.Failure
					if checkErr == nil || !errors.As(publishErr, &failure) {
						t.Fatalf("physical path accepted: CLI %v broker %v", checkErr, publishErr)
					}
					if !proto.Equal(&contracts.MemoryCheckReport{Diagnostics: report.Diagnostics}, &contracts.MemoryCheckReport{Diagnostics: validate.ProtoDiagnostics(failure.Diagnostics)}) {
						t.Fatalf("diagnostics differ: CLI %v broker %v", report.Diagnostics, failure.Diagnostics)
					}
					if len(report.Diagnostics) != 1 || report.Diagnostics[0].Rule != "text.physical-path" {
						t.Fatalf("unexpected physical-path diagnostics: %v", report.Diagnostics)
					}
				} else {
					if checkErr != nil || publishErr != nil {
						t.Fatalf("learned program rejected: CLI %v %v broker %v", checkErr, report.Diagnostics, publishErr)
					}
					// The proposal checkout and accepted commit contain exactly the same tree.
					nativeGit(t, checkout, "add", "-f", ".sdo")
					authoredTree = nativeGit(t, checkout, "write-tree")
					acceptedTree := nativeGit(t, target, "rev-parse", receipt.Commit+"^{tree}")
					if authoredTree != acceptedTree {
						t.Fatalf("accepted tree %s differs from authored %s", acceptedTree, authoredTree)
					}
				}
				response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"service-targetport-mismatch"}})
				return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
			})
			workspace := IncidentWorkspace{IncidentID: incidentID, Worktree: proposal.Path, BaseCommit: proposal.BaseCommit}
			if _, err := worker.Reflect(ctx, nativeClosure(workspace), workspace, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: incidentID, Classification: "success"}, "repair", proposal.BaseCommit, ReflectionFeedback{}); err != nil {
				t.Fatal(err)
			}
			if !physical && authoredTree == "" {
				t.Fatal("program was not committed")
			}
		})
	}
}
