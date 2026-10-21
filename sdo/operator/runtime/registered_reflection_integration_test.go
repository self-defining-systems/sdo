package runtime

import (
	"context"
	"errors"
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
	for _, scenario := range []string{"detector and README", "malformed then fixed", "no change"} {
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
				if scenario != "no change" {
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
			if scenario == "no change" {
				if receipt.ReflectionCommit != "" {
					t.Fatal("no-change created a commit")
				}
			} else {
				if receipt.ReflectionCommit == "" {
					t.Fatal("update was not committed")
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
