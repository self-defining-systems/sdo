//go:build integration

package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
)

func TestNativeBrokerReflectsOnlyVerifiedPartialRepair(t *testing.T) {
	for _, scenario := range []string{"failed", "cold"} {
		t.Run(scenario, func(t *testing.T) {
			classification := scenario
			if scenario == "cold" {
				classification = "failed"
			}
			ctx := context.Background()
			root := nativeRepository(t)
			var compile broker.Validator
			if scenario == "cold" {
				seedPortProgramMemory(t, root)
				operatorRoot, err := filepath.Abs("..")
				if err != nil {
					t.Fatal(err)
				}
				compile = func(ctx context.Context, candidate string, _ []string) error {
					ds, err := validate.CompileCandidate(ctx, validate.Request{Root: candidate, OperatorRoot: operatorRoot})
					if err != nil {
						return err
					}
					return validate.Err(ds)
				}
			}
			repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), compile)
			if err != nil {
				t.Fatal(err)
			}
			o := partialRepairOutcome(t, classification)
			if scenario == "cold" {
				o = coldPartialRepairOutcome(t)
			}
			original := proto.Clone(o)
			native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), ResponderBackend: "test", ResponderModel: "test", ForceReflection: true,
				Deriver: OutcomeDeriverFunc(func(context.Context, IncidentClosure, string, string, string) (DerivedOutcome, error) {
					return DerivedOutcome{Record: o, RequiresReflection: true}, nil
				}),
			}
			workspace, err := native.PrepareIncident(ctx, "contract")
			if err != nil {
				t.Fatal(err)
			}
			closure := nativeClosure(workspace)
			closure.Request.RepairPolicy = "recorded-actions"
			evidence := incidentResultState(&contracts.IncidentResult{ConfirmedRootCauses: o.ConfirmedRootCauses, RepairActions: o.RepairActions, ProgramRuns: o.ProgramRuns, PlanApplications: o.PlanApplications, ProgramToolCalls: o.ProgramToolCalls})
			closure.Result.ConfirmedRootCauses = evidence.ConfirmedRootCauses
			closure.Result.RepairActions = evidence.RepairActions
			closure.Result.ProgramRuns, closure.Result.PlanApplications, closure.Result.ProgramToolCalls = evidence.ProgramRuns, evidence.PlanApplications, evidence.ProgramToolCalls
			if classification == "failed" {
				native.Deriver = ProtoOutcomeDeriver{}
				closure.Result.Status = IncidentFailed
				closure.FinalDetectorStates, closure.VerifiedAt = nil, time.Time{}
				if scenario == "cold" {
					closure.Request.Findings = nil
					for _, finding := range o.Findings {
						closure.Request.Findings = append(closure.Request.Findings, findingState(finding))
					}
					closure.DetectedAt = time.Unix(90, 0)
					closure.DispatchedAt = time.Unix(95, 0)
					closure.ResponderCompletedAt = time.Unix(120, 0)
				}
			}
			calls := 0
			usagePath := filepath.Join(t.TempDir(), "usage.jsonl")
			native.Reflector = RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.OpenReader(root), Timeout: time.Minute, SessionMode: "fresh", UsageLogPath: func(string) string { return usagePath }, Executor: reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
				calls++
				request := &contracts.ReflectionRequest{}
				if err := task.Request.UnmarshalTo(request); err != nil {
					t.Fatal(err)
				}
				want := 1
				if len(request.Outcome.ConfirmedRootCauses) != want || len(request.Outcome.RepairActions) != want || len(request.Closure.Result.ConfirmedRootCauses) != want || len(request.Closure.Result.RepairActions) != want || len(request.CauseWork) != 1 {
					t.Fatalf("reflection leaked unverified repair or changed full success: %v", request)
				}
				if request.CauseWork[0].GetCauseIndex() != 0 || len(request.CauseWork[0].GetResources()) != 1 || request.CauseWork[0].Resources[0].Name != "api" || len(request.UnassignedWork) != 1 || request.UnassignedWork[0].GetReason() != "no_recorded_repair" || len(request.UnassignedWork[0].GetResources()) != 1 || request.UnassignedWork[0].Resources[0].Name != "unverified" {
					t.Fatalf("verified drafting work or original-resource evidence gap lost: %v", request)
				}
				if request.Outcome.Classification != classification {
					t.Fatal("reflection upgraded aggregate classification")
				}
				if err := os.WriteFile(usagePath, []byte(`{"usage":{"input_tokens":42,"output_tokens":7,"model_requests":2}}`+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
				// The coverage rule requires an executable step for a verified hand
				// repair, so the cold hand repair extends the accepted port program
				// with a positive case linked to its original receipt. The program
				// application's repair needs no new memory.
				result := &contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "the verified repair came from an existing program application"}
				if scenario == "cold" {
					coverRepairsWithPortProgram(t, task.Workspace.CheckoutPath, "partial-repair", request.Outcome)
					result = &contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"service-targetport-mismatch"}}
				}
				response, err := anypb.New(result)
				return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
			})}
			receipt, err := closeAndReflect(t, ctx, native, closure)
			if err != nil {
				t.Fatal(err)
			}
			ledger, err := native.readLedger("contract")
			if err != nil {
				t.Fatal(err)
			}
			if calls != 1 || !ledger.ReflectionProcessed || ledger.ReflectionDecision.GetUsage().GetInputTokens() != 42 || ledger.ReflectionDecision.GetUsage().GetModelRequests() != 2 {
				t.Fatalf("partial reflection/usage missing: calls=%d ledger=%v", calls, ledger)
			}
			if len(ledger.OutcomeDocument.ConfirmedRootCauses) != 2 || ledger.OutcomeDocument.Classification != classification || !proto.Equal(o, original) {
				t.Fatal("learning filter mutated authoritative evidence")
			}
			again, err := native.ProcessClosure(ctx, closure)
			if err != nil || again != receipt || calls != 1 {
				t.Fatalf("retry duplicated reflection: %v %v calls=%d", again, err, calls)
			}
		})
	}
}

func TestNativeBrokerSkipsFailedClosureWithoutVerifiedRepair(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	o := partialRepairOutcome(t, "failed")
	o.PlanApplications[0].VerificationPassed = false
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), ResponderBackend: "test", ResponderModel: "test", ForceReflection: true, Deriver: OutcomeDeriverFunc(func(context.Context, IncidentClosure, string, string, string) (DerivedOutcome, error) {
		return DerivedOutcome{Record: o}, nil
	})}
	workspace, err := native.PrepareIncident(ctx, "contract")
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.RepairPolicy = "recorded-actions"
	if _, err := closeAndReflect(t, ctx, native, closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger("contract")
	if err != nil {
		t.Fatal(err)
	}
	if ledger.RequiresReflection || ledger.ReflectionDecision.GetNoChangeReason() != "no_session" {
		t.Fatalf("missing observable no-proof skip: %v", ledger)
	}
}

// seedPortProgramMemory accepts the shared Service targetPort program fixture.
func seedPortProgramMemory(t *testing.T, root string) {
	t.Helper()
	if err := os.CopyFS(root, os.DirFS("../memory/validate/testdata/good")); err != nil {
		t.Fatal(err)
	}
	acceptPortProgram(t, root)
}

// acceptPortProgram adds the shared Service targetPort program to accepted memory.
func acceptPortProgram(t *testing.T, root string) {
	t.Helper()
	fixture := "../../../tests/fixtures/sdo/playbook_program_app/.sdo"
	module, err := os.ReadFile(filepath.Join(fixture, "diagnostics/go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo/diagnostics/go.mod"), module, 0644); err != nil {
		t.Fatal(err)
	}
	testutil.CopyPortProgram(t, root, fixture)
	index := filepath.Join(root, ".sdo/playbooks/README.md")
	data, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(index, append(data, []byte("\n- [service-targetport-mismatch](service-targetport-mismatch/README.md)\n")...), 0644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "add", "-A")
	nativeGit(t, root, "commit", "-qm", "accepted port program")
}

