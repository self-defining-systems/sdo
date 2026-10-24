package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	"sdo.dev/operator/sdk"
)

func partialRepairOutcome(t *testing.T, classification string) *contracts.OutcomeRecord {
	t.Helper()
	o := t1Outcome(t, "revision")
	o.Classification = classification
	o.ConfirmedRootCauses = append(o.ConfirmedRootCauses, &contracts.ConfirmedRootCause{Summary: "unverified second repair", Resources: []*contracts.ObjectRef{t1Ref("unverified")}})
	action := proto.Clone(o.RepairActions[0]).(*contracts.RepairActionReceipt)
	action.ActionId, action.Target, action.Resources = "unverified-action", "Service/app/unverified", []*contracts.ObjectRef{t1Ref("unverified")}
	// A successful command is deliberately insufficient independent proof.
	o.RepairActions = append(o.RepairActions, action)
	return o
}

func TestNativeBrokerReflectsOnlyVerifiedPartialRepair(t *testing.T) {
	for _, scenario := range []string{"failed", "cold", "success"} {
		t.Run(scenario, func(t *testing.T) {
			classification := scenario
			if scenario == "cold" {
				classification = "failed"
			}
			ctx := context.Background()
			root := nativeRepository(t)
			repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), nil)
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
				want := 2
				if classification == "failed" {
					want = 1
				}
				if len(request.Outcome.ConfirmedRootCauses) != want || len(request.Outcome.RepairActions) != want || len(request.Closure.Result.ConfirmedRootCauses) != want || len(request.Closure.Result.RepairActions) != want || len(request.CauseWork) != want {
					t.Fatalf("reflection leaked unverified repair or changed full success: %v", request)
				}
				if request.Outcome.Classification != classification {
					t.Fatal("reflection upgraded aggregate classification")
				}
				if err := os.WriteFile(usagePath, []byte(`{"usage":{"input_tokens":42,"output_tokens":7,"model_requests":2}}`+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
				response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "synthetic evidence reviewed"})
				return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
			})}
			receipt, err := native.ProcessClosure(ctx, closure)
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
	if _, err := native.ProcessClosure(ctx, closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger("contract")
	if err != nil {
		t.Fatal(err)
	}
	if ledger.RequiresReflection || ledger.ReflectionDecision.GetNoChangeReason() != "no_independently_verified_repair" {
		t.Fatalf("missing observable no-proof skip: %v", ledger)
	}
}

func TestPartialRepairRequiresIndependentBoundFinalProof(t *testing.T) {
	cases := map[string]func(*contracts.OutcomeRecord){
		"successful_action_only": func(o *contracts.OutcomeRecord) { o.PlanApplications = nil },
		"manual_without_equivalence": func(o *contracts.OutcomeRecord) {
			o.RepairActions[0].Kind = "kubectl-patch"
			o.RepairActions[0].ActionId = "sdo-act-manual"
		},
		"post_apply_unknown": func(o *contracts.OutcomeRecord) {
			o.ProgramToolCalls[1].ResponseJson = strings.ReplaceAll(o.ProgramToolCalls[1].ResponseJson, "NoMatch", "Unknown")
		},
		"no_bound_transcript":     func(o *contracts.OutcomeRecord) { o.ProgramToolCalls = nil },
		"no_revision":             func(o *contracts.OutcomeRecord) { o.ProgramRuns[0].ProgramRevision = "" },
		"no_final_proof":          func(o *contracts.OutcomeRecord) { o.PlanApplications[0].FinalEquivalenceVerified = nil },
		"final_not_equivalent":    func(o *contracts.OutcomeRecord) { o.PlanApplications[0].FinalEquivalenceVerified = proto.Bool(false) },
		"rolled_back":             func(o *contracts.OutcomeRecord) { o.PlanApplications[0].RolledBack = true },
		"dry_run":                 func(o *contracts.OutcomeRecord) { o.PlanApplications[0].DryRun = true },
		"verification_failed":     func(o *contracts.OutcomeRecord) { o.PlanApplications[0].VerificationPassed = false },
		"verification_incomplete": func(o *contracts.OutcomeRecord) { o.PlanApplications[0].VerificationCompleted = false },
		"verification_unknown": func(o *contracts.OutcomeRecord) {
			o.PlanApplications[0].Verification.Result = contracts.Verdict_RESULT_UNKNOWN
		},
		"apply_error":          func(o *contracts.OutcomeRecord) { o.PlanApplications[0].Error = "write failed" },
		"unrelated_receipt_id": func(o *contracts.OutcomeRecord) { o.RepairActions[0].ActionId = "unrelated-success" },
		"ambiguous_overlapping_causes": func(o *contracts.OutcomeRecord) {
			o.ConfirmedRootCauses[1].Resources = []*contracts.ObjectRef{t1Ref("api")}
		},
		"failed_receipt":            func(o *contracts.OutcomeRecord) { o.RepairActions[0].Success = false },
		"missing_receipt_resources": func(o *contracts.OutcomeRecord) { o.RepairActions[0].Resources = nil },
		"missing_receipt_time":      func(o *contracts.OutcomeRecord) { o.RepairActions[0].CompletedAt = nil },
		"receipt_after_verification": func(o *contracts.OutcomeRecord) {
			o.RepairActions[0].CompletedAt = proto.Clone(o.ProgramToolCalls[1].At).(*timestamppb.Timestamp)
			o.RepairActions[0].CompletedAt.Seconds++
		},
		"partly_verified_cause": func(o *contracts.OutcomeRecord) {
			o.ConfirmedRootCauses[0].Resources = append(o.ConfirmedRootCauses[0].Resources, t1Ref("unverified"))
		},
		"receipt_also_touches_unverified": func(o *contracts.OutcomeRecord) {
			o.RepairActions[0].Resources = append(o.RepairActions[0].Resources, t1Ref("unverified"))
		},
		"other_namespace": func(o *contracts.OutcomeRecord) { o.RepairActions[0].Resources[0].Namespace = "other" },
		"other_kind":      func(o *contracts.OutcomeRecord) { o.RepairActions[0].Resources[0].Kind = "Deployment" },
		"extra_unbound_plan_write": func(o *contracts.OutcomeRecord) {
			a := proto.Clone(o.PlanApplications[0].Plan.Actions[0]).(*contracts.Action)
			a.GetSetField().Ref = t1Ref("unverified")
			o.PlanApplications[0].Plan.Actions = append(o.PlanApplications[0].Plan.Actions, a)
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			o := partialRepairOutcome(t, "failed")
			change(o)
			if got := partialRepairCauses(o); len(got) != 0 {
				t.Fatalf("unproven cause admitted: %v", got)
			}
		})
	}
}

func TestPartialReflectionProjectionDoesNotLeakUnverifiedEvidence(t *testing.T) {
	o := partialRepairOutcome(t, "failed")
	// Put the verified cause second so durable and compact indices differ.
	o.ConfirmedRootCauses[0], o.ConfirmedRootCauses[1] = o.ConfirmedRootCauses[1], o.ConfirmedRootCauses[0]
	badRun := proto.Clone(o.ProgramRuns[0]).(*contracts.ProgramRun)
	badRun.Bindings[0].Object = t1Ref("unverified")
	o.ProgramRuns = append(o.ProgramRuns, badRun)
	badApp := proto.Clone(o.PlanApplications[0]).(*contracts.PlanApplication)
	badApp.ApplicationId = "unverified-app"
	badApp.VerificationCompleted = false
	// Even another application on the same object cannot inherit its peer proof.
	o.PlanApplications = append(o.PlanApplications, badApp)
	o.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: []*contracts.ObjectRef{t1Ref("unverified")}, Items: []*contracts.ReflectionWorkItem{{Reason: "evidence_gap"}}}, {CauseIndex: 1, Resources: []*contracts.ObjectRef{t1Ref("api")}, Items: []*contracts.ReflectionWorkItem{{Disposition: "question", Reason: "unbound_write"}}}}, Unassigned: []*contracts.ReflectionWorkItem{{Resources: []*contracts.ObjectRef{t1Ref("unverified")}}}}
	original := proto.Clone(o)
	current := partialReflectionOutcome(o)
	if len(current.ConfirmedRootCauses) != 1 || current.ConfirmedRootCauses[0].Resources[0].Name != "api" || len(current.RepairActions) != 1 || len(current.ProgramRuns) != 1 || len(current.PlanApplications) != 1 || len(current.ProgramToolCalls) != 0 {
		t.Fatalf("unverified evidence leaked: %v", current)
	}
	if len(current.ReflectionEligibility.Causes) != 1 || current.ReflectionEligibility.Causes[0].CauseIndex != 0 || len(current.ReflectionEligibility.Unassigned) != 0 || current.ReflectionEligibility.GetEvidenceGapCount() != 0 {
		t.Fatalf("learning work escaped verified cause: %v", current.ReflectionEligibility)
	}
	current.ConfirmedRootCauses[0].Summary = "changed projection"
	if !proto.Equal(o, original) {
		t.Fatal("projection changed durable evidence")
	}
}

func TestPartialReflectionIncludesControllerVerifiedEquivalentManualRepair(t *testing.T) {
	o := partialRepairOutcome(t, "failed")
	o.PlanApplications[0].ApplicationId = "equivalent-repair-1"
	o.ProgramToolCalls[1].ResponseJson = strings.ReplaceAll(o.ProgramToolCalls[1].ResponseJson, "apply-1", "equivalent-repair-1")
	o.RepairActions[0].Kind, o.RepairActions[0].ActionId = "kubectl-patch", "sdo-act-manual"
	current := partialReflectionOutcome(o)
	if len(current.ConfirmedRootCauses) != 1 || len(current.RepairActions) != 1 || current.RepairActions[0].ActionId != "sdo-act-manual" || len(current.PlanApplications) != 1 {
		t.Fatalf("independently verified equivalent manual repair lost: %v", current)
	}
}

// No installed program or plan exists: this is the first generation of memory.
func coldPartialRepairOutcome(t *testing.T) *contracts.OutcomeRecord {
	t.Helper()
	o := partialRepairOutcome(t, "failed")
	o.PlanApplications, o.ProgramRuns, o.ProgramToolCalls = nil, nil, nil
	o.RepairActions[0].ActionId, o.RepairActions[0].Kind = "sdo-act-cold", "kubectl-patch"
	o.Timestamps = &contracts.OutcomeTimestamps{DetectedAt: timestamppb.New(time.Unix(90, 0)), DispatchedAt: timestamppb.New(time.Unix(95, 0)), CompletedAt: timestamppb.New(time.Unix(120, 0))}
	for _, name := range []string{"api", "unverified"} {
		finding, err := findingContract(sdk.Finding{DetectorID: "health-objective", RuleID: "unavailable", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: "required resource unavailable", Evidence: "independent health objective unavailable", PrimaryResource: objectRefState(t1Ref(name)), Fingerprint: "health-objective/" + name})
		if err != nil {
			t.Fatal(err)
		}
		o.Findings = append(o.Findings, finding)
	}
	response := ToolResponse{Command: "verify", Summary: "verification: 1 open findings", Findings: []sdk.Finding{findingState(o.Findings[1])}, DetectorStates: []DetectorEvaluation{{DetectorID: "health-objective", EvaluatedAt: time.Unix(110, 0), Status: DetectorEvaluationFiring}}}
	o.ProgramToolCalls = []*contracts.ProgramToolCall{{Command: "verify", At: timestamppb.New(time.Unix(111, 0)), RequestJson: `{"command":"verify"}`, ResponseJson: string(t1JSON(t, response))}}
	return o
}

func TestColdPartialRepairNeedsNoPriorProgram(t *testing.T) {
	o := coldPartialRepairOutcome(t)
	if got := partialRepairCauses(o); len(got) != 1 || got[0] != 0 {
		t.Fatalf("cold independently verified repair was not admitted: %v", got)
	}
	current := partialReflectionOutcome(o)
	if len(current.ConfirmedRootCauses) != 1 || len(current.RepairActions) != 1 || len(current.PlanApplications) != 0 || len(current.ProgramToolCalls) != 1 || current.ProgramToolCalls[0].Command != "verify" {
		t.Fatalf("cold reflection did not retain only verified cause/receipt/proof: %v", current)
	}
}

func TestColdPartialRepairSurvivesOpaqueResponseTruncation(t *testing.T) {
	o := coldPartialRepairOutcome(t)
	var response ToolResponse
	if err := json.Unmarshal([]byte(o.ProgramToolCalls[0].ResponseJson), &response); err != nil {
		t.Fatal(err)
	}
	response.Findings[0].Evidence = strings.Repeat("large independent evidence ", 300)
	o.ProgramToolCalls[0].ResponseJson = string(recordedToolResponseJSON(response, nil))
	if !strings.Contains(o.ProgramToolCalls[0].ResponseJson, `"truncated":true`) {
		t.Fatal("fixture did not exercise opaque truncation")
	}
	if got := partialRepairCauses(o); len(got) != 1 || got[0] != 0 {
		t.Fatalf("controller compact evidence lost cold repair: %v response=%s", got, o.ProgramToolCalls[0].ResponseJson)
	}
}

func TestColdPartialRepairAbstainsWithoutCompleteIndependentProof(t *testing.T) {
	cases := map[string]func(*contracts.OutcomeRecord){
		"missing verify":           func(o *contracts.OutcomeRecord) { o.ProgramToolCalls = nil },
		"receipt not trusted":      func(o *contracts.OutcomeRecord) { o.RepairActions[0].ActionId = "worker-claim" },
		"read only action":         func(o *contracts.OutcomeRecord) { o.RepairActions[0].Kind = "kubectl-get" },
		"failed action":            func(o *contracts.OutcomeRecord) { o.RepairActions[0].Success = false },
		"missing action time":      func(o *contracts.OutcomeRecord) { o.RepairActions[0].CompletedAt = nil },
		"before dispatch":          func(o *contracts.OutcomeRecord) { o.Timestamps.DispatchedAt = timestamppb.New(time.Unix(101, 0)) },
		"missing detector trigger": func(o *contracts.OutcomeRecord) { o.Findings[0].DetectorId = "not-evaluated" },
		"missing resource trigger": func(o *contracts.OutcomeRecord) { o.Findings = o.Findings[1:] },
		"same resource ambiguous cause": func(o *contracts.OutcomeRecord) {
			o.ConfirmedRootCauses[1].Resources = o.ConfirmedRootCauses[0].Resources
		},
		"late same resource mutation": func(o *contracts.OutcomeRecord) {
			a := proto.Clone(o.RepairActions[0]).(*contracts.RepairActionReceipt)
			a.CompletedAt = timestamppb.New(time.Unix(112, 0))
			o.RepairActions = append(o.RepairActions, a)
		},
		"newer verify failed": func(o *contracts.OutcomeRecord) {
			c := proto.Clone(o.ProgramToolCalls[0]).(*contracts.ProgramToolCall)
			c.At = timestamppb.New(time.Unix(115, 0))
			c.Error = "snapshot failed"
			o.ProgramToolCalls = append(o.ProgramToolCalls, c)
		},
		"unknown verify time": func(o *contracts.OutcomeRecord) { o.ProgramToolCalls[0].At = nil },
		"opaque old truncation": func(o *contracts.OutcomeRecord) {
			o.ProgramToolCalls[0].ResponseJson = `{"command":"verify","truncated":true,"findings_count":1}`
		},
		"verify request mismatch": func(o *contracts.OutcomeRecord) { o.ProgramToolCalls[0].RequestJson = `{"command":"status"}` },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			o := coldPartialRepairOutcome(t)
			mutate(o)
			if got := partialRepairCauses(o); len(got) != 0 {
				t.Fatalf("unsafe cold proof admitted: %v", got)
			}
		})
	}
	responses := map[string]func(*ToolResponse){
		"same resource still firing":    func(r *ToolResponse) { r.Findings[0].PrimaryResource = objectRefState(t1Ref("api")) },
		"related resource still firing": func(r *ToolResponse) { r.Findings[0].RelatedResources = []sdk.ObjectRef{objectRefState(t1Ref("api"))} },
		"no peer still firing":          func(r *ToolResponse) { r.Findings = nil },
		"detector error":                func(r *ToolResponse) { r.DetectorStates[0].Error = "failed" },
		"detector absent":               func(r *ToolResponse) { r.DetectorStates = nil },
		"detector stale":                func(r *ToolResponse) { r.DetectorStates[0].EvaluatedAt = time.Unix(99, 0) },
		"detector future":               func(r *ToolResponse) { r.DetectorStates[0].EvaluatedAt = time.Unix(112, 0) },
		"resource missing":              func(r *ToolResponse) { r.Findings[0].PrimaryResource = sdk.ObjectRef{} },
		"verify error":                  func(r *ToolResponse) { r.Error = "snapshot failed" },
	}
	for name, mutate := range responses {
		for _, compact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compact=%v", name, compact), func(t *testing.T) {
				o := coldPartialRepairOutcome(t)
				var r ToolResponse
				if err := json.Unmarshal([]byte(o.ProgramToolCalls[0].ResponseJson), &r); err != nil {
					t.Fatal(err)
				}
				mutate(&r)
				if compact {
					r.Summary = strings.Repeat("summary", 500)
					o.ProgramToolCalls[0].ResponseJson = string(recordedToolResponseJSON(r, nil))
				} else {
					o.ProgramToolCalls[0].ResponseJson = string(t1JSON(t, r))
				}
				if got := partialRepairCauses(o); len(got) != 0 {
					t.Fatalf("unsafe verification admitted: %v", got)
				}
			})
		}
	}
}

func TestColdVerificationSummaryKeepsBoundAndAbstainsOnOverflow(t *testing.T) {
	o := coldPartialRepairOutcome(t)
	var r ToolResponse
	if err := json.Unmarshal([]byte(o.ProgramToolCalls[0].ResponseJson), &r); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		f := r.Findings[0]
		f.PrimaryResource.Name = fmt.Sprintf("peer-%03d", i)
		r.Findings = append(r.Findings, f)
	}
	record := recordedToolResponseJSON(r, nil)
	if len(record) > maxRecordedToolResponse {
		t.Fatalf("verification record exceeds budget: %d", len(record))
	}
	o.ProgramToolCalls[0].ResponseJson = string(record)
	if got := partialRepairCauses(o); len(got) != 0 {
		t.Fatalf("omitted resource proof admitted: %v", got)
	}
}

func TestFailedGlobalClosureCanLearnControllerAllClearRepair(t *testing.T) {
	o := coldPartialRepairOutcome(t)
	response := ToolResponse{Command: "verify", Summary: "verification: healthy; return the structured incident result immediately", Healthy: true, DetectorStates: []DetectorEvaluation{{DetectorID: "health-objective", EvaluatedAt: time.Unix(110, 0), Status: DetectorEvaluationClear}}}
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprintf("compact=%v", compact), func(t *testing.T) {
			r := response
			if compact {
				r.Summary = strings.Repeat("complete healthy evaluation ", 200)
				o.ProgramToolCalls[0].ResponseJson = string(recordedToolResponseJSON(r, nil))
			} else {
				o.ProgramToolCalls[0].ResponseJson = string(t1JSON(t, r))
			}
			if got := partialRepairCauses(o); len(got) != 1 || got[0] != 0 {
				t.Fatalf("all-clear controller proof rejected because global failed: %v", got)
			}
			view := partialReflectionOutcome(o)
			if view.Classification != "failed" || len(view.ConfirmedRootCauses) != 1 || len(view.RepairActions) != 1 {
				t.Fatalf("all-clear learning upgraded or leaked global outcome: %v", view)
			}
		})
	}
}

func TestAllClearPartialEvidenceStillRejectsContradictions(t *testing.T) {
	cases := map[string]func(*ToolResponse){
		"healthy with open finding": func(r *ToolResponse) {
			r.Findings = []sdk.Finding{{PrimaryResource: objectRefState(t1Ref("unverified"))}}
		},
		"healthy with firing evaluation": func(r *ToolResponse) { r.DetectorStates[0].Status = DetectorEvaluationFiring },
		"healthy with unresolved":        func(r *ToolResponse) { r.Unresolved = true },
		"empty but not healthy":          func(r *ToolResponse) { r.Healthy = false },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			o := coldPartialRepairOutcome(t)
			r := ToolResponse{Command: "verify", Healthy: true, DetectorStates: []DetectorEvaluation{{DetectorID: "health-objective", EvaluatedAt: time.Unix(110, 0), Status: DetectorEvaluationClear}}}
			mutate(&r)
			o.ProgramToolCalls[0].ResponseJson = string(t1JSON(t, r))
			if got := partialRepairCauses(o); len(got) > 0 {
				t.Fatalf("contradictory all-clear proof admitted: %v", got)
			}
		})
	}
}
