package runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
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
	o.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: []*contracts.ObjectRef{t1Ref("unverified")}, Items: []*contracts.ReflectionWorkItem{{Reason: "no_recorded_repair"}}}, {CauseIndex: 1, Resources: []*contracts.ObjectRef{t1Ref("api")}, Items: []*contracts.ReflectionWorkItem{{Disposition: "question", Reason: "unbound_write"}}}}, Unassigned: []*contracts.ReflectionWorkItem{{Resources: []*contracts.ObjectRef{t1Ref("unverified")}}}}
	original := proto.Clone(o)
	current := partialReflectionOutcome(o)
	if len(current.ConfirmedRootCauses) != 1 || current.ConfirmedRootCauses[0].Resources[0].Name != "api" || len(current.RepairActions) != 1 || len(current.ProgramRuns) != 1 || len(current.PlanApplications) != 1 || len(current.ProgramToolCalls) != 0 {
		t.Fatalf("unverified evidence leaked: %v", current)
	}
	if len(current.ReflectionEligibility.Causes) != 1 || current.ReflectionEligibility.Causes[0].CauseIndex != 0 || len(current.ReflectionEligibility.Unassigned) != 1 || current.ReflectionEligibility.Unassigned[0].GetReason() != "no_recorded_repair" || current.ReflectionEligibility.GetNoRepairCount() != 0 {
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

// coverRepairsWithPortProgram encodes each verified hand repair as a step of
// the accepted port program: one positive case per repaired Service, each
// linked to the incident's original repair receipts.
func coverRepairsWithPortProgram(t *testing.T, checkout, casePrefix string, outcome *contracts.OutcomeRecord) {
	t.Helper()
	seen := map[string]bool{}
	for _, action := range outcome.RepairActions {
		for _, ref := range action.GetResources() {
			key := ref.Kind + "/" + ref.Namespace + "/" + ref.Name
			if !action.GetSuccess() || seen[key] {
				continue
			}
			seen[key] = true
			caseID := fmt.Sprintf("%s-%d", casePrefix, len(seen))
			if err := os.MkdirAll(filepath.Join(checkout, ".sdo/corpus", caseID), 0755); err != nil {
				t.Fatal(err)
			}
			apiVersion := ref.ApiVersion
			if apiVersion == "" {
				apiVersion = "v1"
			}
			labels, err := json.Marshal(map[string]any{
				"root_cause_program_ids": []string{"service-targetport-mismatch"},
				"bindings":               map[string]any{"service": map[string]string{"api_version": apiVersion, "kind": ref.Kind, "namespace": ref.Namespace, "name": ref.Name}},
				"provenance":             map[string]string{"incident_id": outcome.IncidentId},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(checkout, ".sdo/corpus", caseID, "labels.json"), labels, 0644); err != nil {
				t.Fatal(err)
			}
			appendControllerRepairReceipts(t, checkout, caseID, outcome)
		}
	}
	readme := filepath.Join(checkout, ".sdo/playbooks/service-targetport-mismatch/README.md")
	text, err := os.ReadFile(readme)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readme, append(text, []byte("\nRecorded Service repairs of incident "+outcome.IncidentId+" use this mitigation.\n")...), 0644); err != nil {
		t.Fatal(err)
	}
}
