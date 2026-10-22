package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// CreditEquivalentRepairs records a responder repair as a program application
// only when its successful receipts and observed object fields equal a plan
// prepared from a matching pre-repair view.
func (l *ResponderLoop) CreditEquivalentRepairs(ctx context.Context, repairs []RepairActionReceipt) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.Executor.Client == nil || len(repairs) == 0 {
		return
	}
	view, err := l.snapshotView(ctx)
	if err != nil {
		return
	}
	seen := map[string]bool{}
	for _, application := range l.applicationsLog {
		if application.Result.Applied {
			for _, id := range application.ProgramIDs {
				seen[id] = true
			}
		}
	}
	for _, run := range l.runs {
		if run.Verdict.Result != program.Match || seen[run.ProgramID] {
			continue
		}
		preview, ok := l.matchedPlans[matchedViewKey(run.ProgramID, run.Bindings)]
		if !ok || !equivalentPlanResources(preview, repairs) {
			continue
		}
		if !l.planFieldsMatch(ctx, preview) {
			continue
		}
		diagnoser, ok := l.Programs.Diagnoser(run.ProgramID)
		if !ok {
			continue
		}
		verdict := program.RunDiagnose(ctx, diagnoser, view, run.Bindings, l.deadline())
		if verdict.Result != program.NoMatch {
			continue
		}
		preview.Applied = true
		preview.DryRun = false
		preview.VerificationCompleted = true
		preview.VerificationPassed = true
		completed := time.Now().UTC()
		applicationID := fmt.Sprintf("equivalent-repair-%d", len(l.applicationsLog)+1)
		planID := applicationID + "-plan"
		l.applicationsLog = append(l.applicationsLog, RecordedPlanApplication{
			ID: applicationID, ProgramIDs: []string{run.ProgramID}, Result: preview, CompletedAt: completed,
		})
		verified := ProgramRun{ProgramID: run.ProgramID, ProgramRevision: run.ProgramRevision, Bindings: cloneProgramBindings(run.Bindings), Verdict: verdict}
		l.runs = append(l.runs, verified)
		// These controller receipts describe equivalence verification, not a live
		// program apply. They preserve the same bound trust evidence as an apply.
		planRequest, _ := json.Marshal(ToolRequest{Command: "plan", ProgramID: run.ProgramID, Bindings: run.Bindings})
		planResponse, _ := json.Marshal(ToolResponse{Command: "plan", PlanID: planID})
		applyRequest, _ := json.Marshal(ToolRequest{Command: "apply", PlanIDs: []string{planID}})
		applyResponse, _ := json.Marshal(ToolResponse{Command: "apply", ApplicationID: applicationID, ProgramRuns: []ProgramRun{verified}})
		summary := "controller-verified equivalent responder repair; no program mutation"
		l.calls = append(l.calls, ToolCall{Command: "plan", At: completed, RequestJSON: string(planRequest), ResponseJSON: string(planResponse), Summary: summary}, ToolCall{Command: "apply", At: completed, RequestJSON: string(applyRequest), ResponseJSON: string(applyResponse), Summary: summary})
		seen[run.ProgramID] = true
	}
}

func equivalentPlanResources(preview PlanApplicationResult, repairs []RepairActionReceipt) bool {
	planned := map[sdk.ObjectRef]bool{}
	for _, preImage := range preview.PreImages {
		planned[preImage.Ref] = true
	}
	if len(planned) == 0 {
		return false
	}
	touched := map[sdk.ObjectRef]bool{}
	for _, receipt := range repairs {
		if !receipt.Success {
			continue
		}
		if len(receipt.Resources) == 0 {
			return false
		}
		for _, ref := range receipt.Resources {
			if planned[ref] {
				touched[ref] = true
			}
		}
	}
	return reflect.DeepEqual(planned, touched)
}

func (l *ResponderLoop) planFieldsMatch(ctx context.Context, preview PlanApplicationResult) bool {
	for _, preImage := range preview.PreImages {
		before, err := json.Marshal(preImage.Object.Object)
		if err != nil {
			return false
		}
		expected := before
		for _, patch := range preview.Patches {
			if patch.Ref == preImage.Ref {
				expected, err = jsonpatch.MergePatch(expected, patch.Patch)
				if err != nil {
					return false
				}
			}
		}
		want := new(unstructured.Unstructured)
		if err := json.Unmarshal(expected, &want.Object); err != nil {
			return false
		}
		current, err := l.Executor.Client.Get(ctx, preImage.Ref)
		if err != nil || current == nil || !samePlanRef(current, preImage.Ref) {
			return false
		}
		owner := preImage.Object
		if len(owner.GetManagedFields()) == 0 {
			owner = current
		}
		expectedFields := planComparableFields(want, want, owner)
		if reflect.DeepEqual(planComparableFields(preImage.Object, want, owner), expectedFields) ||
			!reflect.DeepEqual(planComparableFields(current, want, owner), expectedFields) {
			return false
		}
	}
	return true
}

// Project both sides onto the plan's declared, deployer-owned fields. This
// removes server defaults and unrelated controllers' fields without letting a
// different value in a repaired field earn equivalent-repair credit.
func planComparableFields(object, declared, owner *unstructured.Unstructured) map[string]any {
	observed := flattenDriftObject(object)
	owned := ownedDriftPaths(owner, "sdo-deployer")
	fields := map[string]any{}
	for path := range flattenDriftObject(declared) {
		if path == "metadata.annotations.kubectl.kubernetes.io/last-applied-configuration" || path == "metadata.annotations.sdo.dev/source-tree" {
			continue
		}
		if len(owner.GetManagedFields()) > 0 && !ownsDriftPath(owned, path) {
			continue
		}
		fields[path] = observed[path]
	}
	// Kubernetes unstructured reads and decoded merge patches can represent the
	// same JSON number using different Go numeric types.
	encoded, err := json.Marshal(fields)
	if err != nil {
		return nil
	}
	var normalized map[string]any
	if json.Unmarshal(encoded, &normalized) != nil {
		return nil
	}
	return normalized
}
