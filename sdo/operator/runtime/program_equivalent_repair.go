package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
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
	for index := range l.applicationsLog {
		if l.applicationsLog[index].Result.Applied {
			l.applicationsLog[index].Result.FinalEquivalenceVerified = nil
			l.applicationsLog[index].Result.FinalVerificationBasis = ""
		}
	}
	if l.Executor.Client == nil || len(repairs) == 0 {
		return
	}
	view, err := l.snapshotView(ctx)
	if err != nil {
		return
	}
	// Completion performs at most one final GET per distinct object, shared
	// by direct applications and equivalent repair candidates. Errors are
	// cached too; a later retry cannot silently change this completion proof.
	objects := map[sdk.ObjectRef]*unstructured.Unstructured{}
	getFinal := func(ref sdk.ObjectRef) *unstructured.Unstructured {
		if object, ok := objects[ref]; ok {
			return object
		}
		object, err := l.Executor.Client.Get(ctx, ref)
		if err != nil {
			object = nil
		}
		objects[ref] = object
		return object
	}
	// Final equivalence is separate from the historical application result.
	// A later different-field repair is a question and earns no credit.
	for index := range l.applicationsLog {
		app := &l.applicationsLog[index]
		if app.Result.Applied {
			if hasAppliedScript(app.Result.Plan.Actions) {
				if l.finalScriptApplication(ctx, *app, view) {
					app.Result.FinalVerificationBasis = "observed-transition"
				}
				continue
			}
			exact := finalPlanFieldsMatch(app.Result, getFinal)
			app.Result.FinalEquivalenceVerified = &exact
		}
	}
	seen := map[string]bool{}
	for _, application := range l.applicationsLog {
		if !application.Result.Applied || !application.Result.VerificationPassed || ((application.Result.FinalEquivalenceVerified == nil || !*application.Result.FinalEquivalenceVerified) && application.Result.FinalVerificationBasis != "observed-transition") {
			continue
		}
		for _, call := range l.calls {
			var response ToolResponse
			if call.Command != "apply" || json.Unmarshal([]byte(call.ResponseJSON), &response) != nil || response.ApplicationID != application.ID {
				continue
			}
			for _, run := range response.ProgramRuns {
				if run.Verdict.Result == program.NoMatch {
					seen[matchedViewKey(run.ProgramID, run.Bindings)] = true
				}
			}
		}
	}
	for _, run := range l.runs {
		if run.Verdict.Result != program.Match || seen[matchedViewKey(run.ProgramID, run.Bindings)] {
			continue
		}
		preview, ok := l.matchedPlans[matchedViewKey(run.ProgramID, run.Bindings)]
		if !ok || !equivalentPlanResources(preview, repairs) {
			continue
		}
		if !finalPlanFieldsMatch(preview, getFinal) {
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
		exact := true
		preview.FinalEquivalenceVerified = &exact
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
		seen[matchedViewKey(run.ProgramID, run.Bindings)] = true
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

func finalPlanFieldsMatch(preview PlanApplicationResult, getFinal func(sdk.ObjectRef) *unstructured.Unstructured) bool {
	if len(preview.PreImages) == 0 {
		return false
	}
	for _, preImage := range preview.PreImages {
		// A created object has no pre-image: compare the final object with the
		// object the plan created, starting from an empty baseline.
		baseline := preImage.Object
		var start *unstructured.Unstructured
		switch {
		case preImage.Absent:
			baseline = &unstructured.Unstructured{Object: map[string]any{}}
			for _, patch := range preview.Patches {
				if patch.Ref == preImage.Ref && patch.Operation == "create" && patch.After != nil {
					start = patch.After
					break
				}
			}
		case preImage.Object != nil:
			start = preImage.Object
		}
		if start == nil {
			return false
		}
		before, err := json.Marshal(start.Object)
		if err != nil {
			return false
		}
		expected := before
		for _, patch := range preview.Patches {
			if patch.Ref == preImage.Ref && patch.Operation != "create" {
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
		current := getFinal(preImage.Ref)
		if current == nil || !samePlanRef(current, preImage.Ref) {
			return false
		}
		owner := baseline
		if len(owner.GetManagedFields()) == 0 {
			owner = current
		}
		// A fault injector may own the broken field before the plan repairs it.
		// Include explicit plan changes even when SSA ownership moved away from
		// the deployer; otherwise a verified repair can never earn exact credit.
		changed := map[string]bool{}
		beforeFields, afterFields := flattenDriftObject(baseline), flattenDriftObject(want)
		reconcileSource, explicitWrite := false, false
		for _, action := range preview.Plan.Actions {
			ref, err := planActionRef(action)
			if err != nil || ref != preImage.Ref {
				continue
			}
			switch action.(type) {
			case program.ReconcileFromSource, *program.ReconcileFromSource:
				reconcileSource = true
			default:
				explicitWrite = true
			}
		}
		for path, value := range beforeFields {
			// Full source manifests omit server defaults and other controllers'
			// fields. Keep that existing projection for source-only reconciliation.
			if _, present := afterFields[path]; !present && reconcileSource && !explicitWrite {
				continue
			}
			if !reflect.DeepEqual(value, afterFields[path]) {
				changed[path] = true
			}
		}
		for path, value := range afterFields {
			if !reflect.DeepEqual(value, beforeFields[path]) {
				changed[path] = true
			}
		}
		expectedFields := planComparableFields(want, want, owner, changed)
		if reflect.DeepEqual(planComparableFields(baseline, want, owner, changed), expectedFields) ||
			!reflect.DeepEqual(planComparableFields(current, want, owner, changed), expectedFields) {
			return false
		}
	}
	return true
}

// Project both sides onto declared deployer-owned fields and explicit writes. This
// removes server defaults and unrelated controllers' fields without letting a
// different value in a repaired field earn equivalent-repair credit.
func planComparableFields(object, declared, owner *unstructured.Unstructured, changed map[string]bool) map[string]any {
	observed := flattenDriftObject(object)
	owned := ownedDriftPaths(owner, "sdo-deployer")
	fields := map[string]any{}
	paths := flattenDriftObject(declared)
	for path := range changed {
		paths[path] = nil
	}
	for path := range paths {
		if strings.HasPrefix(path, "metadata.annotations.") && sourceBookkeepingAnnotation(strings.TrimPrefix(path, "metadata.annotations.")) {
			continue
		}
		if len(owner.GetManagedFields()) > 0 && !ownsDriftPath(owned, path) && !changed[path] {
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
