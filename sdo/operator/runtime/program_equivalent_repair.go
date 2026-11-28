package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	jsonpatch "github.com/evanphx/json-patch"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/planwire"
	"sdo.dev/operator/rendered"
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
			if planwire.HasNonObjectEffect(app.Result.Plan.Actions) {
				if l.finalTransitionApplication(ctx, *app, view) {
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
		completed := l.clock().UTC()
		applicationID := fmt.Sprintf("equivalent-repair-%d", len(l.applicationsLog)+1)
		planID := applicationID + "-plan"
		l.setApplicationsLog(append(l.applicationsLog, RecordedPlanApplication{
			ID: applicationID, ProgramIDs: []string{run.ProgramID}, Result: preview, CompletedAt: completed, RunningCommit: l.Request.SourceCommit,
		}))
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
	return finalPlanDesiredFieldsMatch(preview, getFinal, true)
}

func finalPlanDesiredFieldsMatch(preview PlanApplicationResult, getFinal func(sdk.ObjectRef) *unstructured.Unstructured, requireChange bool) bool {
	return explainPlanDesiredFields(preview, getFinal, requireChange).Matched
}

// planFieldsExplanation says why the exact-field check passed or failed:
// Reason names a failure before any comparison, Mismatches the compared fields
// that differ. Values are redacted for Secrets.
type planFieldsExplanation struct {
	Matched    bool
	Reason     string
	Mismatches []*contracts.EffectFieldMismatch
}

func explainPlanDesiredFields(preview PlanApplicationResult, getFinal func(sdk.ObjectRef) *unstructured.Unstructured, requireChange bool) planFieldsExplanation {
	failed := func(reason string) planFieldsExplanation { return planFieldsExplanation{Reason: reason} }
	var mismatches []*contracts.EffectFieldMismatch
	if len(preview.PreImages) == 0 {
		return failed("no pre-image recorded for the written objects")
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
			return failed(fmt.Sprintf("no starting object for %s", preImage.Ref.Name))
		}
		before, err := json.Marshal(start.Object)
		if err != nil {
			return failed("pre-image not encodable: " + err.Error())
		}
		expected := before
		for _, patch := range preview.Patches {
			if patch.Ref == preImage.Ref && patch.Operation != "create" {
				expected, err = jsonpatch.MergePatch(expected, patch.Patch)
				if err != nil {
					return failed("recorded patch not mergeable: " + err.Error())
				}
			}
		}
		want := new(unstructured.Unstructured)
		if err := json.Unmarshal(expected, &want.Object); err != nil {
			return failed("expected object not decodable: " + err.Error())
		}
		current := getFinal(preImage.Ref)
		if current == nil || !samePlanRef(current, preImage.Ref) {
			return failed(fmt.Sprintf("%s/%s unreadable at settlement", preImage.Ref.Kind, preImage.Ref.Name))
		}
		if !preImage.Absent && preImage.Object != nil && preImage.Object.GetUID() != "" && current.GetUID() != preImage.Object.GetUID() {
			return failed(fmt.Sprintf("%s/%s was replaced: uid %s, now %s", preImage.Ref.Kind, preImage.Ref.Name, preImage.Object.GetUID(), current.GetUID()))
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
		// Compare in the API server's wire form, as the source comparison does:
		// a read omits zero and false scalars of omitempty fields and fills
		// built-in defaults, so an explicit written zero reads back absent.
		gvk := schema.FromAPIVersionAndKind(preImage.Ref.APIVersion, preImage.Ref.Kind)
		wireWant, wireCurrent := want.DeepCopy(), current.DeepCopy()
		normalizeSourceWire(wireWant.Object, wireCurrent.Object, gvk)
		expectedFields := planComparableFields(wireWant, wireWant, owner, changed)
		observedFields := planComparableFields(wireCurrent, wireWant, owner, changed)
		baseWant, wireBaseline := want.DeepCopy(), baseline.DeepCopy()
		normalizeSourceWire(baseWant.Object, wireBaseline.Object, gvk)
		if requireChange && reflect.DeepEqual(planComparableFields(wireBaseline, baseWant, owner, changed), planComparableFields(baseWant, baseWant, owner, changed)) {
			return failed(fmt.Sprintf("%s/%s: the plan changed no compared field", preImage.Ref.Kind, preImage.Ref.Name))
		}
		if !reflect.DeepEqual(observedFields, expectedFields) {
			mismatches = append(mismatches, planFieldMismatches(preImage.Ref, expectedFields, observedFields)...)
		}
	}
	if len(mismatches) > 0 {
		return planFieldsExplanation{Mismatches: mismatches}
	}
	return planFieldsExplanation{Matched: true}
}

// planFieldMismatches lists every compared path whose observed value differs
// from the expected one, in path order, bounded so an outcome stays small.
func planFieldMismatches(ref sdk.ObjectRef, expected, observed map[string]any) []*contracts.EffectFieldMismatch {
	const limit = 50
	paths := map[string]bool{}
	for path := range expected {
		paths[path] = true
	}
	for path := range observed {
		paths[path] = true
	}
	sorted := make([]string, 0, len(paths))
	for path := range paths {
		sorted = append(sorted, path)
	}
	slices.Sort(sorted)
	secret := ref.Kind == "Secret"
	var out []*contracts.EffectFieldMismatch
	for _, path := range sorted {
		want, have := expected[path], observed[path]
		if planValuesEqual(want, have) {
			continue
		}
		if len(out) == limit {
			break
		}
		out = append(out, &contracts.EffectFieldMismatch{Ref: planRefProto(ref), Path: path, Expected: renderMismatchValue(path, want, secret), Observed: renderMismatchValue(path, have, secret)})
	}
	return out
}

func renderMismatchValue(path string, value any, secret bool) string {
	if value == nil {
		return "<absent>"
	}
	if secret && (strings.HasPrefix(path, "data.") || strings.HasPrefix(path, "stringData.") || strings.HasPrefix(path, "binaryData.")) {
		return "<redacted sha256:" + digestJSON(value) + ">"
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "<unencodable>"
	}
	if len(encoded) > 200 {
		return string(encoded[:200]) + "..."
	}
	return string(encoded)
}

// Project both sides onto declared deployer-owned fields and explicit writes. This
// removes server defaults and unrelated controllers' fields without letting a
// different value in a repaired field earn equivalent-repair credit.
func planComparableFields(object, declared, owner *unstructured.Unstructured, changed map[string]bool) map[string]any {
	observed := flattenDriftObject(object)
	owned := ownedDriftPaths(owner, sdk.DeployerFieldManager)
	fields := map[string]any{}
	paths := flattenDriftObject(declared)
	for path := range changed {
		paths[path] = nil
	}
	for path := range paths {
		if strings.HasPrefix(path, "metadata.annotations.") && sourceBookkeepingAnnotation(strings.TrimPrefix(path, "metadata.annotations.")) {
			continue
		}
		if strings.HasPrefix(path, "metadata.labels.") && rendered.IsSourceTreeLabel(strings.TrimPrefix(path, "metadata.labels.")) {
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
