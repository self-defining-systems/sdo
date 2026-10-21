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
		if !ok || program.RunDiagnose(ctx, diagnoser, view, run.Bindings, l.deadline()).Result != program.NoMatch {
			continue
		}
		preview.Applied = true
		preview.DryRun = false
		preview.VerificationCompleted = true
		preview.VerificationPassed = true
		l.applicationsLog = append(l.applicationsLog, RecordedPlanApplication{
			ID: fmt.Sprintf("equivalent-repair-%d", len(l.applicationsLog)+1), ProgramIDs: []string{run.ProgramID},
			Result: preview, CompletedAt: time.Now().UTC(),
		})
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
			if !planned[ref] {
				return false
			}
			touched[ref] = true
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
		if reflect.DeepEqual(planComparableFields(preImage.Object), planComparableFields(want)) ||
			!reflect.DeepEqual(planComparableFields(current), planComparableFields(want)) {
			return false
		}
	}
	return true
}

func planComparableFields(object *unstructured.Unstructured) map[string]any {
	fields := map[string]any{}
	for _, key := range []string{"spec", "data", "binaryData"} {
		if value, ok := object.Object[key]; ok {
			fields[key] = value
		}
	}
	if labels := object.GetLabels(); len(labels) > 0 {
		fields["metadata.labels"] = labels
	}
	annotations := map[string]string{}
	for name, value := range object.GetAnnotations() {
		if name != "kubectl.kubernetes.io/last-applied-configuration" {
			annotations[name] = value
		}
	}
	if len(annotations) > 0 {
		fields["metadata.annotations"] = annotations
	}
	return fields
}
