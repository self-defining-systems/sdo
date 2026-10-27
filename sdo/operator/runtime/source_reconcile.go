package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

// SourceReconcilePartialError means the write landed, but direct readback
// still names work. It never requests compensation of useful applied writes.
type SourceReconcilePartialError struct {
	Results []*contracts.SourceReconcileResult
}

func (e *SourceReconcilePartialError) Error() string {
	return fmt.Sprintf("source_reconcile_partial: %d object(s) retain source differences or unavailable observations; inspect action_reports.source_reconcile", len(e.Results))
}

func isSourceReconcile(action program.Action) bool {
	switch a := action.(type) {
	case program.ReconcileFromSource:
		return true
	case *program.ReconcileFromSource:
		return a != nil
	}
	return false
}

// Read back after rollout observation as admission and other writers can
// change the object after our successful PATCH. Paths only; never object values.
func (e PlanExecutor) reportSourceResiduals(ctx context.Context, incident IncidentRequest, result *PlanApplicationResult) error {
	if result.DryRun || !result.Applied || result.RolledBack {
		return nil
	}
	var partial []*contracts.SourceReconcileResult
	for index, action := range result.Plan.Actions {
		if !isSourceReconcile(action) || index >= len(result.ActionsApplied) || index >= len(result.ActionReports) {
			continue
		}
		ref, _ := planActionRef(action)
		report := result.ActionReports[index].SourceReconcile
		if report == nil {
			report = &contracts.SourceReconcileResult{Ref: planRefProto(ref)}
		}
		result.ActionReports[index].SourceReconcile = report
		observed, err := e.Client.Get(ctx, ref)
		if err == nil && (observed == nil || !samePlanRef(observed, ref)) {
			err = errors.New("object identity unavailable")
		}
		if err == nil {
			for _, image := range result.PreImages {
				if image.Ref == ref && image.Object != nil && image.Object.GetUID() != "" && image.Object.GetUID() != observed.GetUID() {
					err = errors.New("object UID changed")
					break
				}
			}
		}
		if err != nil {
			report.ObservationError = "direct object readback unavailable or identity changed"
		} else {
			source, sourceErr := acceptedIncidentManifest(ctx, e.Source, incident.SourceCommit, incident.Namespace, ref)
			if sourceErr != nil {
				report.ObservationError = "accepted source unavailable for readback comparison"
			} else {
				report.ResidualPaths, report.AdditionsKnown = sourceResidualPaths(observed, source)
			}
		}
		report.Converged = len(report.ResidualPaths) == 0 && report.ObservationError == ""
		for _, step := range report.RolloutSteps {
			if step.Error != "" {
				report.Converged = false
			}
		}
		if !report.Converged {
			result.ActionReports[index].Status = "partial"
			result.ActionReports[index].FailureReason = "source_residual"
			partial = append(partial, report)
		}
	}
	if len(partial) > 0 {
		result.VerificationPassed = false
		return &SourceReconcilePartialError{Results: partial}
	}
	return nil
}

func sourceResidualPaths(observed, source *unstructured.Unstructured) ([]string, bool) {
	declared := source.DeepCopy()
	preserveSourceControllerFields(declared, observed)
	want := comparableFields(declared.Object)
	got := projectSourceFields(comparableFields(observed.Object), want)
	paths := map[string]bool{}
	var compare func(string, any, any)
	compare = func(path string, left, right any) {
		if planValuesEqual(left, right) {
			return
		}
		if fields, ok := left.(map[string]any); ok {
			actual, _ := right.(map[string]any)
			for key, value := range fields {
				next := key
				if path != "" {
					next = path + "." + key
				}
				compare(next, value, actual[key])
			}
			return
		}
		if items, ok := left.([]any); ok {
			actual, _ := right.([]any)
			if len(items) != len(actual) {
				paths[path] = true
			}
			for index, value := range items {
				if index < len(actual) {
					compare(fmt.Sprintf("%s[%d]", path, index), value, actual[index])
				}
			}
			return
		}
		paths[path] = true
	}
	compare("", want, got)
	added, err := addedSourceFields(observed, source)
	for _, path := range added {
		paths[path] = true
	}
	out := make([]string, 0, len(paths))
	for path := range paths {
		out = append(out, path)
	}
	sort.Strings(out)
	return out, err == nil
}

// Controller exclusions come from the same managed-field and annotation
// policies as source comparison. No list of manager names or new allowlist.
func preserveSourceControllerFields(next, observed *unstructured.Unstructured) {
	owned := ownedBySubresourceWriters(observed)
	var preserve func(string, map[string]any, map[string]any)
	preserve = func(prefix string, target, live map[string]any) {
		for key, value := range live {
			path := key
			if prefix != "" {
				path = prefix + "." + key
			}
			if ownsDriftPath(owned, path) {
				target[key] = runtime.DeepCopyJSONValue(value)
				continue
			}
			if fields, ok := value.(map[string]any); ok {
				if child, ok := target[key].(map[string]any); ok {
					preserve(path, child, fields)
				}
			}
		}
	}
	preserve("", next.Object, observed.Object)
	for _, location := range [][]string{{"metadata", "annotations"}, {"spec", "template", "metadata", "annotations"}} {
		annotations, _, _ := unstructured.NestedMap(next.Object, location...)
		live, _, _ := unstructured.NestedMap(observed.Object, location...)
		for _, policy := range sourceBookkeepingAnnotations {
			if value, ok := live[policy.name]; ok {
				if annotations == nil {
					annotations = map[string]any{}
				}
				annotations[policy.name] = value
			} else {
				delete(annotations, policy.name)
			}
		}
		if annotations != nil {
			_ = unstructured.SetNestedMap(next.Object, annotations, location...)
		}
	}
}
