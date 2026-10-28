package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

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
	return terminalSourceResiduals(result, e.observeSourceResiduals(ctx, incident, result))
}

func terminalSourceResiduals(result *PlanApplicationResult, err error) error {
	var partial *SourceReconcilePartialError
	if errors.As(err, &partial) {
		result.VerificationPassed = false
		for i := range result.ActionReports {
			if report := result.ActionReports[i].SourceReconcile; report != nil && !report.Converged {
				result.ActionReports[i].Status = "partial"
				result.ActionReports[i].FailureReason = "source_residual"
			}
		}
	}
	return err
}

// Observation records evidence without turning an accepted, in-flight write into
// a terminal failure. Only the owner of the convergence bound settles it.
func (e PlanExecutor) observeSourceResiduals(ctx context.Context, incident IncidentRequest, result *PlanApplicationResult) error {
	if result.DryRun || !result.Applied || result.RolledBack {
		return nil
	}
	var partial []*contracts.SourceReconcileResult
	unread := 0
	for index, action := range result.Plan.Actions {
		if isSourceReconcile(action) && index < len(result.ActionsApplied) && index < len(result.ActionReports) {
			unread++
		}
	}
	for index, action := range result.Plan.Actions {
		if !isSourceReconcile(action) || index >= len(result.ActionsApplied) || index >= len(result.ActionReports) {
			continue
		}
		// Each object's readback gets its share of what remains, so one slow read
		// never consumes the readbacks of the objects after it.
		readCtx, cancel := context.WithCancel(ctx)
		if deadline, ok := ctx.Deadline(); ok {
			cancel()
			readCtx, cancel = context.WithTimeout(ctx, time.Until(deadline)/time.Duration(unread))
		}
		unread--
		ref, _ := planActionRef(action)
		previous := result.ActionReports[index].SourceReconcile
		report := &contracts.SourceReconcileResult{Ref: planRefProto(ref)}
		result.ActionReports[index].SourceReconcile = report
		observed, err := e.Client.Get(readCtx, ref)
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
			// A deadline cannot erase already measured unresolved work. Preserve its
			// paths as pending evidence, qualified by the unavailable final read.
			if readCtx.Err() != nil && previous != nil {
				report.ResidualPaths = append([]string(nil), previous.ResidualPaths...)
				report.AdditionsKnown = previous.AdditionsKnown
			}
			report.ObservationError = "direct object readback unavailable or identity changed"
		} else {
			source, sourceErr := acceptedIncidentManifest(readCtx, e.Source, incident.SourceCommit, incident.Namespace, ref)
			if sourceErr != nil || source == nil || !samePlanRef(source, ref) {
				report.ObservationError = "accepted source unavailable for readback comparison"
			} else {
				report.ResidualPaths, report.AdditionsKnown = sourceResidualPaths(observed, source)
				if observer, ok := e.Rollouts.(interface {
					SourceRolloutResiduals(context.Context, *unstructured.Unstructured) ([]string, error)
				}); ok {
					paths, observeErr := observer.SourceRolloutResiduals(readCtx, observed)
					report.ResidualPaths = append(report.ResidualPaths, paths...)
					sort.Strings(report.ResidualPaths)
					if observeErr != nil {
						report.ObservationError = "rollout residual observation unavailable or identity changed"
					}
				}
			}
		}
		cancel()
		report.Converged = len(report.ResidualPaths) == 0 && report.ObservationError == ""
		if !report.Converged {
			partial = append(partial, report)
		} else if result.ActionReports[index].FailureReason == "source_residual" {
			result.ActionReports[index].Status = "applied"
			result.ActionReports[index].FailureReason = ""
		}
	}
	if len(partial) > 0 {
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

// waitSourceConvergence polls until waitCtx ends. Ordinary source-field
// residuals are terminal after rollout; old-template capacity may still
// disappear after Deployment status reports completion. The final readback
// uses readCtx, the reserved remainder of the same deadline.
func (e PlanExecutor) waitSourceConvergence(waitCtx, readCtx context.Context, incident IncidentRequest, result *PlanApplicationResult) (*contracts.RolloutPending, error) {
	for {
		observeCtx := waitCtx
		if waitCtx.Err() != nil {
			observeCtx = readCtx
		}
		err := e.observeSourceResiduals(observeCtx, incident, result)
		if err == nil {
			return nil, nil
		}
		var partial *SourceReconcilePartialError
		if !errors.As(err, &partial) {
			return nil, err
		}
		rolling := false
		for _, report := range partial.Results {
			for _, path := range report.ResidualPaths {
				rolling = rolling || strings.HasPrefix(path, "rollout.")
			}
		}
		if !rolling || observeCtx == readCtx {
			// Preserve the last measured residual, even when the final API read can
			// no longer run under the original deadline.
			return nil, terminalSourceResiduals(result, err)
		}
		if e.PollRollouts {
			pending := &contracts.RolloutPending{Reason: "source rollout is settling; writes remain applied and are not independently verified"}
			return pending, &RolloutPendingError{Pending: pending}
		}
		select {
		case <-waitCtx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
}
