package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

var errPlanExecRefused = errors.New("exec refused")

// PlanExecClient returns only a process exit code. It must discard output and
// preserve Kubernetes RBAC denial, without adding a grant or retrying a command.
type PlanExecClient interface {
	Exec(context.Context, sdk.ObjectRef, string, []string) (int, error)
}
type planPodDeleter interface {
	Delete(context.Context, sdk.ObjectRef, types.UID, string) error
}

func hasRuntimeEffects(plan program.Plan) bool {
	for _, a := range plan.Actions {
		switch a.(type) {
		case program.EnsureDatabaseUser, *program.EnsureDatabaseUser, program.Exec, *program.Exec, program.DeletePod, *program.DeletePod:
			return true
		}
	}
	return false
}
func concreteRuntimeAction(a program.Action) program.Action {
	switch p := a.(type) {
	case *program.EnsureDatabaseUser:
		if p != nil {
			return *p
		}
	case *program.Exec:
		if p != nil {
			return *p
		}
	case *program.DeletePod:
		if p != nil {
			return *p
		}
	}
	return a
}

// applyRuntimePlan preflights the entire batch before the first effect. Exec
// and pod deletion are irreversible and never replayed or compensated.
func (e PlanExecutor) applyRuntimePlan(ctx context.Context, incident IncidentRequest, plan program.Plan, dryRun bool) (result PlanApplicationResult, err error) {
	started := time.Now()
	result = PlanApplicationResult{Plan: plan, DryRun: dryRun, ActionReports: newPlanReports(plan)}
	complete := false
	defer func() {
		if err != nil && result.Applied && !complete {
			recovery, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if rollbackErr := e.rollback(recovery, &result); rollbackErr != nil {
				result.RollbackError = rollbackErr.Error()
				err = errors.Join(err, rollbackErr)
			} else {
				result.RolledBack = true
			}
		}
		result.Duration = time.Since(started)
		settlePlanReports(&result, err)
	}()
	if err = plan.Validate(); err != nil {
		return result, err
	}
	if !dryRun && incident.RepairPolicy != "recorded-actions" {
		return result, fmt.Errorf("repair policy denies runtime actions")
	}
	if e.Client == nil {
		return result, fmt.Errorf("plan object client is required")
	}
	if !dryRun && (e.Rollouts == nil || e.Verifier == nil) {
		return result, fmt.Errorf("rollout waiter and health verifier are required")
	}
	type prepared struct {
		uid     types.UID
		rv      string
		command []string
		users   []program.DatabaseUser
	}
	items := make([]prepared, len(plan.Actions))
	projectedCatalogs := map[string][]program.DatabaseUser{}
	for i, original := range plan.Actions {
		a := concreteRuntimeAction(original)
		ref, refErr := planActionRef(a)
		if refErr != nil {
			return result, refErr
		}
		if ref.Namespace == "" || incident.Namespace != "" && ref.Namespace != incident.Namespace {
			return result, fmt.Errorf("runtime action is outside incident namespace")
		}
		switch a := a.(type) {
		case program.DeletePod, program.Exec, program.EnsureDatabaseUser:
			object, getErr := e.Client.Get(ctx, ref)
			if getErr != nil || object == nil || !samePlanRef(object, ref) || object.GetUID() == "" || object.GetResourceVersion() == "" {
				if _, database := a.(program.EnsureDatabaseUser); database {
					return result, databaseOperationFailure(DatabaseContainerLookup, getErr)
				}
				return result, fmt.Errorf("runtime action requires current named Pod identity")
			}
			items[i].uid = object.GetUID()
			items[i].rv = object.GetResourceVersion()
			switch a := a.(type) {
			case program.EnsureDatabaseUser:
				if err := validateDatabaseContainer(object, a.Target.Container); err != nil {
					return result, err
				}
				catalog, ok := e.Client.(DatabaseCatalogClient)
				if !ok {
					return result, fmt.Errorf("database catalog transport unavailable")
				}
				users, readErr := catalog.DatabaseUsers(ctx, a.Target)
				if readErr != nil {
					return result, databaseSafeError(readErr, "database catalog observation unavailable")
				}
				// Preserve the measured pre-repair catalog for reflection, even
				// when the responder invokes the raw typed action in one call.
				// Values were already projected by DatabaseUsers; only refs
				// and names/direct roles cross the recording boundary.
				if !dryRun && e.RecordPrimitive != nil {
					args, _ := json.Marshal([]any{a.Target})
					value, _ := json.Marshal(users)
					if recordErr := e.RecordPrimitive(sdktest.PrimitiveCall{Method: "DatabaseUsers", Arguments: args, Result: value}); recordErr != nil {
						result.ActionReports[i].RecordingError = "database catalog capture unavailable"
					}
				}
				keyTarget := a.Target
				keyTarget.Auth = nil
				keyTarget.Shell = ""
				keyRaw, _ := json.Marshal(keyTarget)
				key := string(keyRaw)
				if projected, ok := projectedCatalogs[key]; ok {
					users = projected
				}
				items[i].users, readErr = program.ApplyEnsureDatabaseUser(users, a)
				projectedCatalogs[key] = items[i].users
				if readErr != nil {
					return result, readErr
				}
			case program.DeletePod:
				if _, ok := e.Client.(planPodDeleter); !ok {
					return result, fmt.Errorf("pod delete client unavailable")
				}
			case program.Exec:
				if _, ok := e.Client.(PlanExecClient); !ok {
					return result, fmt.Errorf("exec refused: controller exec transport unavailable")
				}
				for _, v := range a.Command {
					value, provenance, resolveErr := program.ResolveValue(v, e.valueInputs(ctx, incident))
					if resolveErr != nil {
						return result, resolveErr
					}
					argument, ok := value.(string)
					if !ok {
						return result, fmt.Errorf("Exec command values must resolve to strings")
					}
					items[i].command = append(items[i].command, argument)
					result.ActionReports[i].Values = append(result.ActionReports[i].Values, provenance)
				}
			}
		default:
			preview, prepareErr := e.Apply(ctx, incident, program.Plan{Actions: []program.Action{original}}, true)
			if prepareErr != nil {
				return result, prepareErr
			}
			if len(preview.ActionReports) > 0 {
				result.ActionReports[i].Values = preview.ActionReports[0].Values
			}
		}
	}
	if dryRun {
		return result, nil
	}
	for i, original := range plan.Actions {

		a := concreteRuntimeAction(original)
		ref, _ := planActionRef(a)
		switch a := a.(type) {
		case program.DeletePod:
			deleteErr := e.Client.(planPodDeleter).Delete(ctx, ref, items[i].uid, items[i].rv)
			if apierrors.IsForbidden(deleteErr) || apierrors.IsUnauthorized(deleteErr) {
				return result, fmt.Errorf("pod deletion refused: controller RBAC denied delete on pods: %w", deleteErr)
			}
			if apierrors.IsConflict(deleteErr) {
				return result, fmt.Errorf("pod deletion refused: identity changed")
			}

			live, getErr := e.Client.Get(ctx, ref)
			confirmed := apierrors.IsNotFound(getErr) || (getErr == nil && live != nil && live.GetUID() != "" && (live.GetUID() != items[i].uid || live.GetDeletionTimestamp() != nil))
			if !confirmed {
				result.ActionReports[i].Status = "unknown"
				return result, fmt.Errorf("pod deletion unverified: original UID remains or observation unavailable")
			}

			if getErr == nil && live != nil && live.GetUID() == items[i].uid {
				result.ActionReports[i].Observed = "terminating"
			}
			result.ObjectEffects = append(result.ObjectEffects, PlanObjectEffect{Operation: "delete", Ref: ref, UID: string(items[i].uid), State: "applied", Confirmed: true, Irreversible: true})
			result.Patches = append(result.Patches, PlanPatch{Ref: ref, Operation: "delete_pod", Irreversible: true})
		case program.EnsureDatabaseUser:
			current, getErr := e.Client.Get(ctx, ref)
			if getErr != nil || current == nil || current.GetUID() != items[i].uid || current.GetResourceVersion() != items[i].rv {
				return result, fmt.Errorf("database operation refused: Pod identity/version changed")
			}
			catalog := e.Client.(DatabaseCatalogClient)
			// Dispatch may have changed the catalog even if transport fails. Retain
			// irreversible uncertainty; only successful direct read-back confirms it.
			result.Patches = append(result.Patches, PlanPatch{Ref: ref, Operation: "ensure_database_user", Irreversible: true})
			result.ActionsApplied = append(result.ActionsApplied, original)
			result.Applied = true
			if effectErr := catalog.EnsureDatabaseUser(ctx, a); effectErr != nil {
				result.ActionReports[i].Status = "unknown"
				return result, databaseSafeError(effectErr, "database operation outcome unknown")
			}
			observedPod, podErr := e.Client.Get(ctx, ref)
			if podErr != nil || observedPod == nil || observedPod.GetUID() != items[i].uid {
				result.ActionReports[i].Status = "unknown"
				return result, fmt.Errorf("database operation identity unverified")
			}

			users, readErr := catalog.DatabaseUsers(ctx, a.Target)
			actual, normalizeErr := program.NormalizeDatabaseUsers(users)
			observedPod, podErr = e.Client.Get(ctx, ref)
			if podErr != nil || observedPod == nil || observedPod.GetUID() != items[i].uid {
				result.ActionReports[i].Status = "unknown"
				return result, fmt.Errorf("database operation identity unverified")
			}
			if readErr != nil || normalizeErr != nil || !planValuesEqual(actual, items[i].users) {
				result.ActionReports[i].Status = "unknown"
				return result, databaseSafeError(readErr, "database user change unverified")
			}
			if e.RecordPrimitive != nil {
				arguments, _ := json.Marshal([]any{a.Target})
				observed, _ := json.Marshal(actual)
				if recordErr := e.RecordPrimitive(sdktest.PrimitiveCall{Method: "DatabaseUsers", Arguments: arguments, Result: observed, Phase: "fixed"}); recordErr != nil {
					result.ActionReports[i].RecordingError = "fixed database catalog capture unavailable"
				}
			}
			result.ActionReports[i].Status = "applied"
			continue
		case program.Exec:
			current, getErr := e.Client.Get(ctx, ref)
			if getErr != nil || current == nil || current.GetUID() != items[i].uid || current.GetResourceVersion() != items[i].rv {
				return result, fmt.Errorf("exec refused: Pod identity/version changed")
			}
			code, execErr := e.Client.(PlanExecClient).Exec(ctx, ref, a.Container, items[i].command)
			if execErr != nil {
				if errors.Is(execErr, errPlanExecRefused) || apierrors.IsForbidden(execErr) || apierrors.IsUnauthorized(execErr) {
					return result, fmt.Errorf("exec refused: controller transport unavailable or RBAC denied")
				}
				result.ActionReports[i].Status = "unknown"
				return result, fmt.Errorf("exec outcome unknown")
			}
			exitCode := int32(code)
			result.ActionReports[i].ExitCode = &exitCode
			result.Patches = append(result.Patches, PlanPatch{Ref: ref, Operation: "exec", Irreversible: true})
		default:
			applied, applyErr := e.Apply(ctx, incident, program.Plan{Actions: []program.Action{original}}, false)
			result.Patches = append(result.Patches, applied.Patches...)
			result.PreImages = append(result.PreImages, applied.PreImages...)
			if applyErr != nil {
				return result, applyErr
			}
		}
		result.ActionsApplied = append(result.ActionsApplied, original)
		result.Applied = true
		result.ActionReports[i].Status = "applied"
	}
	complete = true
	passed, verifyErr := e.Verifier.Verify(ctx)
	result.VerificationCompleted = true
	result.VerificationPassed = passed && verifyErr == nil
	return result, verifyErr
}
