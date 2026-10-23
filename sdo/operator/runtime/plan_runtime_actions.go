package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

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
		case program.Exec, *program.Exec, program.DeletePod, *program.DeletePod:
			return true
		}
	}
	return false
}
func concreteRuntimeAction(a program.Action) program.Action {
	switch p := a.(type) {
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
	}
	items := make([]prepared, len(plan.Actions))
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
		case program.DeletePod, program.Exec:
			object, getErr := e.Client.Get(ctx, ref)
			if getErr != nil || object == nil || !samePlanRef(object, ref) || object.GetUID() == "" || object.GetResourceVersion() == "" {
				return result, fmt.Errorf("runtime action requires current named Pod identity")
			}
			items[i].uid = object.GetUID()
			items[i].rv = object.GetResourceVersion()
			switch a := a.(type) {
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
		if actionUsesSourceValue(original) {
			if revisionErr := e.validateAcceptedRevision(incident); revisionErr != nil {
				return result, revisionErr
			}
		}
		a := concreteRuntimeAction(original)
		ref, _ := planActionRef(a)
		switch a := a.(type) {
		case program.DeletePod:
			deleteErr := e.Client.(planPodDeleter).Delete(ctx, ref, items[i].uid, items[i].rv)
			if apierrors.IsConflict(deleteErr) || apierrors.IsForbidden(deleteErr) || apierrors.IsUnauthorized(deleteErr) {
				return result, fmt.Errorf("pod deletion refused: identity changed or controller RBAC denied")
			}

			live, getErr := e.Client.Get(ctx, ref)
			confirmed := apierrors.IsNotFound(getErr) || (getErr == nil && live != nil && live.GetUID() != "" && live.GetUID() != items[i].uid)
			if !confirmed {
				result.ActionReports[i].Status = "unknown"
				return result, fmt.Errorf("pod deletion unverified: original UID remains or observation unavailable")
			}

			result.ObjectEffects = append(result.ObjectEffects, PlanObjectEffect{Operation: "delete", Ref: ref, UID: string(items[i].uid), State: "applied", Confirmed: true, Irreversible: true})
			result.Patches = append(result.Patches, PlanPatch{Ref: ref, Operation: "delete_pod", Irreversible: true})
		case program.Exec:
			current, getErr := e.Client.Get(ctx, ref)
			if getErr != nil || current == nil || current.GetUID() != items[i].uid || current.GetResourceVersion() != items[i].rv {
				return result, fmt.Errorf("exec refused: Pod identity/version changed")
			}
			code, execErr := e.Client.(PlanExecClient).Exec(ctx, ref, a.Container, items[i].command)
			if execErr != nil {
				if apierrors.IsForbidden(execErr) || apierrors.IsUnauthorized(execErr) {
					return result, fmt.Errorf("exec refused by controller RBAC")
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
