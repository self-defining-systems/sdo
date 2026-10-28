package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// PlanLifecycleClient supports named, identity-conditional object operations.
// Create returns the server's UID receipt, required before compensation is safe.
type PlanLifecycleClient interface {
	Create(context.Context, sdk.ObjectRef, *unstructured.Unstructured) (*unstructured.Unstructured, error)
	Delete(context.Context, sdk.ObjectRef, types.UID, string) error
}

// PlanAcceptanceClient publishes UID-bound records through the same
// acceptance field manager used by adoption. Source creates require this owner.
type PlanAcceptanceClient interface {
	RecordAcceptance(context.Context, sdk.ObjectRef, string, *unstructured.Unstructured) error
}

// PlanObjectEffect is read-only evidence. DeletedPreImage is informational and
// must never authorize recreation, including after a controller restart.
type PlanObjectEffect struct {
	Operation       string
	Ref             sdk.ObjectRef
	UID             string
	State           string
	Confirmed       bool
	Irreversible    bool
	DeletedPreImage *unstructured.Unstructured `json:",omitempty"`
}

func objectActionOperation(action program.Action) string {
	switch a := action.(type) {
	case program.CreateFromSource:
		return "create"
	case program.DeleteObject:
		return "delete"
	case *program.CreateFromSource:
		if a != nil {
			return "create"
		}
	case *program.DeleteObject:
		if a != nil {
			return "delete"
		}
	}
	return ""
}
func hasObjectActions(plan program.Plan) bool {
	for _, a := range plan.Actions {
		if objectActionOperation(a) != "" {
			return true
		}
	}
	return false
}

func (e PlanExecutor) applyObjectPlan(ctx context.Context, incident IncidentRequest, plan program.Plan, dryRun bool) (result PlanApplicationResult, err error) {
	started := time.Now()
	result = PlanApplicationResult{Plan: plan, DryRun: dryRun, ActionReports: newPlanReports(plan)}
	defer func() { settlePlanReports(&result, err) }()
	complete := false
	defer func() {
		result.Duration = time.Since(started)
		if err == nil {
			return
		}
		if result.PendingAction != nil {
			recovery, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			reconciled, reconcileErr := e.Reconcile(recovery, result)
			cancel()
			result = reconciled
			if reconcileErr != nil {
				err = errors.Join(err, reconcileErr)
				return
			}
		}
		if result.Applied && !complete {
			recovery, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if rollbackErr := e.rollback(recovery, &result); rollbackErr != nil {
				result.RollbackError = rollbackErr.Error()
				err = errors.Join(err, rollbackErr)
			}
		}
	}()
	if err := plan.Validate(); err != nil {
		return result, err
	}
	if !dryRun && incident.RepairPolicy != "recorded-actions" {
		return result, fmt.Errorf("repair policy denies object mutations")
	}
	client, ok := e.Client.(PlanLifecycleClient)
	if !ok || e.Client == nil {
		return result, fmt.Errorf("named object lifecycle client is required")
	}

	if !dryRun && (e.Rollouts == nil || e.Verifier == nil) {
		return result, fmt.Errorf("rollout waiter and health verifier are required")
	}

	counts := map[sdk.ObjectRef]int{}
	lifecycle := map[sdk.ObjectRef]bool{}
	for _, action := range plan.Actions {
		ref, err := planActionRef(action)
		if err != nil {
			return result, err
		}
		counts[ref]++
		lifecycle[ref] = lifecycle[ref] || objectActionOperation(action) != ""
	}
	for ref, count := range counts {
		if count > 1 && lifecycle[ref] {
			return result, fmt.Errorf("object lifecycle action requires exclusive named-object ownership in its batch")
		}
	}
	current := map[sdk.ObjectRef]*unstructured.Unstructured{}
	originals := map[sdk.ObjectRef]*unstructured.Unstructured{}
	// Capture and validate every pre-image before the first mutation.
	for _, action := range plan.Actions {
		ref, _ := planActionRef(action)
		if err := validatePlanRef(ref); err != nil {
			return result, err
		}
		if incident.Namespace != "" && ref.Namespace != incident.Namespace {
			return result, fmt.Errorf("object action is outside the incident namespace")
		}
		if _, exists := current[ref]; exists {
			continue
		}

		object, getErr := e.Client.Get(ctx, ref)
		if objectActionOperation(action) == "create" {
			if !apierrors.IsNotFound(getErr) {
				return result, fmt.Errorf("CreateFromSource requires a confirmed missing object")
			}
			object = nil
		} else {
			if getErr != nil || object == nil || !samePlanRef(object, ref) {
				return result, fmt.Errorf("object pre-image is unavailable")
			}
			if object.GetUID() == "" || object.GetResourceVersion() == "" {
				return result, fmt.Errorf("object identity/version preconditions are required")
			}

		}
		current[ref] = object.DeepCopy()
		originals[ref] = object.DeepCopy()
		result.PreImages = append(result.PreImages, newPlanPreImage(ref, object))
	}
	for index, action := range plan.Actions {
		e.report = &result.ActionReports[index]
		ref, _ := planActionRef(action)
		operation := objectActionOperation(action)
		patch := PlanPatch{Authorization: planAuthorizationMode(action), Ref: ref, Operation: operation, Before: current[ref].DeepCopy(), Structured: true, SourceCommit: incident.SourceCommit, Irreversible: operation == "delete"}
		if operation != "" {
			patch.After, err = e.prepareObjectAction(ctx, incident, action, current[ref])
			if err != nil {
				return result, err
			}
			effect := PlanObjectEffect{Operation: operation, Ref: ref, State: "not_applied", Irreversible: operation == "delete"}
			if operation == "delete" {
				effect.UID = string(current[ref].GetUID())
				effect.DeletedPreImage = current[ref].DeepCopy()
			}
			result.ObjectEffects = append(result.ObjectEffects, effect)
		} else {
			patch.After, err = e.prepareAction(ctx, incident, action, current[ref])
			if err != nil {
				return result, err
			}

			before, _ := json.Marshal(current[ref].Object)
			after, _ := json.Marshal(patch.After.Object)
			patch.Patch, err = createConditionalObjectPatch(before, after, current[ref])
			if err != nil {
				return result, err
			}
		}
		canonicalizePlanObject(patch.After)
		result.Patches = append(result.Patches, patch)
		current[ref] = patch.After.DeepCopy()
	}
	if dryRun {
		return result, nil
	}
	written := map[sdk.ObjectRef]bool{}
	for index := range result.Patches {
		patch := &result.Patches[index]

		pending := index
		result.PendingAction = &pending
		effect := objectEffectFor(&result, patch.Ref)
		if effect != nil {
			effect.State = "unknown"
		}
		var created *unstructured.Unstructured
		switch patch.Operation {
		case "create":
			if _, getErr := e.Client.Get(ctx, patch.Ref); !apierrors.IsNotFound(getErr) {
				return result, fmt.Errorf("CreateFromSource lost its absence precondition")
			}

			var createErr error
			created, createErr = client.Create(ctx, patch.Ref, patch.After.DeepCopy())
			if createErr != nil {
				return result, fmt.Errorf("create receipt unknown")
			}
			if created == nil || created.GetUID() == "" || !samePlanRef(created, patch.Ref) {
				return result, fmt.Errorf("create UID receipt is unavailable")
			}
			patch.After.SetUID(created.GetUID())
			patch.After.SetResourceVersion(created.GetResourceVersion())
			effect.UID = string(created.GetUID())

		case "delete":
			live, getErr := e.Client.Get(ctx, patch.Ref)
			if getErr != nil {
				return result, fmt.Errorf("delete precondition read failed")
			}

			if live.GetUID() != patch.Before.GetUID() || live.GetResourceVersion() != patch.Before.GetResourceVersion() {
				return result, fmt.Errorf("delete identity/version changed")
			}
			if err := client.Delete(ctx, patch.Ref, patch.Before.GetUID(), patch.Before.GetResourceVersion()); err != nil {
				return result, fmt.Errorf("delete receipt unknown")
			}
		default:
			if written[patch.Ref] {
				patch.Patch, err = uidOnlyPlanPatch(patch.Patch, originals[patch.Ref])
				if err != nil {
					return result, err
				}
			}

			if err := e.Client.Patch(ctx, patch.Ref, types.MergePatchType, patch.Patch); err != nil {
				return result, fmt.Errorf("field write receipt unknown")
			}
		}
		written[patch.Ref] = true
		result.Applied = true
		result.ActionsApplied = append(result.ActionsApplied, plan.Actions[index])
		result.PendingAction = nil
		if effect != nil {
			effect.State = "applied"
		}
		if created != nil {
			// Retain the server receipt before crossing the metadata boundary.
			if err := e.recordCreatedAcceptance(ctx, *patch, created); err != nil {
				return result, err
			}
		}
		if patch.Operation != "" {
			if err := e.observeObjectEffect(ctx, patch); err != nil {
				return result, err
			}
			effect.Confirmed = true
		} else {
			if err := e.verifyStructuredWrites(ctx, PlanApplicationResult{Patches: []PlanPatch{*patch}}); err != nil {
				return result, err
			}
			if live, getErr := e.Client.Get(ctx, patch.Ref); getErr == nil {
				patch.After.SetResourceVersion(live.GetResourceVersion())
			}
		}
		patch.DirectReadConfirmed = true
	}
	complete = true

	return result, nil
}

func (e PlanExecutor) prepareObjectAction(ctx context.Context, incident IncidentRequest, action program.Action, before *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	ref, _ := planActionRef(action)
	if objectActionOperation(action) == "delete" {
		encoded, _ := json.Marshal(before.Object)
		if len(encoded) > 256*1024 {
			return nil, fmt.Errorf("delete pre-image exceeds receipt limit")
		}
		return nil, nil
	}
	if e.Source == nil || incident.SourceCommit == "" {
		return nil, fmt.Errorf("accepted source is required for CreateFromSource")
	}
	if _, ok := e.Client.(PlanAcceptanceClient); !ok {
		return nil, fmt.Errorf("acceptance record writer is required for CreateFromSource")
	}
	source, err := e.Source.AcceptedManifest(ctx, incident.SourceCommit, ref)
	if err != nil || source == nil || !samePlanRef(source, ref) {
		return nil, fmt.Errorf("CreateFromSource requires a matching accepted object")
	}
	desired := source.DeepCopy()
	delete(desired.Object, "status")
	desired.SetUID("")
	desired.SetResourceVersion("")
	desired.SetManagedFields(nil)
	desired.SetGeneration(0)
	desired.SetCreationTimestamp(metav1.Time{})

	return desired, nil
}

func (e PlanExecutor) observeObjectEffect(ctx context.Context, patch *PlanPatch) error {

	if patch.Operation == "delete" {
		return e.observeDeletedObject(ctx, *patch)
	}
	live, err := e.Client.Get(ctx, patch.Ref)
	if err != nil || live == nil || !samePlanRef(live, patch.Ref) {
		return fmt.Errorf("create observation unknown: current object is unavailable")
	}
	if patch.After.GetUID() == "" || live.GetUID() != patch.After.GetUID() {
		return fmt.Errorf("create observation unknown: UID changed or unavailable")
	}

	if patch.Authorization == "accepted-source" && !acceptanceRecordBound(live, patch.After) {
		return &AcceptanceRecordError{Ref: patch.Ref, Err: ErrAcceptanceRecordUnavailable}
	}

	for _, field := range []string{"spec", "data", "binaryData"} {
		if expected, exists := patch.After.Object[field]; exists && !planValueContains(live.Object[field], expected) {
			return fmt.Errorf("create observation unknown: source fields differ")
		}
	}
	// The outer cache barrier must include acceptance publication, not only
	// the earlier create version whose object had no baseline yet.
	patch.After.SetResourceVersion(live.GetResourceVersion())
	return nil
}

func (e PlanExecutor) restoreCreatedObject(ctx context.Context, patch PlanPatch) error {
	client, ok := e.Client.(PlanLifecycleClient)
	if !ok {
		return fmt.Errorf("create compensation requires lifecycle client")
	}

	live, err := e.Client.Get(ctx, patch.Ref)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("create compensation current read failed")
	}
	if patch.After == nil || patch.After.GetUID() == "" || live.GetUID() != patch.After.GetUID() {
		return fmt.Errorf("create compensation UID conflict")
	}

	// Do not delete a create whose source-owned fields were changed by another writer.
	for _, field := range []string{"spec", "data", "binaryData"} {
		if expected, exists := patch.After.Object[field]; exists && !planValueContains(live.Object[field], expected) {
			return fmt.Errorf("create compensation conflict: source-owned fields changed")
		}
	}
	if err := client.Delete(ctx, patch.Ref, live.GetUID(), live.GetResourceVersion()); err != nil {
		return fmt.Errorf("create compensation delete failed")
	}
	return e.observeDeletedObject(ctx, PlanPatch{Operation: "delete", Ref: patch.Ref, Before: live})
}

func createConditionalObjectPatch(before, after []byte, object *unstructured.Unstructured) ([]byte, error) {
	patch, err := jsonpatch.CreateMergePatch(before, after)
	if err != nil {
		return nil, err
	}
	return conditionalPlanPatch(patch, object)
}

func objectEffectFor(result *PlanApplicationResult, ref sdk.ObjectRef) *PlanObjectEffect {
	for i := range result.ObjectEffects {
		if result.ObjectEffects[i].Ref == ref {
			return &result.ObjectEffects[i]
		}
	}
	return nil
}

func (e PlanExecutor) reconcileObjectEffect(ctx context.Context, result PlanApplicationResult, index int) (PlanApplicationResult, error) {
	patch := &result.Patches[index]
	if patch.Operation == "create" && (patch.After == nil || patch.After.GetUID() == "") {
		return result, fmt.Errorf("create receipt unknown: ownership cannot be inferred without the server UID receipt")
	}
	if err := e.observeObjectEffect(ctx, patch); err != nil {
		return result, err
	}
	if len(result.ActionsApplied) == index {
		result.ActionsApplied = append(result.ActionsApplied, result.Plan.Actions[index])
		result.Applied = true
	}
	if effect := objectEffectFor(&result, patch.Ref); effect != nil {
		effect.Confirmed = true
		effect.State = "unknown"
	}
	result.PendingAction = nil
	return result, nil
}

// recordCreatedAcceptance uses the server's create response as the acceptance
// point, never a later read that could absorb another writer's additions.
func (e PlanExecutor) recordCreatedAcceptance(ctx context.Context, patch PlanPatch, created *unstructured.Unstructured) error {
	baseline := created.DeepCopy()
	if err := RecordAcceptance(baseline, patch.After); err != nil {
		return err
	}
	writer := e.Client.(PlanAcceptanceClient)
	if err := writer.RecordAcceptance(ctx, patch.Ref, baseline.GetAnnotations()[acceptedPathsAnnotation], created); err != nil {
		return &AcceptanceRecordError{Ref: patch.Ref, Err: err}
	}
	return nil
}

// ObjectDeletionPendingError retains evidence of an accepted delete whose
// original instance still exists at the observation bound. It grants no repair
// credit and never authorizes recreation.
type ObjectDeletionPendingError struct {
	Ref        sdk.ObjectRef
	UID        string
	Finalizers []string
	Err        error
}

func (e *ObjectDeletionPendingError) Error() string {
	return fmt.Sprintf("delete observation pending: %s/%s UID %s is still present: %v", e.Ref.Kind, e.Ref.Name, e.UID, e.Err)
}
func (e *ObjectDeletionPendingError) Unwrap() error { return e.Err }

func (e PlanExecutor) observeDeletedObject(ctx context.Context, patch PlanPatch) error {
	limit := cacheObservationLimit
	if e.RolloutTimeout > 0 && e.RolloutTimeout < limit {
		limit = e.RolloutTimeout
	}
	observation, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var last *unstructured.Unstructured
	for {
		live, err := e.Client.Get(observation, patch.Ref)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			if last != nil && observation.Err() != nil {
				return &ObjectDeletionPendingError{Ref: patch.Ref, UID: string(last.GetUID()), Finalizers: append([]string(nil), last.GetFinalizers()...), Err: observation.Err()}
			}
			return fmt.Errorf("delete observation unknown: current read failed: %w", err)
		}
		if live == nil || !samePlanRef(live, patch.Ref) || live.GetUID() != patch.Before.GetUID() {
			return fmt.Errorf("delete observation unknown: named object identity changed")
		}
		last = live
		select {
		case <-observation.Done():
			return &ObjectDeletionPendingError{Ref: patch.Ref, UID: string(last.GetUID()), Finalizers: append([]string(nil), last.GetFinalizers()...), Err: observation.Err()}
		case <-ticker.C:
		}
	}
}
