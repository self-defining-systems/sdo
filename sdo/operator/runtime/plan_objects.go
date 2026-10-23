package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// PlanLifecycleClient supports named, identity-conditional object operations.
// Create returns the server's UID receipt, required before compensation is safe.
type PlanLifecycleClient interface {
	Create(context.Context, sdk.ObjectRef, *unstructured.Unstructured) (*unstructured.Unstructured, error)
	Delete(context.Context, sdk.ObjectRef, types.UID, string) error
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
	if e.Source == nil {
		return result, fmt.Errorf("accepted source is required for object actions")
	}
	if !dryRun && (e.Rollouts == nil || e.Verifier == nil) {
		return result, fmt.Errorf("rollout waiter and health verifier are required")
	}
	if err := e.validateAcceptedRevision(incident); err != nil {
		return result, err
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
	var refs []sdk.ObjectRef
	// Complete every source/safety/pre-image check before the first mutation.
	for _, action := range plan.Actions {
		ref, _ := planActionRef(action)
		if err := validatePlanRef(ref); err != nil {
			return result, err
		}
		if ref.Namespace != "" && incident.Namespace != "" && ref.Namespace != incident.Namespace {
			return result, fmt.Errorf("object action is outside the incident namespace")
		}
		if _, exists := current[ref]; exists {
			continue
		}
		if err := e.Safety.authorizeRead(ref, incident.SourceCommit, e.safetyNow()); err != nil {
			return result, err
		}
		object, getErr := e.Client.Get(ctx, ref)
		if objectActionOperation(action) == "create" {
			if !apierrors.IsNotFound(getErr) {
				return result, fmt.Errorf("CreateFromSource requires a confirmed missing object")
			}
			object = nil
		} else {
			if getErr != nil || object == nil {
				return result, fmt.Errorf("object pre-image is unavailable")
			}
			if object.GetUID() == "" || object.GetResourceVersion() == "" {
				return result, fmt.Errorf("object identity/version preconditions are required")
			}
			if err := e.Safety.checkObject(object, ref); err != nil {
				return result, err
			}
		}
		current[ref] = object.DeepCopy()
		originals[ref] = object.DeepCopy()
		refs = append(refs, ref)
		result.PreImages = append(result.PreImages, PlanPreImage{Ref: ref, Object: object.DeepCopy()})
	}
	for index, action := range plan.Actions {
		e.report = &result.ActionReports[index]
		ref, _ := planActionRef(action)
		operation := objectActionOperation(action)
		patch := PlanPatch{Authorization: planAuthorizationMode(action, e.SourceAuthority != nil), Ref: ref, Operation: operation, Before: current[ref].DeepCopy(), Structured: true, SourceCommit: incident.SourceCommit, Irreversible: operation == "delete"}
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
			patch.After, err = e.prepareAction(ctx, incident, action, current[ref], true)
			if err != nil {
				return result, err
			}
			if err := e.Safety.checkObject(patch.After, ref); err != nil {
				return result, err
			}
			if _, err := e.validateSourceRepair(ctx, incident, ref, originals[ref], current[ref], patch.After); err != nil {
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
		if err := e.validateAcceptedRevision(incident); err != nil {
			return result, err
		}
		if err := e.Safety.authorizeRead(patch.Ref, incident.SourceCommit, e.safetyNow()); err != nil {
			return result, err
		}
		pending := index
		result.PendingAction = &pending
		effect := objectEffectFor(&result, patch.Ref)
		if effect != nil {
			effect.State = "unknown"
		}
		switch patch.Operation {
		case "create":
			if _, getErr := e.Client.Get(ctx, patch.Ref); !apierrors.IsNotFound(getErr) {
				return result, fmt.Errorf("CreateFromSource lost its absence precondition")
			}
			if err := e.Safety.checkObject(patch.After, patch.Ref); err != nil {
				return result, err
			}
			created, createErr := client.Create(ctx, patch.Ref, patch.After.DeepCopy())
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
			if err := e.Safety.checkObject(live, patch.Ref); err != nil {
				return result, err
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
			if err := e.Safety.checkObject(patch.After, patch.Ref); err != nil {
				return result, err
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
		if patch.Operation != "" {
			if err := e.observeObjectEffect(ctx, *patch); err != nil {
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
	if err := e.awaitObserved(ctx, result.Patches); err != nil {
		return result, err
	}
	if err := e.Rollouts.Wait(ctx, refs); err != nil {
		return result, fmt.Errorf("wait for object plan rollouts: %w", err)
	}
	passed, verifyErr := e.Verifier.Verify(ctx)
	if verifyErr != nil {
		return result, fmt.Errorf("object plan verification failed")
	}
	result.VerificationPassed = passed
	result.VerificationCompleted = true
	return result, nil
}

func (e PlanExecutor) prepareObjectAction(ctx context.Context, incident IncidentRequest, action program.Action, before *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	ref, _ := planActionRef(action)
	proof := e.Safety.Objects[ref]
	var source *unstructured.Unstructured
	var err error
	if ref.Namespace == "" {
		// Cluster absence is anchored to the application's current accepted render
		// selection, never an empty-namespace lock or a failed source lookup.
		scoped, ok := e.Source.(interface {
			AcceptedScopedManifest(context.Context, string, string, sdk.ObjectRef) (*unstructured.Unstructured, error)
		})
		if !ok || incident.Namespace == "" {
			return nil, fmt.Errorf("cluster object action requires application-scoped accepted source evidence")
		}
		source, err = scoped.AcceptedScopedManifest(ctx, incident.SourceCommit, incident.Namespace, ref)
	} else {
		source, err = e.Source.AcceptedManifest(ctx, incident.SourceCommit, ref)
	}
	if objectActionOperation(action) == "delete" {
		if !proof.DeleteAllowed {
			return nil, fmt.Errorf("plan safety: irreversible delete effect is not authorized")
		}
		if !errors.Is(err, rendered.ErrAcceptedManifestAbsent) {
			return nil, fmt.Errorf("DeleteObject requires authoritative accepted-source absence")
		}
		if err := e.Safety.checkObject(before, ref); err != nil {
			return nil, err
		}
		encoded, _ := json.Marshal(before.Object)
		if len(encoded) > 256*1024 {
			return nil, fmt.Errorf("delete pre-image exceeds receipt limit")
		}
		return nil, nil
	}
	if !proof.CreateAllowed {
		return nil, fmt.Errorf("plan safety: create effect is not authorized")
	}
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
	if err := e.Safety.checkObject(desired, ref); err != nil {
		return nil, err
	}
	if err := validateCreateSchema(desired); err != nil {
		return nil, err
	}
	return desired, nil
}

func validateCreateSchema(object *unstructured.Unstructured) error {
	var typed any
	switch object.GetAPIVersion() + "/" + object.GetKind() {
	case "v1/ConfigMap":
		typed = &corev1.ConfigMap{}
	case "v1/Service":
		typed = &corev1.Service{}
	default:
		return fmt.Errorf("create kind is unsupported")
	}
	raw, err := json.Marshal(object.Object)
	if err != nil {
		return fmt.Errorf("invalid accepted object")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(typed) != nil {
		return fmt.Errorf("accepted object has invalid schema kinds")
	}
	return nil
}

func (e PlanExecutor) observeObjectEffect(ctx context.Context, patch PlanPatch) error {
	if err := e.Safety.authorizeRead(patch.Ref, patch.SourceCommit, e.safetyNow()); err != nil {
		return err
	}
	live, err := e.Client.Get(ctx, patch.Ref)
	if patch.Operation == "delete" {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete observation unknown: named object is not confirmed absent")
	}
	if err != nil || live == nil {
		return fmt.Errorf("create observation unknown: current object is unavailable")
	}
	if patch.After.GetUID() == "" || live.GetUID() != patch.After.GetUID() {
		return fmt.Errorf("create observation unknown: UID changed or unavailable")
	}
	if err := e.Safety.checkObservedObject(live, patch.After, patch.Ref, true); err != nil {
		return err
	}
	for _, field := range []string{"spec", "data", "binaryData"} {
		if expected, exists := patch.After.Object[field]; exists && !planValueContains(live.Object[field], expected) {
			return fmt.Errorf("create observation unknown: source fields differ")
		}
	}
	return nil
}

func (e PlanExecutor) restoreCreatedObject(ctx context.Context, patch PlanPatch) error {
	client, ok := e.Client.(PlanLifecycleClient)
	if !ok {
		return fmt.Errorf("create compensation requires lifecycle client")
	}
	if err := e.Safety.authorizeRead(patch.Ref, patch.SourceCommit, e.safetyNow()); err != nil {
		return err
	}
	if err := e.validateAcceptedRevision(IncidentRequest{SourceCommit: patch.SourceCommit}); err != nil {
		return err
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
	if err := e.Safety.checkObservedObject(live, patch.After, patch.Ref, true); err != nil {
		return err
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
	if _, err := e.Client.Get(ctx, patch.Ref); !apierrors.IsNotFound(err) {
		return fmt.Errorf("create compensation absence is unknown")
	}
	return nil
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
	patch := result.Patches[index]
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
