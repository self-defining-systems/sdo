package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// PlanObjectClient is the Kubernetes object boundary used by plan application.
type PlanObjectClient interface {
	Get(context.Context, sdk.ObjectRef) (*unstructured.Unstructured, error)
	Patch(context.Context, sdk.ObjectRef, types.PatchType, []byte) error
}

// PlanSource reads manifests and revisions from trusted, accepted source state.
type PlanSource interface {
	AcceptedManifest(context.Context, string, sdk.ObjectRef) (*unstructured.Unstructured, error)
	Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error)
}

type PlanRollouts interface {
	Wait(context.Context, []sdk.ObjectRef) error
}

type PlanVerifier interface {
	Verify(context.Context) (bool, error)
}

type PlanPatch struct {
	Ref    sdk.ObjectRef
	Patch  json.RawMessage
	Before *unstructured.Unstructured
	After  *unstructured.Unstructured
}

type PlanPreImage struct {
	Ref    sdk.ObjectRef              `json:"ref"`
	Object *unstructured.Unstructured `json:"object"`
}

// PlanApplicationResult contains the PlanApplication fields, generated patches,
// and the pre-images needed to reverse an applied plan.
type PlanApplicationResult struct {
	Plan                  program.Plan
	Applied               bool
	DryRun                bool
	PendingAction         *int
	RollingBack           bool
	RollbackCompleted     int
	RolledBack            bool
	RollbackError         string
	VerificationPassed    bool
	VerificationCompleted bool
	ActionsApplied        []program.Action
	Patches               []PlanPatch
	Duration              time.Duration
	PreImages             []PlanPreImage
}

type PlanExecutor struct {
	Client   PlanObjectClient
	Source   PlanSource
	Rollouts PlanRollouts
	Verifier PlanVerifier
	// Observer, when set, is the cache the verifier reads; Apply waits for it to
	// show the application's writes before verifying.
	Observer WriteObserver
	Now      func() time.Time
}

// Apply writes the plan once and reports. A write that fails part-way through a
// multi-action plan is compensated; once every write has landed, a failed
// rollout wait or health verification is reported and left to the caller.
func (e PlanExecutor) Apply(ctx context.Context, incident IncidentRequest, plan program.Plan, dryRun bool) (result PlanApplicationResult, err error) {
	started := time.Now()
	result = PlanApplicationResult{Plan: plan, DryRun: dryRun}
	writesLanded := false
	defer func() {
		result.Duration = time.Since(started)
		if err != nil && result.PendingAction != nil {
			recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			reconciled, reconcileErr := e.Reconcile(recoveryCtx, result)
			cancel()
			result = reconciled
			if reconcileErr != nil {
				err = errors.Join(err, reconcileErr)
				return
			}
		}
		if err != nil && result.Applied && !writesLanded {
			recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if rollbackErr := e.rollback(recoveryCtx, &result); rollbackErr != nil {
				result.RollbackError = rollbackErr.Error()
				result.RollingBack = true
				err = errors.Join(err, fmt.Errorf("compensate partial application: %w", rollbackErr))
			} else {
				result.RolledBack = true
			}
		}
	}()
	if err := plan.Validate(); err != nil {
		return result, fmt.Errorf("validate plan: %w", err)
	}
	if !dryRun && incident.RepairPolicy != "recorded-actions" {
		return result, fmt.Errorf("repair policy %q denies direct plan mutations", incident.RepairPolicy)
	}
	if e.Client == nil {
		return result, errors.New("plan object client is required")
	}
	if !dryRun && (e.Rollouts == nil || e.Verifier == nil) {
		return result, errors.New("rollout waiter and health verifier are required")
	}
	result.PreImages = make([]PlanPreImage, 0, len(plan.Actions))
	current := make(map[sdk.ObjectRef]*unstructured.Unstructured)
	originals := make(map[sdk.ObjectRef]*unstructured.Unstructured)
	refs := make([]sdk.ObjectRef, 0, len(plan.Actions))
	// Capture every pre-image before any write, including later actions' targets.
	for _, action := range plan.Actions {
		ref, refErr := planActionRef(action)
		if refErr != nil {
			return result, refErr
		}
		if err := validatePlanRef(ref); err != nil {
			return result, err
		}
		if incident.Namespace != "" && ref.Namespace != incident.Namespace {
			return result, fmt.Errorf("plan object %s/%s is outside incident namespace %q", ref.Namespace, ref.Name, incident.Namespace)
		}
		if _, exists := current[ref]; exists {
			continue
		}
		object, getErr := e.Client.Get(ctx, ref)
		if getErr != nil {
			return result, fmt.Errorf("read pre-image of %s/%s: %w", ref.Kind, ref.Name, getErr)
		}
		if object == nil || !samePlanRef(object, ref) {
			return result, fmt.Errorf("pre-image does not match %s/%s", ref.Kind, ref.Name)
		}
		result.PreImages = append(result.PreImages, PlanPreImage{Ref: ref, Object: object.DeepCopy()})
		current[ref] = object.DeepCopy()
		originals[ref] = object.DeepCopy()
		refs = append(refs, ref)
	}
	for _, action := range plan.Actions {
		ref, _ := planActionRef(action)
		next, actionErr := e.prepareAction(ctx, incident, action, current[ref])
		if actionErr != nil {
			return result, actionErr
		}
		before, marshalErr := json.Marshal(current[ref].Object)
		if marshalErr != nil {
			return result, fmt.Errorf("encode pre-image of %s/%s: %w", ref.Kind, ref.Name, marshalErr)
		}
		after, marshalErr := json.Marshal(next.Object)
		if marshalErr != nil {
			return result, fmt.Errorf("encode action for %s/%s: %w", ref.Kind, ref.Name, marshalErr)
		}
		patch, patchErr := jsonpatch.CreateMergePatch(before, after)
		if patchErr != nil {
			return result, fmt.Errorf("patch %s/%s: %w", ref.Kind, ref.Name, patchErr)
		}
		patch, patchErr = conditionalPlanPatch(patch, current[ref])
		if patchErr != nil {
			return result, patchErr
		}
		result.Patches = append(result.Patches, PlanPatch{Ref: ref, Patch: patch, Before: current[ref].DeepCopy(), After: next.DeepCopy()})
		current[ref] = next
	}
	// Rollback and write reconciliation compare after-images with live values, so
	// the plan's values take the form the API server stores.
	for i := range result.Patches {
		canonicalizePlanObject(result.Patches[i].After)
	}
	if dryRun {
		return result, nil
	}
	// The first write to an object is conditional on its pre-image; later writes to the same
	// object are conditional only on its identity, because the object's own controllers bump
	// resourceVersion between our writes.
	written := make(map[sdk.ObjectRef]bool)
	for index, patch := range result.Patches {
		if written[patch.Ref] {
			var patchErr error
			result.Patches[index].Patch, patchErr = uidOnlyPlanPatch(patch.Patch, originals[patch.Ref])
			if patchErr != nil {
				return result, patchErr
			}
		}
		pending := index
		result.PendingAction = &pending
		if err := e.Client.Patch(ctx, patch.Ref, types.MergePatchType, result.Patches[index].Patch); err != nil {
			return result, fmt.Errorf("apply action %d to %s/%s: %w", index, patch.Ref.Kind, patch.Ref.Name, err)
		}
		written[patch.Ref] = true
		result.Applied = true
		result.ActionsApplied = append(result.ActionsApplied, plan.Actions[index])
		result.PendingAction = nil
		// Rollback compares against the version our write produced; if the read fails the
		// after-image keeps its predicted version and rollback retries on a conflict.
		if observed, getErr := e.Client.Get(ctx, patch.Ref); getErr == nil {
			result.Patches[index].After.SetResourceVersion(observed.GetResourceVersion())
		}
	}
	writesLanded = true
	result.Applied = true
	if err := e.awaitObserved(ctx, result.Patches); err != nil {
		return result, err
	}
	if err := e.Rollouts.Wait(ctx, refs); err != nil {
		return result, fmt.Errorf("wait for plan rollouts: %w", err)
	}
	passed, err := e.Verifier.Verify(ctx)
	if err != nil {
		return result, fmt.Errorf("verify plan health: %w", err)
	}
	result.VerificationPassed = passed
	result.VerificationCompleted = true
	return result, nil
}

// Rollback restores only this application's writes, with identity and version guards.
func (e PlanExecutor) Rollback(ctx context.Context, application PlanApplicationResult) error {
	return e.rollback(ctx, &application)
}

// rollback advances the caller's result even when compensation stops partway.
func (e PlanExecutor) rollback(ctx context.Context, application *PlanApplicationResult) error {
	if application.RolledBack {
		return nil
	}
	if e.Client == nil {
		return errors.New("plan object client is required")
	}
	application.RollingBack = true
	for index := len(application.ActionsApplied) - 1 - application.RollbackCompleted; index >= 0; index-- {
		if index >= len(application.Patches) {
			return errors.New("rollback requires in-process field writes")
		}
		patch := application.Patches[index]
		if patch.Before == nil || patch.After == nil {
			return errors.New("rollback requires before and after images")
		}
		if err := e.restoreAction(ctx, patch); err != nil {
			return err
		}
		application.RollbackCompleted++
	}
	application.RollingBack = false
	application.RolledBack = true
	application.RollbackError = ""
	return nil
}

// maxRestoreAttempts bounds retries when a controller bumps resourceVersion
// between the restore's read and its conditional patch.
const maxRestoreAttempts = 5

// restoreAction reverts one applied write. Each attempt re-reads the object, so
// a version conflict from an unrelated writer is retried with the fresh version
// while a changed owned field still fails.
func (e PlanExecutor) restoreAction(ctx context.Context, patch PlanPatch) error {
	var err error
	for attempt := 0; attempt < maxRestoreAttempts; attempt++ {
		if err = e.restoreActionOnce(ctx, patch); !apierrors.IsConflict(err) {
			return err
		}
	}
	return err
}

func (e PlanExecutor) restoreActionOnce(ctx context.Context, patch PlanPatch) error {
	current, err := e.Client.Get(ctx, patch.Ref)
	if err != nil {
		return err
	}
	if current.GetUID() != patch.Before.GetUID() {
		return fmt.Errorf("rollback conflict: %s/%s was recreated", patch.Ref.Kind, patch.Ref.Name)
	}
	restored := current.DeepCopy()
	for _, field := range []string{"spec", "data", "binaryData"} {
		before, bok := patch.Before.Object[field]
		after, aok := patch.After.Object[field]
		live, lok := current.Object[field]
		value, exists, err := restorePlanFields(before, bok, after, aok, live, lok)
		if err != nil {
			return fmt.Errorf("rollback conflict on %s/%s %s: %w", patch.Ref.Kind, patch.Ref.Name, field, err)
		}
		if exists {
			restored.Object[field] = value
		} else {
			delete(restored.Object, field)
		}
	}
	from, _ := json.Marshal(current.Object)
	to, _ := json.Marshal(restored.Object)
	delta, err := jsonpatch.CreateMergePatch(from, to)
	if err != nil {
		return err
	}
	if string(delta) == "{}" {
		return nil
	}
	delta, err = conditionalPlanPatch(delta, current)
	if err != nil {
		return err
	}
	if err := e.Client.Patch(ctx, patch.Ref, types.MergePatchType, delta); err != nil {
		return fmt.Errorf("restore %s/%s: %w", patch.Ref.Kind, patch.Ref.Name, err)
	}
	return nil
}

func conditionalPlanPatch(patch []byte, observed *unstructured.Unstructured) ([]byte, error) {
	if observed.GetUID() == "" && observed.GetResourceVersion() == "" {
		return patch, nil
	}
	var fields map[string]any
	if err := json.Unmarshal(patch, &fields); err != nil {
		return nil, err
	}
	fields["metadata"] = map[string]any{"uid": string(observed.GetUID()), "resourceVersion": observed.GetResourceVersion()}
	return json.Marshal(fields)
}

func uidOnlyPlanPatch(patch []byte, observed *unstructured.Unstructured) ([]byte, error) {
	var fields map[string]any
	if err := json.Unmarshal(patch, &fields); err != nil {
		return nil, err
	}
	if observed.GetUID() == "" {
		delete(fields, "metadata")
	} else {
		fields["metadata"] = map[string]any{"uid": string(observed.GetUID())}
	}
	return json.Marshal(fields)
}

// Descend into equal-length arrays so a probe repair cannot roll back an image update.
// Array shape changes and changed named-item identities conservatively conflict.
func restorePlanFields(before any, bok bool, after any, aok bool, live any, lok bool) (any, bool, error) {
	if bok == aok && planValuesEqual(before, after) {
		return live, lok, nil
	}
	if bok == lok && planValuesEqual(before, live) {
		return live, lok, nil
	} // already compensated
	bm, bmap := before.(map[string]any)
	am, amap := after.(map[string]any)
	lm, lmap := live.(map[string]any)
	if bmap && amap && lmap {
		result := runtime.DeepCopyJSONValue(lm).(map[string]any)
		keys := map[string]bool{}
		for k := range bm {
			keys[k] = true
		}
		for k := range am {
			keys[k] = true
		}
		for k := range keys {
			bv, bexists := bm[k]
			av, aexists := am[k]
			lv, lexists := lm[k]
			v, exists, err := restorePlanFields(bv, bexists, av, aexists, lv, lexists)
			if err != nil {
				return nil, false, fmt.Errorf("%s: %w", k, err)
			}
			if exists {
				result[k] = v
			} else {
				delete(result, k)
			}
		}
		return result, true, nil
	}
	ba, barray := before.([]any)
	aa, aarray := after.([]any)
	la, larray := live.([]any)
	if barray && aarray && larray && len(ba) == len(aa) && len(aa) == len(la) {
		result := make([]any, len(la))
		for i := range la {
			// An unchanged name is still an identity precondition. Without it,
			// another item's pre-image value can look like completed compensation.
			bm, _ := ba[i].(map[string]any)
			am, _ := aa[i].(map[string]any)
			lm, _ := la[i].(map[string]any)
			if !planValuesEqual(bm["name"], am["name"]) || !planValuesEqual(am["name"], lm["name"]) {
				return nil, false, errors.New("named list item identity changed")
			}
			v, _, err := restorePlanFields(ba[i], true, aa[i], true, la[i], true)
			if err != nil {
				return nil, false, err
			}
			result[i] = v
		}
		return result, true, nil
	}
	if aok != lok || !planValueContains(live, after) {
		return nil, false, errors.New("field no longer holds application value")
	}
	return before, bok, nil
}

// planValueContains reports whether live equals applied, ignoring keys the API server added to
// applied objects (defaults such as a probe's scheme or a port's protocol).
func planValueContains(live, applied any) bool {
	switch a := applied.(type) {
	case map[string]any:
		l, ok := live.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range a {
			if lv, exists := l[key]; !exists || !planValueContains(lv, value) {
				return false
			}
		}
		return true
	case []any:
		l, ok := live.([]any)
		if !ok || len(l) != len(a) {
			return false
		}
		for i := range a {
			if !planValueContains(l[i], a[i]) {
				return false
			}
		}
		return true
	}
	return planValuesEqual(applied, live)
}

func (e PlanExecutor) prepareAction(ctx context.Context, incident IncidentRequest, action program.Action, current *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	next := current.DeepCopy()
	switch a := action.(type) {
	case program.ReconcileFromSource:
		if e.Source == nil || incident.SourceCommit == "" {
			return nil, errors.New("accepted source is required for reconcile")
		}
		source, err := e.Source.AcceptedManifest(ctx, incident.SourceCommit, a.Ref)
		if err != nil {
			return nil, err
		}
		return sourceFields(next, source, a.Ref)
	case program.Rollback:
		if e.Source == nil || a.ToRevision == "" {
			return nil, errors.New("revision source and target revision are required")
		}
		previous, err := e.Source.Revision(ctx, a.Ref, a.ToRevision)
		if err != nil {
			return nil, err
		}
		return sourceFields(next, previous, a.Ref)
	case program.SetField:
		encoded, err := json.Marshal(a.Value)
		if err != nil {
			return nil, err
		}
		var value any
		if err := json.Unmarshal(encoded, &value); err != nil {
			return nil, err
		}
		if err := program.ApplySetField(next.Object, a.Path, value); err != nil {
			return nil, err
		}
	case program.RestartRollout:
		if a.Ref.Kind != "Deployment" || a.Ref.APIVersion != "apps/v1" {
			return nil, errors.New("RestartRollout requires an apps/v1 Deployment")
		}
		now := time.Now
		if e.Now != nil {
			now = e.Now
		}
		if err := unstructured.SetNestedField(next.Object, now().UTC().Format(time.RFC3339Nano), "spec", "template", "metadata", "annotations", "kubectl.kubernetes.io/restartedAt"); err != nil {
			return nil, err
		}
	case *program.ReconcileFromSource:
		if a == nil {
			return nil, errors.New("nil reconcile action")
		}
		return e.prepareAction(ctx, incident, *a, current)
	case *program.Rollback:
		if a == nil {
			return nil, errors.New("nil rollback action")
		}
		return e.prepareAction(ctx, incident, *a, current)
	case *program.SetField:
		if a == nil {
			return nil, errors.New("nil SetField action")
		}
		return e.prepareAction(ctx, incident, *a, current)
	case *program.RestartRollout:
		if a == nil {
			return nil, errors.New("nil restart action")
		}
		return e.prepareAction(ctx, incident, *a, current)
	default:
		return nil, fmt.Errorf("unsupported plan action %T", action)
	}
	return next, nil
}

func sourceFields(current, source *unstructured.Unstructured, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if source == nil || !samePlanRef(source, ref) {
		return nil, fmt.Errorf("source object does not match %s/%s", ref.Kind, ref.Name)
	}
	for _, field := range []string{"spec", "data", "binaryData"} {
		if value, ok := source.Object[field]; ok {
			current.Object[field] = runtime.DeepCopyJSONValue(value)
		}
	}
	return current, nil
}

func planActionRef(action program.Action) (sdk.ObjectRef, error) {
	switch a := action.(type) {
	case program.ReconcileFromSource:
		return a.Ref, nil
	case program.Rollback:
		return a.Ref, nil
	case program.SetField:
		return a.Ref, nil
	case program.RestartRollout:
		return a.Ref, nil
	case *program.ReconcileFromSource:
		if a != nil {
			return a.Ref, nil
		}
	case *program.Rollback:
		if a != nil {
			return a.Ref, nil
		}
	case *program.SetField:
		if a != nil {
			return a.Ref, nil
		}
	case *program.RestartRollout:
		if a != nil {
			return a.Ref, nil
		}
	default:
		return sdk.ObjectRef{}, fmt.Errorf("unsupported plan action %T", action)
	}
	return sdk.ObjectRef{}, fmt.Errorf("nil plan action %T", action)
}

func validatePlanRef(ref sdk.ObjectRef) error {
	if ref.APIVersion == "" || ref.Kind == "" || ref.Namespace == "" || ref.Name == "" {
		return fmt.Errorf("plan action requires a namespaced object reference: %+v", ref)
	}
	return nil
}

func samePlanRef(object *unstructured.Unstructured, ref sdk.ObjectRef) bool {
	return object.GetAPIVersion() == ref.APIVersion && object.GetKind() == ref.Kind && object.GetNamespace() == ref.Namespace && object.GetName() == ref.Name
}

// PlanHealthVerifier runs the accepted health detectors on a fresh snapshot.
type PlanHealthVerifier struct {
	Detectors []sdk.Detector
	Provider  core.SnapshotProvider
}

func (v PlanHealthVerifier) Verify(ctx context.Context) (bool, error) {
	sink := &planFindingSink{}
	var health []sdk.Detector
	for _, detector := range v.Detectors {
		if detector.Spec().Class == sdk.DetectorClassHealth {
			health = append(health, detector)
		}
	}
	if len(health) == 0 {
		return false, errors.New("health detectors are required")
	}
	if err := (core.Runner{Detectors: health, Provider: v.Provider, Sink: sink}).RunOnce(ctx); err != nil {
		return false, err
	}
	return !sink.firing, nil
}

type planFindingSink struct{ firing bool }

func (s *planFindingSink) Emit(_ context.Context, finding sdk.Finding) error {
	if finding.Status == sdk.FindingActive {
		s.firing = true
	}
	return nil
}

// Unstructured reloads integral JSON numbers as int64; compare JSON values rather
// than Go number representations across the durable boundary.
func planValuesEqual(left, right any) bool {
	a, err := json.Marshal(left)
	if err != nil {
		return false
	}
	b, err := json.Marshal(right)
	return err == nil && string(a) == string(b)
}
