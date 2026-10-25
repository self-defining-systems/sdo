package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
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
	Authorization string
	// DirectReadConfirmed is process-local proof, never restored from a receipt.
	DirectReadConfirmed bool `json:"-"`
	Operation           string
	Irreversible        bool
	Structured          bool
	SourceCommit        string
	Ref                 sdk.ObjectRef
	Patch               json.RawMessage
	Before              *unstructured.Unstructured
	After               *unstructured.Unstructured
}

type PlanPreImage struct {
	Ref    sdk.ObjectRef              `json:"ref"`
	Object *unstructured.Unstructured `json:"object"`
}

// PlanApplicationResult contains the PlanApplication fields, generated patches,
// and the pre-images needed to reverse an applied plan.
type PlanApplicationResult struct {
	// MitigationHealth is process-local tool feedback, not closure evidence.
	MitigationHealth         *MitigationHealth `json:"-"`
	Plan                     program.Plan
	Applied                  bool
	DryRun                   bool
	PendingAction            *int
	RollingBack              bool
	RollbackCompleted        int
	RolledBack               bool
	RollbackError            string
	VerificationPassed       bool
	VerificationCompleted    bool
	RolloutPending           *contracts.RolloutPending
	FinalEquivalenceVerified *bool
	ActionsApplied           []program.Action
	Patches                  []PlanPatch
	Duration                 time.Duration
	PreImages                []PlanPreImage
	ObjectEffects            []PlanObjectEffect
	ActionReports            []PlanActionReport
}

type PlanExecutor struct {
	// PollRollouts returns pending after one observation instead of waiting.
	PollRollouts bool
	DeleteObject func(context.Context, sdk.ObjectRef, types.UID, string) error
	// RolloutTimeout can shorten the default ninety-second rollout budget.
	RecordPrimitive func(sdktest.PrimitiveCall) error
	RolloutTimeout  time.Duration
	Client          PlanObjectClient
	Source          PlanSource
	Rollouts        PlanRollouts
	Verifier        PlanVerifier
	// Observer, when set, is the cache the verifier reads; Apply waits for it to
	// show the application's writes before verifying.
	Observer WriteObserver
	Now      func() time.Time
	Values   program.ValueInputs
	report   *PlanActionReport
}

// Apply writes the plan and reports. A version conflict before anything was
// written (a status or controller bump since the pre-image read) is not a
// refusal: the plan is applied again against the fresh object, up to
// maxRestoreAttempts times. A write that fails part-way through a multi-action
// plan is compensated; once every write has landed, a failed rollout wait or
// health verification is reported and left to the caller.
func (e PlanExecutor) Apply(ctx context.Context, incident IncidentRequest, plan program.Plan, dryRun bool) (result PlanApplicationResult, err error) {
	defer func() {
		if err != nil {
			reason := "execution_failed"
			if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
				reason = "authorization_denied"
			}
			for i := range result.ActionReports {
				if result.ActionReports[i].Status != "applied" {
					if result.ActionReports[i].Status == "unknown" && reason != "authorization_denied" {
						reason = "outcome_unknown"
					}
					if result.ActionReports[i].FailureReason == "" {
						result.ActionReports[i].FailureReason = reason
					}
					break
				}
			}
		}
	}()
	// A mitigation never spends the incident's global health budget. Keep
	// write/rollout confirmation, then independently sample its own scope once.
	if verifier := scopedMitigationVerifier(e.Verifier, plan, incident.Findings); verifier != nil && !dryRun {
		e.Verifier = verifier
		defer func() { result.MitigationHealth = verifier.observation }()
	}

	for attempt := 0; attempt < maxRestoreAttempts; attempt++ {
		result, err = e.applyOnce(ctx, incident, plan, dryRun)
		if err == nil || result.Applied || !apierrors.IsConflict(err) {
			break
		}
	}
	return result, err
}

func (e PlanExecutor) applyOnce(ctx context.Context, incident IncidentRequest, plan program.Plan, dryRun bool) (result PlanApplicationResult, err error) {
	if hasGeneralActions(plan) {
		return e.applyGeneralPlan(ctx, incident, plan, dryRun)
	}
	if hasRuntimeEffects(plan) {
		return e.applyRuntimePlan(ctx, incident, plan, dryRun)
	}
	if hasObjectActions(plan) {
		return e.applyObjectPlan(ctx, incident, plan, dryRun)
	}
	started := time.Now()
	result = PlanApplicationResult{Plan: plan, DryRun: dryRun, ActionReports: newPlanReports(plan)}
	defer func() { settlePlanReports(&result, err) }()
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
				result.RollingBack = result.RollbackCompleted < len(result.ActionsApplied)
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
		if incident.Namespace != "" && ref.Namespace != incident.Namespace && !genericIncidentRef(incident, action, ref) {
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
	for actionIndex, action := range plan.Actions {
		e.report = &result.ActionReports[actionIndex]
		ref, _ := planActionRef(action)
		next, actionErr := e.prepareAction(ctx, incident, action, current[ref])
		if actionErr != nil {
			return result, fmt.Errorf("prepare learned repair: %w; investigate accepted source and intended application behavior", actionErr)
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
		result.Patches = append(result.Patches, PlanPatch{Authorization: planAuthorizationMode(action), Ref: ref, Patch: patch, Before: current[ref].DeepCopy(), After: next.DeepCopy(), Structured: structuredPlanRef(plan, ref), SourceCommit: incident.SourceCommit})
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
		if written[patch.Ref] && isRemoveServicePort(plan.Actions[index]) {
			fresh, readErr := e.Client.Get(ctx, patch.Ref)
			if readErr != nil || fresh == nil || !samePlanRef(fresh, patch.Ref) || fresh.GetUID() != originals[patch.Ref].GetUID() {
				return result, errors.New("Service port removal identity changed")
			}
			next, prepareErr := e.prepareAction(ctx, incident, plan.Actions[index], fresh)
			if prepareErr != nil {
				return result, prepareErr
			}
			before, marshalErr := json.Marshal(fresh.Object)
			if marshalErr != nil {
				return result, marshalErr
			}
			after, marshalErr := json.Marshal(next.Object)
			if marshalErr != nil {
				return result, marshalErr
			}
			delta, patchErr := jsonpatch.CreateMergePatch(before, after)
			if patchErr != nil {
				return result, patchErr
			}
			delta, patchErr = conditionalPlanPatch(delta, fresh)
			if patchErr != nil {
				return result, patchErr
			}
			canonicalizePlanObject(next)
			result.Patches[index].Before = fresh.DeepCopy()
			result.Patches[index].After = next
			result.Patches[index].Patch = delta
		} else if written[patch.Ref] {
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
		if err := e.verifyStructuredWrites(ctx, PlanApplicationResult{Patches: []PlanPatch{result.Patches[index]}}); err != nil {
			return result, err
		}
		if err := e.verifyValueWrite(ctx, plan.Actions[index], result.Patches[index]); err != nil {
			return result, err
		}
		result.Patches[index].DirectReadConfirmed = patch.Structured
	}
	writesLanded = true
	result.Applied = true
	if err := e.awaitObserved(ctx, result.Patches); err != nil {
		return result, err
	}
	if pending, err := e.waitForRollouts(ctx, incident, refs); err != nil {
		result.RolloutPending = pending
		return result, err
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
	var irreversible error
	// Keep irreversible effects disclosed even when resuming a completed walk.
	for i := 0; i < len(application.ActionsApplied) && i < len(application.Patches); i++ {
		patch := application.Patches[i]
		if patch.Operation == "delete" || patch.Irreversible {
			irreversible = errors.Join(irreversible, fmt.Errorf("irreversible %s effect on %s/%s was not restored; never auto-recreate or replay", patch.Operation, patch.Ref.Kind, patch.Ref.Name))
		}
	}

	// RollbackCompleted checkpoints only the contiguous processed suffix. If a
	// step fails, still attempt every earlier write; idempotent restores safely
	// revisit those successes on retry without skipping the failed step.
	contiguous := true
	for index := len(application.ActionsApplied) - 1 - application.RollbackCompleted; index >= 0; index-- {
		restoreErr := func() error {
			if index >= len(application.Patches) {
				return errors.New("rollback requires in-process field writes")
			}
			patch := application.Patches[index]
			if patch.Operation == "delete" || patch.Irreversible {
				return nil
			}
			if patch.Operation == "create" {
				return e.restoreCreatedObject(ctx, patch)
			}
			if patch.Before == nil || patch.After == nil {
				return errors.New("rollback requires before and after images")
			}
			return e.restoreAction(ctx, patch)
		}()
		if restoreErr != nil {
			irreversible = errors.Join(irreversible, restoreErr)
			contiguous = false
		} else if contiguous {
			application.RollbackCompleted++
		}
	}
	application.RollingBack = application.RollbackCompleted < len(application.ActionsApplied)
	if irreversible != nil {
		application.RollbackError = irreversible.Error()
		return irreversible
	}

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
	for _, field := range planPatchFields(patch) {
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
	if patch.Structured {
		observed, err := e.Client.Get(ctx, patch.Ref)
		if err != nil {
			return fmt.Errorf("compensation observation unknown")
		}

		if observed.GetUID() != current.GetUID() {
			return fmt.Errorf("compensation observation unknown: UID changed")
		}
		for _, field := range planPatchFields(patch) {
			if !planValuesEqual(observed.Object[field], restored.Object[field]) {
				return fmt.Errorf("compensation observation unknown: restored fields differ")
			}
		}
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
	metadata, _ := fields["metadata"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadata["uid"] = string(observed.GetUID())
	metadata["resourceVersion"] = observed.GetResourceVersion()
	fields["metadata"] = metadata
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
	case program.RemoveServicePort:
		if err := program.ApplyRemoveServicePort(next.Object, a); err != nil {
			return nil, err
		}
	case *program.RemoveServicePort:
		if a == nil {
			return nil, errors.New("nil RemoveServicePort")
		}
		return e.prepareAction(ctx, incident, *a, current)
	case program.SetField:
		value, provenance, err := program.ResolveSetFieldValue(a, e.valueInputs(ctx, incident))
		if err != nil {
			return nil, err
		}
		if e.report != nil {
			e.report.Values = append(e.report.Values, provenance)
		}
		if err := program.ApplySetFieldAction(next.Object, a, value); err != nil {
			return nil, err
		}

	case program.ScaleWorkload:
		value, provenance, err := program.ResolveValue(a.Replicas, e.valueInputs(ctx, incident))
		if err != nil {
			return nil, err
		}
		field := program.SetField{Ref: a.Ref, Path: "spec.replicas", Value: a.Replicas}
		if err := program.ValidateResolvedSetField(field, value); err != nil {
			return nil, err
		}
		raw, _ := json.Marshal(value)
		var replicas int32
		if err := json.Unmarshal(raw, &replicas); err != nil || replicas < 0 {
			return nil, fmt.Errorf("replicas must be a nonnegative int32")
		}
		if e.report != nil {
			e.report.Values = append(e.report.Values, provenance)
		}
		if err := unstructured.SetNestedField(next.Object, int64(replicas), "spec", "replicas"); err != nil {
			return nil, err
		}
	case *program.ScaleWorkload:
		if a == nil {
			return nil, errors.New("nil ScaleWorkload")
		}
		return e.prepareAction(ctx, incident, *a, current)

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
	// Source configuration is open-ended (RBAC, admission, custom resources).
	// Identity/status stay API-owned; source metadata keys merge so an
	// independent controller's completion annotations survive reconciliation.
	for field, value := range source.Object {
		switch field {
		case "apiVersion", "kind", "status":
			continue
		case "metadata":
			metadata, _ := value.(map[string]any)
			for key, declared := range metadata {
				switch key {
				case "name", "namespace", "uid", "resourceVersion", "generation", "creationTimestamp", "deletionTimestamp", "managedFields":
					continue
				}
				if key == "annotations" || key == "labels" {
					values, _ := declared.(map[string]any)
					for name, item := range values {
						if err := unstructured.SetNestedField(current.Object, runtime.DeepCopyJSONValue(item), "metadata", key, name); err != nil {
							return nil, err
						}
					}
				} else if err := unstructured.SetNestedField(current.Object, runtime.DeepCopyJSONValue(declared), "metadata", key); err != nil {
					return nil, err
				}
			}
		default:
			current.Object[field] = runtime.DeepCopyJSONValue(value)
		}
	}
	return current, nil
}

func planActionRef(action program.Action) (sdk.ObjectRef, error) {
	if ref, ok := externalRef(action); ok {
		return ref, nil
	}
	switch a := generalAction(action).(type) {
	case program.PatchObject:
		return a.Ref, nil
	case program.ApplyObject:
		return a.Ref, nil
	case program.KubernetesRequest:
		return a.Ref, nil
	case program.WaitFor:
		return a.Ref, nil
	case program.EnsureDatabaseUser:
		return a.Target.Pod, nil
	case *program.EnsureDatabaseUser:
		if a != nil {
			return a.Target.Pod, nil
		}
	case program.RemoveServicePort:
		return a.Ref, nil
	case *program.RemoveServicePort:
		if a != nil {
			return a.Ref, nil
		}
	case program.DeletePod:
		return a.Ref, nil
	case program.ScaleWorkload:
		return a.Ref, nil
	case program.Exec:
		return a.Pod, nil
	case *program.DeletePod:
		if a != nil {
			return a.Ref, nil
		}
	case *program.ScaleWorkload:
		if a != nil {
			return a.Ref, nil
		}
	case *program.Exec:
		if a != nil {
			return a.Pod, nil
		}
	case program.CreateFromSource:
		return a.Ref, nil
	case program.DeleteObject:
		return a.Ref, nil
	case *program.CreateFromSource:
		if a != nil {
			return a.Ref, nil
		}
	case *program.DeleteObject:
		if a != nil {
			return a.Ref, nil
		}
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
	if ref.APIVersion == "" || ref.Kind == "" || ref.Name == "" {
		return fmt.Errorf("plan action requires a versioned object reference: %+v", ref)
	}
	return nil
}

func samePlanRef(object *unstructured.Unstructured, ref sdk.ObjectRef) bool {
	return object.GetAPIVersion() == ref.APIVersion && object.GetKind() == ref.Kind && object.GetNamespace() == ref.Namespace && object.GetName() == ref.Name
}

// PlanHealthVerifier runs the accepted health detectors on a fresh snapshot.
type PlanHealthVerifier struct {
	Detectors    []sdk.Detector
	Provider     core.SnapshotProvider
	Timeout      time.Duration
	PollInterval time.Duration
}

func (v PlanHealthVerifier) Verify(ctx context.Context) (bool, error) {
	var health []sdk.Detector
	for _, detector := range v.Detectors {
		if detector.Spec().Class == sdk.DetectorClassHealth {
			health = append(health, detector)
		}
	}
	if len(health) == 0 {
		return false, errors.New("health detectors are required")
	}
	budget := v.Timeout
	if budget <= 0 {
		sink := &planFindingSink{}
		confirmed, err := v.sample(ctx, health, sink)
		if err != nil {
			return false, err
		}
		return confirmed && !sink.firing, nil
	}
	interval := v.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	verification, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	for {
		sink := &planFindingSink{}
		confirmed, err := v.sample(verification, health, sink)
		if err != nil {
			if ctx.Err() == nil && errors.Is(verification.Err(), context.DeadlineExceeded) {
				return false, nil
			}
			return false, err
		}
		if confirmed && !sink.firing {
			return true, nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-verification.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			return false, nil
		case <-timer.C:
		}
	}
}

// Confirmation and findings must come from the same fresh snapshot.
func (v PlanHealthVerifier) sample(ctx context.Context, detectors []sdk.Detector, sink *planFindingSink) (bool, error) {
	if v.Provider == nil {
		return false, errors.New("snapshot provider is required")
	}
	snapshot, err := v.Provider.Snapshot(ctx)
	if err != nil {
		return false, fmt.Errorf("create detection snapshot: %w", err)
	}
	if err := (core.Runner{Detectors: detectors, Provider: core.StaticSnapshotProvider{SnapshotValue: snapshot}, Sink: sink}).RunOnce(ctx); err != nil {
		return false, err
	}
	return detectorsConfirmed(detectors, snapshot), nil
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

// Generic field plans may cross the app boundary only for an exact object
// already cited by the incident, never from an arbitrary program-supplied name.
func genericIncidentRef(incident IncidentRequest, action program.Action, ref sdk.ObjectRef) bool {
	var path string
	switch a := action.(type) {
	case program.SetField:
		path = a.Path
	case *program.SetField:
		if a != nil {
			path = a.Path
		}
	}
	if !strings.HasPrefix(path, "/") {
		return false
	}
	for _, known := range findingCaptureRefs(incident.Findings) {
		if known == ref {
			return true
		}
	}
	return false
}

func isRemoveServicePort(action program.Action) bool {
	switch action.(type) {
	case program.RemoveServicePort, *program.RemoveServicePort:
		return true
	}
	return false
}
