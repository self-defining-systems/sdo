package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
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
	FinalEquivalenceVerified *bool
	ActionsApplied           []program.Action
	Patches                  []PlanPatch
	Duration                 time.Duration
	PreImages                []PlanPreImage
	ObjectEffects            []PlanObjectEffect
}

type PlanExecutor struct {
	Client PlanObjectClient
	Source PlanSource
	// Safety is trusted controller-owned provenance for structured actions.
	Safety *PlanSafetyPolicy
	// AcceptedRevision reads the controller-owned accepted head, never the agent checkout.
	AcceptedRevision func() string
	Rollouts         PlanRollouts
	Verifier         PlanVerifier
	// Observer, when set, is the cache the verifier reads; Apply waits for it to
	// show the application's writes before verifying.
	Observer WriteObserver
	Now      func() time.Time
}

// Apply writes the plan and reports. A version conflict before anything was
// written (a status or controller bump since the pre-image read) is not a
// refusal: the plan is applied again against the fresh object, up to
// maxRestoreAttempts times. A write that fails part-way through a multi-action
// plan is compensated; once every write has landed, a failed rollout wait or
// health verification is reported and left to the caller.
func (e PlanExecutor) Apply(ctx context.Context, incident IncidentRequest, plan program.Plan, dryRun bool) (result PlanApplicationResult, err error) {
	for attempt := 0; attempt < maxRestoreAttempts; attempt++ {
		result, err = e.applyOnce(ctx, incident, plan, dryRun)
		if err == nil || result.Applied || !apierrors.IsConflict(err) {
			break
		}
	}
	return result, err
}

func (e PlanExecutor) applyOnce(ctx context.Context, incident IncidentRequest, plan program.Plan, dryRun bool) (result PlanApplicationResult, err error) {
	if hasObjectActions(plan) {
		return e.applyObjectPlan(ctx, incident, plan, dryRun)
	}
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
		if incident.Namespace != "" && ref.Namespace != incident.Namespace {
			return result, fmt.Errorf("plan object %s/%s is outside incident namespace %q", ref.Namespace, ref.Name, incident.Namespace)
		}
		if _, exists := current[ref]; exists {
			continue
		}
		if structuredPlanRef(plan, ref) {
			if err := e.validateAcceptedRevision(incident); err != nil {
				return result, err
			}
			if err := e.Safety.authorizeRead(ref, incident.SourceCommit, e.safetyNow()); err != nil {
				return result, err
			}
		}
		object, getErr := e.Client.Get(ctx, ref)
		if getErr != nil {
			return result, fmt.Errorf("read pre-image of %s/%s: %w", ref.Kind, ref.Name, getErr)
		}
		if object == nil || !samePlanRef(object, ref) {
			return result, fmt.Errorf("pre-image does not match %s/%s", ref.Kind, ref.Name)
		}
		if structuredPlanRef(plan, ref) {
			if object.GetUID() == "" {
				return result, fmt.Errorf("plan safety: live UID is required")
			}
			if err := e.Safety.checkObject(object, ref); err != nil {
				return result, err
			}
		}
		result.PreImages = append(result.PreImages, PlanPreImage{Ref: ref, Object: object.DeepCopy()})
		current[ref] = object.DeepCopy()
		originals[ref] = object.DeepCopy()
		refs = append(refs, ref)
	}
	guarded := false
	for _, action := range plan.Actions {
		ref, _ := planActionRef(action)
		next, actionErr := e.prepareAction(ctx, incident, action, current[ref], structuredPlanRef(plan, ref))
		if actionErr != nil {
			return result, fmt.Errorf("prepare learned repair: %w; investigate accepted source and intended application behavior", actionErr)
		}
		if structuredPlanRef(plan, ref) {
			if err := e.Safety.checkObject(next, ref); err != nil {
				return result, err
			}
		}
		checked, guardErr := e.validateSourceRepair(ctx, incident, ref, originals[ref], current[ref], next)
		guarded = guarded || checked
		if guardErr != nil {
			return result, guardErr
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
		result.Patches = append(result.Patches, PlanPatch{Ref: ref, Patch: patch, Before: current[ref].DeepCopy(), After: next.DeepCopy(), Structured: structuredPlanRef(plan, ref), SourceCommit: incident.SourceCommit})
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
		if patch.Structured {
			if err := e.Safety.authorizeRead(patch.Ref, incident.SourceCommit, e.safetyNow()); err != nil {
				return result, err
			}
			if err := e.Safety.checkObject(patch.After, patch.Ref); err != nil {
				return result, err
			}
			if err := e.validateAcceptedRevision(incident); err != nil {
				return result, err
			}
		}
		if guarded {
			if err := e.validateAcceptedRevision(incident); err != nil {
				return result, err
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
		result.Patches[index].DirectReadConfirmed = patch.Structured
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
	var irreversible error
	// Keep irreversible effects disclosed even when resuming a completed walk.
	for i := 0; i < len(application.ActionsApplied) && i < len(application.Patches); i++ {
		patch := application.Patches[i]
		if patch.Operation == "delete" {
			irreversible = errors.Join(irreversible, fmt.Errorf("irreversible deletion of %s/%s was not restored; never auto-recreate", patch.Ref.Kind, patch.Ref.Name))
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
			if patch.Operation == "delete" {
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
	if patch.Structured {
		if err := e.Safety.authorizeRead(patch.Ref, patch.SourceCommit, e.safetyNow()); err != nil {
			return err
		}
	}
	current, err := e.Client.Get(ctx, patch.Ref)
	if err != nil {
		return err
	}
	if patch.Structured {
		if err := e.Safety.checkObservedObject(current, patch.After, patch.Ref, false); err != nil {
			return err
		}
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
	if patch.Structured {
		if err := e.Safety.checkObservedObject(restored, patch.Before, patch.Ref, false); err != nil {
			return err
		}
	}
	if err := validateCredentialChannelDelta(patch.Before, restored); err != nil {
		return err
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
	if patch.Structured {
		if err := e.Safety.authorizeRead(patch.Ref, patch.SourceCommit, e.safetyNow()); err != nil {
			return err
		}
		if err := e.validateAcceptedRevision(IncidentRequest{SourceCommit: patch.SourceCommit}); err != nil {
			return err
		}
		// Restoring the captured pre-image does not require a new account grant.
		// The channel subset check above refuses anything absent from that image.

	}
	if err := e.Client.Patch(ctx, patch.Ref, types.MergePatchType, delta); err != nil {
		return fmt.Errorf("restore %s/%s: %w", patch.Ref.Kind, patch.Ref.Name, err)
	}
	if patch.Structured {
		observed, err := e.Client.Get(ctx, patch.Ref)
		if err != nil {
			return fmt.Errorf("compensation observation unknown")
		}
		if err := e.Safety.checkObject(observed, patch.Ref); err != nil {
			return err
		}
		if observed.GetUID() != current.GetUID() {
			return fmt.Errorf("compensation observation unknown: UID changed")
		}
		for _, field := range []string{"spec", "data", "binaryData"} {
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

// validateSourceRepair gates learned plans for objects with observed drift. Source
// values come from the accepted manifest, never from historical/event display text.
// Other repairs (including independently justified source edits) remain outside
// this learned-plan boundary.
func (e PlanExecutor) validateSourceRepair(ctx context.Context, incident IncidentRequest, ref sdk.ObjectRef, original, before, after *unstructured.Unstructured) (bool, error) {
	events := append(append([]sdk.DriftEvent(nil), incident.DriftEvents...), driftEventsForFindings(nil, incident.Findings)...)
	var fields []sdk.DriftField
	for _, event := range events {
		if event.Ref == ref {
			fields = append(fields, event.Fields...)
		}
	}
	if len(fields) == 0 {
		return false, nil
	}
	refuse := func(reason string) (bool, error) {
		return true, fmt.Errorf("learned repair of %s/%s refused: %s; investigate current application behavior and accepted source before repairing", ref.Kind, ref.Name, reason)
	}
	if e.Source == nil || incident.SourceCommit == "" {
		return refuse("accepted source evidence is unavailable")
	}
	if err := e.validateAcceptedRevision(incident); err != nil {
		return true, err
	}
	source, err := e.Source.AcceptedManifest(ctx, incident.SourceCommit, ref)
	if err != nil {
		return refuse(fmt.Sprintf("accepted source evidence: %v", err))
	}
	if source == nil || !samePlanRef(source, ref) {
		return refuse("accepted source object is unavailable or mismatched")
	}
	accepted := flattenDriftObject(source)
	observed := flattenDriftObject(original)
	prior := flattenDriftObject(before)
	next := flattenDriftObject(after)
	paths := map[string]bool{}
	for path := range prior {
		paths[path] = true
	}
	for path := range next {
		paths[path] = true
	}
	ordered := make([]string, 0, len(paths))
	for path := range paths {
		ordered = append(ordered, path)
	}
	sort.Strings(ordered)
	for _, path := range ordered {
		previous, pok := prior[path]
		proposed, nok := next[path]
		if pok == nok && planValuesEqual(previous, proposed) {
			continue
		}
		expected, known := accepted[path]
		initial, existed := observed[path]
		// Absent source fields may be API defaults, not observed source drift.
		// Explicitly observed paths below still fail closed without a source value.
		drifted := known && (!existed || !planValuesEqual(initial, expected))
		// Retain observed paths even when a previous action or external writer
		// already restored them. An aggregate action cannot evade a leaf guard.
		for _, field := range fields {
			if path == field.Path || strings.HasPrefix(path, field.Path+".") || strings.HasPrefix(path, field.Path+"[") {
				drifted = true
			}
		}
		if drifted && (!known || !nok || !planValuesEqual(proposed, expected)) {
			return refuse(fmt.Sprintf("field %s must restore its accepted-source value at %s", path, incident.SourceCommit))
		}
	}
	return true, nil
}

func (e PlanExecutor) validateAcceptedRevision(incident IncidentRequest) error {
	if e.AcceptedRevision == nil || e.AcceptedRevision() != incident.SourceCommit {
		return fmt.Errorf("learned repair refused: accepted revision changed or is unavailable; investigate and recompute the plan against current accepted source")
	}
	return nil
}

func (e PlanExecutor) prepareAction(ctx context.Context, incident IncidentRequest, action program.Action, current *unstructured.Unstructured, safe bool) (prepared *unstructured.Unstructured, prepareErr error) {
	defer func() {
		if prepareErr == nil {
			prepareErr = e.validateCredentialDelta(ctx, current, prepared)
		}
	}()
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
		if safe {
			if err := e.Safety.checkObject(source, a.Ref); err != nil {
				return nil, err
			}
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
		if safe {
			if err := e.Safety.checkObject(previous, a.Ref); err != nil {
				return nil, err
			}
		}
		return sourceFields(next, previous, a.Ref)
	case program.SetField:
		var source map[string]any
		if a.ValueFromSource {
			if e.Source == nil || incident.SourceCommit == "" {
				return nil, errors.New("accepted source is required")
			}
			if err := e.validateAcceptedRevision(incident); err != nil {
				return nil, err
			}
			accepted, err := e.Source.AcceptedManifest(ctx, incident.SourceCommit, a.Ref)
			if err != nil {
				return nil, errors.New("accepted source lookup failed")
			}
			if err := e.Safety.checkObject(accepted, a.Ref); err != nil {
				return nil, err
			}
			source = accepted.Object
		}
		err := program.ApplySetFieldAction(next.Object, a, source)
		if err != nil {
			return nil, err
		}
		if a.Path == "spec.template.spec.serviceAccountName" {
			if err := e.validateServiceAccount(ctx, a.Ref, next); err != nil {
				return nil, err
			}
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
		return e.prepareAction(ctx, incident, *a, current, safe)
	case *program.Rollback:
		if a == nil {
			return nil, errors.New("nil rollback action")
		}
		return e.prepareAction(ctx, incident, *a, current, safe)
	case *program.SetField:
		if a == nil {
			return nil, errors.New("nil SetField action")
		}
		return e.prepareAction(ctx, incident, *a, current, safe)
	case *program.RestartRollout:
		if a == nil {
			return nil, errors.New("nil restart action")
		}
		return e.prepareAction(ctx, incident, *a, current, safe)
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
	clusterDelete := ref.APIVersion == "admissionregistration.k8s.io/v1" && (ref.Kind == "MutatingWebhookConfiguration" || ref.Kind == "ValidatingWebhookConfiguration") && ref.Namespace == ""
	if ref.APIVersion == "" || ref.Kind == "" || (!clusterDelete && ref.Namespace == "") || ref.Name == "" {
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
