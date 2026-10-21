package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
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
	Ref   sdk.ObjectRef
	Patch json.RawMessage
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
	Now      func() time.Time
}

func (e PlanExecutor) Apply(ctx context.Context, incident IncidentRequest, plan program.Plan, dryRun bool) (result PlanApplicationResult, err error) {
	started := time.Now()
	result = PlanApplicationResult{Plan: plan, DryRun: dryRun}
	defer func() { result.Duration = time.Since(started) }()
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
		result.Patches = append(result.Patches, PlanPatch{Ref: ref, Patch: patch})
		current[ref] = next
	}
	if dryRun {
		return result, nil
	}
	for index, patch := range result.Patches {
		if err := e.Client.Patch(ctx, patch.Ref, types.MergePatchType, patch.Patch); err != nil {
			return result, fmt.Errorf("apply action %d to %s/%s: %w", index, patch.Ref.Kind, patch.Ref.Name, err)
		}
		result.ActionsApplied = append(result.ActionsApplied, plan.Actions[index])
	}
	result.Applied = true
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

// Rollback restores each object touched by an applied plan to its pre-image.
func (e PlanExecutor) Rollback(ctx context.Context, application PlanApplicationResult) error {
	if e.Client == nil {
		return errors.New("plan object client is required")
	}
	seen := make(map[sdk.ObjectRef]bool)
	for index := len(application.ActionsApplied) - 1; index >= 0; index-- {
		ref, err := planActionRef(application.ActionsApplied[index])
		if err != nil {
			return err
		}
		if seen[ref] {
			continue
		}
		seen[ref] = true
		var before *unstructured.Unstructured
		for _, preImage := range application.PreImages {
			if preImage.Ref == ref {
				before = preImage.Object
				break
			}
		}
		if before == nil {
			return fmt.Errorf("missing pre-image of %s/%s", ref.Kind, ref.Name)
		}
		current, err := e.Client.Get(ctx, ref)
		if err != nil {
			return err
		}
		toObject := current.DeepCopy()
		for _, field := range []string{"spec", "data", "binaryData"} {
			if value, ok := before.Object[field]; ok {
				toObject.Object[field] = runtime.DeepCopyJSONValue(value)
			} else {
				delete(toObject.Object, field)
			}
		}
		from, err := json.Marshal(current.Object)
		if err != nil {
			return err
		}
		to, err := json.Marshal(toObject.Object)
		if err != nil {
			return err
		}
		patch, err := jsonpatch.CreateMergePatch(from, to)
		if err != nil {
			return err
		}
		if err := e.Client.Patch(ctx, ref, types.MergePatchType, patch); err != nil {
			return fmt.Errorf("restore %s/%s: %w", ref.Kind, ref.Name, err)
		}
	}
	return nil
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
		if err := program.ApplySetField(next.Object, a.Path, a.Value); err != nil {
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
