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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// PlanAPIClient is the raw API boundary for subresources, server-side apply,
// metrics and operations without an object-document projection.
type PlanAPIClient interface {
	KubernetesRequest(context.Context, program.KubernetesRequest) (json.RawMessage, error)
}

func generalAction(a program.Action) program.Action {
	switch p := a.(type) {
	case *program.PatchObject:
		if p != nil {
			return *p
		}
	case *program.ApplyObject:
		if p != nil {
			return *p
		}
	case *program.KubernetesRequest:
		if p != nil {
			return *p
		}
	case *program.WaitFor:
		if p != nil {
			return *p
		}
	case *program.DeleteObject:
		if p != nil {
			return *p
		}
	}
	return concreteExternalAction(a)
}
func isGeneralAction(a program.Action) bool {
	switch generalAction(a).(type) {
	case program.PatchObject, program.ApplyObject, program.KubernetesRequest, program.WaitFor, program.DeleteObject:
		return true
	}
	return isExternalAction(a)
}
func hasGeneralActions(p program.Plan) bool {
	for _, a := range p.Actions {
		if isGeneralAction(a) {
			return true
		}
	}
	return false
}

// applyGeneralPlan shares the normal result/compensation contract. There is no
// source-value or menu-specific safety policy: Kubernetes enforces RBAC.
func (e PlanExecutor) applyGeneralPlan(ctx context.Context, incident IncidentRequest, plan program.Plan, dry bool) (result PlanApplicationResult, err error) {
	start := time.Now()
	result = PlanApplicationResult{Plan: plan, DryRun: dry, ActionReports: newPlanReports(plan)}
	complete := false
	defer func() {
		result.Duration = time.Since(start)
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
		for i := range result.ActionReports {
			if err != nil && result.ActionReports[i].Status == "prepared" {
				result.ActionReports[i].Status = "refused"
			}
		}
	}()
	if err = plan.Validate(); err != nil {
		return result, err
	}
	if e.Client == nil {
		return result, fmt.Errorf("plan object client is required")
	}
	if !dry && (e.Verifier == nil || e.Rollouts == nil) {
		return result, fmt.Errorf("rollout waiter and health verifier are required")
	}
	// Validate every action and transport before dispatching the first mutation.
	previews, prepareErr := e.preflightGeneral(ctx, incident, plan)
	if prepareErr != nil {
		return result, prepareErr
	}
	if dry {
		for _, step := range previews {
			result.Patches = append(result.Patches, step.Patches...)
			result.PreImages = append(result.PreImages, step.PreImages...)
		}
		return result, nil
	}
	var refs []sdk.ObjectRef
	for i, a := range plan.Actions {
		step, stepErr := e.generalStep(ctx, incident, generalAction(a), false)
		result.Patches = append(result.Patches, step.Patches...)
		result.PreImages = append(result.PreImages, step.PreImages...)
		result.ObjectEffects = append(result.ObjectEffects, step.ObjectEffects...)
		if len(step.ActionReports) > 0 {
			report := step.ActionReports[0]
			report.ActionIndex = i
			result.ActionReports[i] = report
		}
		if step.Applied {
			result.Applied = true
			result.ActionsApplied = append(result.ActionsApplied, a)
		}
		if stepErr != nil {
			return result, stepErr
		}
		if ref, refErr := planActionRef(a); refErr == nil && ref.Kind != "" {
			refs = append(refs, ref)
		}
	}
	complete = true
	if pending, waitErr := e.waitForRollouts(ctx, incident, refs); waitErr != nil {
		result.RolloutPending = pending
		return result, waitErr
	}
	result.VerificationPassed, err = e.Verifier.Verify(ctx)
	result.VerificationCompleted = true
	return result, err
}
func (e PlanExecutor) generalStep(ctx context.Context, incident IncidentRequest, action program.Action, dry bool) (r PlanApplicationResult, err error) {
	r.Plan = program.Plan{Actions: []program.Action{action}}
	r.DryRun = dry
	r.ActionReports = newPlanReports(r.Plan)
	if !isGeneralAction(action) {
		return e.Apply(ctx, incident, r.Plan, dry)
	}
	if isExternalAction(action) {
		return e.externalStep(ctx, action, dry)
	}
	switch a := action.(type) {
	case program.KubernetesRequest:
		client, ok := e.Client.(PlanAPIClient)
		if !ok {
			return r, fmt.Errorf("Kubernetes API transport unavailable")
		}
		if dry {
			return r, nil
		}
		// A lost write response is an irreversible uncertainty, never a retry grant.
		mutating := a.Method != "GET" && a.Method != "HEAD"
		if mutating {
			r.Applied = true
			r.ActionsApplied = []program.Action{action}
			r.Patches = []PlanPatch{{Ref: a.Ref, Operation: "kubernetes_request", Irreversible: true}}
			r.ActionReports[0].Status = "unknown"
		}
		effect := program.KubernetesEffect{Request: a}
		readImage := func() (map[string]any, bool) {
			if program.ValidateGeneralRef(a.Ref) != nil {
				return nil, false
			}
			object, readErr := e.Client.Get(ctx, a.Ref)
			if apierrors.IsNotFound(readErr) {
				return nil, true
			}
			if readErr != nil || object == nil {
				return nil, false
			}
			return object.DeepCopy().Object, true
		}
		if mutating {
			effect.Before, effect.BeforeKnown = readImage()
		}
		effect.Response, err = client.KubernetesRequest(ctx, a)
		if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) || apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			r.Applied = false
			r.ActionsApplied = nil
			r.Patches = nil
			r.ActionReports[0].Status = "refused"
			return r, err
		}
		if err == nil {
			r.ActionReports[0].Status = "applied"
			if mutating {
				effect.After, effect.AfterKnown = readImage()
				r.ActionReports[0].KubernetesEffect = &effect
				patch := &r.Patches[0]
				if effect.Before != nil {
					patch.Before = &unstructured.Unstructured{Object: effect.Before}
				}
				if effect.After != nil {
					patch.After = &unstructured.Unstructured{Object: effect.After}
				}
				if effect.AfterKnown && effect.After == nil && effect.Before != nil {
					patch.Operation = "delete"
				}
				if effect.BeforeKnown && effect.Before == nil && effect.After != nil {
					patch.Operation = "create"
				}
				if e.RecordPrimitive != nil {
					args, _ := json.Marshal([]any{a})
					value, _ := json.Marshal(effect)
					if recordErr := e.RecordPrimitive(sdktest.PrimitiveCall{Method: "KubernetesEffect", Arguments: args, Result: value}); recordErr != nil {
						r.ActionReports[0].RecordingError = "Kubernetes effect capture unavailable"
					}
				}
			}
		}
		return r, err
	case program.WaitFor:
		if dry {
			return r, nil
		}
		return r, e.waitForField(ctx, a)
	}
	ref, refErr := planActionRef(action)
	if refErr != nil {
		return r, refErr
	}
	if err = program.ValidateGeneralRef(ref); err != nil {
		return r, err
	}
	before, getErr := e.Client.Get(ctx, ref)
	_, applying := action.(program.ApplyObject)
	missing := apierrors.IsNotFound(getErr)
	if getErr != nil && !(missing && applying) {
		return r, getErr
	}
	if !missing && (before == nil || !samePlanRef(before, ref) || before.GetUID() == "" || before.GetResourceVersion() == "") {
		return r, fmt.Errorf("object identity/version is unavailable")
	}
	patch := PlanPatch{Authorization: "general", Ref: ref, Before: before.DeepCopy(), Structured: true}
	switch a := action.(type) {
	case program.PatchObject:
		var fields map[string]any
		fields, err = program.ProjectPatch(before.Object, a)
		patch.After = &unstructured.Unstructured{Object: fields}
	case program.ApplyObject:
		if a.Mode == "create" && !missing {
			return r, apierrors.NewAlreadyExists(schema.GroupResource{Resource: ref.Kind}, ref.Name)
		}
		var fields map[string]any
		raw, _ := json.Marshal(a.Object)
		if !missing && a.Mode != "replace" {
			old, _ := json.Marshal(before.Object)
			raw, err = jsonpatch.MergePatch(old, raw)
		}
		if err == nil {
			err = utiljson.Unmarshal(raw, &fields)
		}
		patch.After = &unstructured.Unstructured{Object: fields}
		if missing {
			patch.Operation = "create"
		} else {
			patch.After.SetUID(before.GetUID())
			patch.After.SetResourceVersion(before.GetResourceVersion())
		}
	case program.DeleteObject:
		patch.Operation = "delete"
		patch.Irreversible = true
	default:
		return r, fmt.Errorf("unsupported general action %T", action)
	}
	if err != nil {
		return r, err
	}
	if patch.Operation != "delete" && patch.Operation != "create" {
		old, _ := json.Marshal(before.Object)
		next, _ := json.Marshal(patch.After.Object)
		patch.Patch, err = createConditionalObjectPatch(old, next, before)
		if err != nil {
			return r, err
		}
	}
	if patch.Operation != "" {
		if _, ok := e.Client.(PlanLifecycleClient); !ok {
			return r, fmt.Errorf("object lifecycle transport unavailable")
		}
	}
	r.PreImages = []PlanPreImage{{Ref: ref, Object: before.DeepCopy()}}
	r.Patches = []PlanPatch{patch}
	if dry {
		return r, nil
	}
	// Mark dispatch before crossing the write boundary. Transport errors cannot
	// turn a possible write into a refused/non-applied action.
	r.ActionReports[0].Status = "unknown"
	switch patch.Operation {
	case "create":
		var created *unstructured.Unstructured
		created, err = e.Client.(PlanLifecycleClient).Create(ctx, ref, patch.After.DeepCopy())
		if err == nil && (created == nil || created.GetUID() == "") {
			err = fmt.Errorf("create UID receipt unavailable")
		}
		if created != nil {
			r.Patches[0].After = created.DeepCopy()
		}
	case "delete":
		if e.DeleteObject != nil {
			err = e.DeleteObject(ctx, ref, before.GetUID(), before.GetResourceVersion())
		} else {
			err = e.Client.(PlanLifecycleClient).Delete(ctx, ref, before.GetUID(), before.GetResourceVersion())
		}
	default:
		err = e.Client.Patch(ctx, ref, types.MergePatchType, patch.Patch)
	}
	if err != nil {
		if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) || apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) {
			r.ActionReports[0].Status = "refused"
			return r, err
		}
		// Persist uncertainty without using an unconfirmed create as delete authority.
		r.Applied = true
		r.ActionsApplied = []program.Action{action}
		return r, fmt.Errorf("Kubernetes write outcome unknown: %w", err)
	}
	r.Applied = true
	r.ActionsApplied = []program.Action{action}
	if patch.Operation != "" {
		err = e.observeObjectEffect(ctx, r.Patches[0])
		effect := PlanObjectEffect{Operation: patch.Operation, Ref: ref, Confirmed: err == nil, State: "applied", Irreversible: patch.Irreversible}
		if before != nil {
			effect.UID = string(before.GetUID())
		}
		if patch.Operation == "delete" {
			effect.DeletedPreImage = before.DeepCopy()
		}
		if patch.Operation == "create" {
			effect.UID = string(r.Patches[0].After.GetUID())
		}
		r.ObjectEffects = []PlanObjectEffect{effect}
	} else {
		err = e.verifyStructuredWrites(ctx, r)
	}
	if err == nil {
		r.ActionReports[0].Status = "applied"
		r.Patches[0].DirectReadConfirmed = true
	}
	return r, err
}
func (e PlanExecutor) waitForField(ctx context.Context, w program.WaitFor) error {
	timeout := w.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	interval := w.Interval
	if interval == 0 {
		interval = 250 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		o, err := e.Client.Get(ctx, w.Ref)
		if err != nil {
			return err
		}
		value, err := program.FieldValue(o.Object, w.Path)
		if err == nil && planValuesEqual(value, w.Equals) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait condition unobserved: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func planPatchFields(p PlanPatch) []string {
	if p.Authorization != "general" {
		return []string{"spec", "data", "binaryData"}
	}
	keys := map[string]bool{}
	for _, o := range []*unstructured.Unstructured{p.Before, p.After} {
		if o != nil {
			for k := range o.Object {
				if k != "apiVersion" && k != "kind" {
					keys[k] = true
				}
			}
		}
	}
	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	return out
}

// previewPlanClient projects earlier steps during whole-batch preflight. It
// never writes to the real API; later actions can reference newly created data.
type previewPlanClient struct {
	PlanObjectClient
	objects map[sdk.ObjectRef]*unstructured.Unstructured
}

func (p *previewPlanClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if o, ok := p.objects[ref]; ok {
		if o == nil {
			return nil, apierrors.NewNotFound(schema.GroupResource{Resource: ref.Kind}, ref.Name)
		}
		return o.DeepCopy(), nil
	}
	return p.PlanObjectClient.Get(ctx, ref)
}
func (p *previewPlanClient) Create(context.Context, sdk.ObjectRef, *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	return nil, fmt.Errorf("preflight cannot create")
}
func (p *previewPlanClient) Delete(context.Context, sdk.ObjectRef, types.UID, string) error {
	return fmt.Errorf("preflight cannot delete")
}
func (p *previewPlanClient) RunOperation(context.Context, program.ExternalOperation) (int32, error) {
	return 0, fmt.Errorf("preflight cannot execute")
}
func (p *previewPlanClient) KubernetesRequest(context.Context, program.KubernetesRequest) (json.RawMessage, error) {
	return nil, fmt.Errorf("preflight cannot dispatch")
}
func (p *previewPlanClient) project(result PlanApplicationResult) {
	for _, patch := range result.Patches {
		after := patch.After.DeepCopy()
		if after != nil && patch.Operation == "create" {
			after.SetUID("preflight-created")
			after.SetResourceVersion("preflight")
		}
		p.objects[patch.Ref] = after
	}
}
func (e PlanExecutor) preflightGeneral(ctx context.Context, incident IncidentRequest, plan program.Plan) ([]PlanApplicationResult, error) {
	preview := &previewPlanClient{PlanObjectClient: e.Client, objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}
	projected := e
	projected.Client = preview
	var steps []PlanApplicationResult
	for _, original := range plan.Actions {
		a := generalAction(original)
		switch a.(type) {
		case program.ApplyObject, program.DeleteObject, program.RunJob:
			if _, ok := e.Client.(PlanLifecycleClient); !ok {
				return nil, fmt.Errorf("object lifecycle transport unavailable")
			}
		case program.EnsureExec, program.EnsureHTTP:
			if _, ok := e.Client.(PlanOperationClient); !ok {
				return nil, fmt.Errorf("external operation transport unavailable")
			}
		case program.KubernetesRequest:
			if _, ok := e.Client.(PlanAPIClient); !ok {
				return nil, fmt.Errorf("Kubernetes API transport unavailable")
			}
		}
		var step PlanApplicationResult
		var err error
		if isGeneralAction(a) {
			step, err = projected.generalStep(ctx, incident, a, true)
		} else {
			step, err = e.generalStep(ctx, incident, a, true)
		}
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
		preview.project(step)
	}
	return steps, nil
}
