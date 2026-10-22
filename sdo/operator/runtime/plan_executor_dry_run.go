package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"sdo.dev/operator/sdk"
)

var ErrPlanDryRunUnsupported = errors.New("plan client does not support server-side dry-run")

// planDryRunner returns the object the API server would store for a patch,
// without persisting it.
type planDryRunner interface {
	DryRunPatch(context.Context, sdk.ObjectRef, types.PatchType, []byte) (*unstructured.Unstructured, error)
}

// normalizeAfterImages replaces each patch's after-image with the object the
// API server returns for a dry-run of the same change, so the apply check,
// rollback and pending-write reconcile compare against server-normalized values
// (canonical quantities, dropped empty lists, defaults) rather than a guess at
// them. A client without server dry-run falls back to canonicalizePlanObject.
// totals[i] is the patch from the pre-image to action i's result.
func (e PlanExecutor) normalizeAfterImages(ctx context.Context, patches []PlanPatch, totals [][]byte) error {
	client, ok := e.Client.(planDryRunner)
	for i := range patches {
		var normalized *unstructured.Unstructured
		if ok {
			var err error
			normalized, err = client.DryRunPatch(ctx, patches[i].Ref, types.MergePatchType, totals[i])
			if err != nil && !errors.Is(err, ErrPlanDryRunUnsupported) {
				return fmt.Errorf("server dry-run %s/%s: %w", patches[i].Ref.Kind, patches[i].Ref.Name, err)
			}
		}
		if normalized == nil {
			canonicalizePlanObject(patches[i].After)
			continue
		}
		patches[i].After = normalized
	}
	return nil
}

// projectPlanOwned keeps the shape of what the plan wrote and takes its values
// from the server-normalized object. Keys the plan wrote that the server did not
// store (dropped empty lists) are not owned; keys it added (defaults) are ignored.
func projectPlanOwned(normalized, written any) any {
	switch w := written.(type) {
	case map[string]any:
		n, ok := normalized.(map[string]any)
		if !ok {
			return normalized
		}
		owned := make(map[string]any, len(w))
		for key, value := range w {
			if stored, exists := n[key]; exists {
				owned[key] = projectPlanOwned(stored, value)
			}
		}
		return owned
	case []any:
		n, ok := normalized.([]any)
		if !ok || len(n) != len(w) {
			return normalized
		}
		owned := make([]any, len(w))
		for i := range w {
			owned[i] = projectPlanOwned(n[i], w[i])
		}
		return owned
	default:
		return normalized
	}
}

func (c KubernetesPlanClient) DryRunPatch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) (*unstructured.Unstructured, error) {
	resource, err := c.resource(ref)
	if err != nil {
		return nil, err
	}
	return resource.Patch(ctx, ref.Name, kind, patch, metav1.PatchOptions{DryRun: []string{metav1.DryRunAll}})
}

func (c TypedPlanClient) DryRunPatch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) (*unstructured.Unstructured, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("Kubernetes plan client is required")
	}
	transport := c.Client.CoreV1().RESTClient()
	if ref.APIVersion == "apps/v1" {
		transport = c.Client.AppsV1().RESTClient()
	}
	// Some clients provide only an in-memory object transport and discard all
	// PatchOptions. They cannot perform a server-side dry-run.
	if transport == nil || (reflect.ValueOf(transport).Kind() == reflect.Pointer && reflect.ValueOf(transport).IsNil()) {
		return nil, ErrPlanDryRunUnsupported
	}
	options := metav1.PatchOptions{DryRun: []string{metav1.DryRunAll}}
	var object any
	var err error
	switch ref.APIVersion + "/" + ref.Kind {
	case "v1/Service":
		object, err = c.Client.CoreV1().Services(ref.Namespace).Patch(ctx, ref.Name, kind, patch, options)
	case "v1/ConfigMap":
		object, err = c.Client.CoreV1().ConfigMaps(ref.Namespace).Patch(ctx, ref.Name, kind, patch, options)
	case "apps/v1/Deployment":
		object, err = c.Client.AppsV1().Deployments(ref.Namespace).Patch(ctx, ref.Name, kind, patch, options)
	default:
		return nil, fmt.Errorf("unsupported plan object %s/%s", ref.APIVersion, ref.Kind)
	}
	if err != nil {
		return nil, err
	}
	return planUnstructured(object, ref)
}

func planUnstructured(object any, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	fields, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		return nil, err
	}
	result := &unstructured.Unstructured{Object: fields}
	result.SetAPIVersion(ref.APIVersion)
	result.SetKind(ref.Kind)
	return result, nil
}
