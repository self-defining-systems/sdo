package sim

import (
	"context"
	"encoding/json"
	"errors"
	jsonpatch "github.com/evanphx/json-patch"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sdo.dev/operator/sdk"
	"strconv"
)

// serverClient mimics the API server: Deployment defaulting after every
// write, a controller status/annotation update that bumps resourceVersion, and
// optimistic concurrency on conditional patches.
type serverClient struct {
	*objectClient
	version int
	// raceBeforePatch controller updates land between the caller's read and its next patch.
	raceBeforePatch int
}

// settle applies API-server normalization and defaulting and a controller-managed status bump.
func (c *serverClient) settle(object *unstructured.Unstructured) {
	(&apiServer{}).defaults(kindOf(object), object)
	c.bump(object)
}

func kindOf(o *unstructured.Unstructured) kindInfo {
	for _, k := range servedKinds {
		if k.kind == o.GetKind() && k.groupVersion() == o.GetAPIVersion() {
			return k
		}
	}
	return kindInfo{kind: o.GetKind()}
}

func (c *serverClient) bump(object *unstructured.Unstructured) {
	c.version++
	object.SetResourceVersion(strconv.Itoa(c.version))
	// The deployment controller, not the API server, stamps revisions and
	// observes generations; the simulation models that for Deployments only.
	if object.GetKind() != "Deployment" {
		return
	}
	annotations := object.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations["deployment.kubernetes.io/revision"] = strconv.Itoa(c.version)
	object.SetAnnotations(annotations)
	_ = unstructured.SetNestedField(object.Object, int64(c.version), "status", "observedGeneration")
}

func (c *serverClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if c.raceBeforePatch > 0 {
		c.raceBeforePatch--
		c.bump(c.objects[ref])
	}
	var document map[string]any
	if err := json.Unmarshal(patch, &document); err != nil {
		return err
	}
	if metadata, ok := document["metadata"].(map[string]any); ok {
		current := c.objects[ref]
		// A uid that is not the object's is an immutable-field change (invalid); a
		// stale resourceVersion is a conflict.
		if uid, _ := metadata["uid"].(string); uid != "" && uid != string(current.GetUID()) {
			return apierrors.NewInvalid(current.GroupVersionKind().GroupKind(), ref.Name, field.ErrorList{field.Invalid(field.NewPath("metadata", "uid"), uid, "field is immutable")})
		}
		if rv, ok := metadata["resourceVersion"]; ok && rv != current.GetResourceVersion() {
			return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, ref.Name, errors.New("the object has been modified"))
		}
	}
	if err := c.objectClient.Patch(ctx, ref, kind, patch); err != nil {
		return err
	}
	object := c.objects[ref]
	object.SetUID(c.objects[ref].GetUID())
	c.settle(object)
	return nil
}

// DryRunPatch returns what Patch would store, without persisting it.
func (c *serverClient) DryRunPatch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) (*unstructured.Unstructured, error) {
	scratch := &serverClient{objectClient: &objectClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: c.objects[ref].DeepCopy()}}, version: c.version}
	err := scratch.Patch(ctx, ref, kind, patch)
	if err != nil {
		return nil, err
	}
	return scratch.objects[ref], nil
}

type objectClient struct {
	objects map[sdk.ObjectRef]*unstructured.Unstructured
	patches []json.RawMessage
}

func (c *objectClient) Get(_ context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	object := c.objects[ref]
	if object == nil {
		return nil, errors.New("missing object")
	}
	return object.DeepCopy(), nil
}

func (c *objectClient) Patch(_ context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if kind != types.MergePatchType {
		return errors.New("wrong patch type")
	}
	before, _ := json.Marshal(c.objects[ref].Object)
	merged, err := jsonpatch.MergePatch(before, patch)
	if err != nil {
		return err
	}
	object := new(unstructured.Unstructured)
	if err := json.Unmarshal(merged, &object.Object); err != nil {
		return err
	}
	c.objects[ref] = object
	c.patches = append(c.patches, append([]byte(nil), patch...))
	return nil
}
