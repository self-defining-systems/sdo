package sim

import (
	"context"
	"encoding/json"
	"errors"
	jsonpatch "github.com/evanphx/json-patch"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
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

// canonicalizeSpec round-trips a Deployment spec through the typed API object,
// as the API server's codec does: resource quantities become canonical ("1000m"
// is stored as "1", "0.5Gi" as "512Mi") and empty optional lists are dropped.
func canonicalizeSpec(object *unstructured.Unstructured) {
	spec, ok := object.Object["spec"].(map[string]any)
	if !ok || object.GetKind() != "Deployment" {
		return
	}
	var typed appsv1.DeploymentSpec
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(spec, &typed); err != nil {
		panic(err)
	}
	canonical, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&typed)
	if err != nil {
		panic(err)
	}
	object.Object["spec"] = canonical
}

// settle applies API-server normalization and defaulting and a controller-managed status bump.
func (c *serverClient) settle(object *unstructured.Unstructured) {
	canonicalizeSpec(object)
	spec, _ := object.Object["spec"].(map[string]any)
	if spec != nil && object.GetKind() == "Deployment" {
		if _, ok := spec["progressDeadlineSeconds"]; !ok {
			spec["progressDeadlineSeconds"] = float64(600)
		}
		if _, ok := spec["revisionHistoryLimit"]; !ok {
			spec["revisionHistoryLimit"] = float64(10)
		}
		containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
		for _, item := range containers {
			if container, ok := item.(map[string]any); ok {
				if _, ok := container["terminationMessagePath"]; !ok {
					container["terminationMessagePath"] = "/dev/termination-log"
				}
				if probe, ok := container["readinessProbe"].(map[string]any); ok {
					for key, value := range map[string]any{"periodSeconds": float64(10), "timeoutSeconds": float64(1), "successThreshold": float64(1), "failureThreshold": float64(3)} {
						if _, ok := probe[key]; !ok {
							probe[key] = value
						}
					}
					if get, ok := probe["httpGet"].(map[string]any); ok {
						if _, ok := get["scheme"]; !ok {
							get["scheme"] = "HTTP"
						}
					}
				}
			}
		}
		if len(containers) > 0 {
			_ = unstructured.SetNestedSlice(object.Object, containers, "spec", "template", "spec", "containers")
		}
	}
	c.bump(object)
}

func (c *serverClient) bump(object *unstructured.Unstructured) {
	c.version++
	object.SetResourceVersion(strconv.Itoa(c.version))
	object.SetAnnotations(map[string]string{"deployment.kubernetes.io/revision": strconv.Itoa(c.version)})
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
		// The API server reports a failed uid precondition as a conflict too.
		if metadata["uid"] != string(current.GetUID()) || metadata["resourceVersion"] != current.GetResourceVersion() {
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
