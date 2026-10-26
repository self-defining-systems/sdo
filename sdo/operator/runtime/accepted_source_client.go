package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/sdk"
)

// errSecretDataUnreadable keeps Secret values out of every accepted-source
// read and apply: their payload is never fetched for comparison or adoption.
var errSecretDataUnreadable = errors.New("accepted-source access never reads Secret data")

// acceptedResource is an accepted manifest's API location, resolved through
// discovery so every kind the server serves takes the same path.
type acceptedResource struct {
	client     rest.Interface
	path       string
	namespaced bool
}

func isSecretRef(ref sdk.ObjectRef) bool {
	return ref.APIVersion == "v1" && ref.Kind == "Secret"
}

func resolveAcceptedResource(client kubernetes.Interface, ref sdk.ObjectRef) (acceptedResource, error) {
	if isSecretRef(ref) {
		return acceptedResource{}, errSecretDataUnreadable
	}
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil || gv.Version == "" || ref.Kind == "" || ref.Name == "" {
		return acceptedResource{}, fmt.Errorf("invalid accepted object reference %s/%s %s", ref.APIVersion, ref.Kind, ref.Name)
	}
	rc := client.Discovery().RESTClient()
	if rc == nil {
		return acceptedResource{}, fmt.Errorf("accepted-source access requires an API server REST client")
	}
	resources, err := client.Discovery().ServerResourcesForGroupVersion(ref.APIVersion)
	if err != nil {
		return acceptedResource{}, fmt.Errorf("discover %s resources: %w", ref.APIVersion, err)
	}
	for _, resource := range resources.APIResources {
		if resource.Kind != ref.Kind || strings.Contains(resource.Name, "/") {
			continue
		}
		path := "/apis/" + gv.Group + "/" + gv.Version
		if gv.Group == "" {
			path = "/api/" + gv.Version
		}
		if resource.Namespaced {
			if ref.Namespace == "" {
				return acceptedResource{}, fmt.Errorf("namespaced %s/%s %s has no namespace", ref.APIVersion, ref.Kind, ref.Name)
			}
			path += "/namespaces/" + ref.Namespace
		}
		return acceptedResource{client: rc, path: path + "/" + resource.Name + "/" + ref.Name, namespaced: resource.Namespaced}, nil
	}
	return acceptedResource{}, fmt.Errorf("API server does not serve %s/%s", ref.APIVersion, ref.Kind)
}

func decodeAcceptedObject(raw []byte, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	content := map[string]any{}
	if err := json.Unmarshal(raw, &content); err != nil {
		return nil, fmt.Errorf("decode %s/%s %s: %w", ref.APIVersion, ref.Kind, ref.Name, err)
	}
	result := &unstructured.Unstructured{Object: content}
	result.SetAPIVersion(ref.APIVersion)
	result.SetKind(ref.Kind)
	return result, nil
}

func (r acceptedResource) get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	raw, err := r.client.Get().AbsPath(r.path).Do(ctx).Raw()
	if err != nil {
		return nil, err
	}
	return decodeAcceptedObject(raw, ref)
}

func (r acceptedResource) apply(ctx context.Context, ref sdk.ObjectRef, source *unstructured.Unstructured, dryRun bool) (*unstructured.Unstructured, error) {
	patch, err := json.Marshal(source.Object)
	if err != nil {
		return nil, err
	}
	request := r.client.Patch(types.ApplyPatchType).AbsPath(r.path).Param("fieldManager", "sdo-deployer").Param("force", "true").Body(patch)
	if dryRun {
		request = request.Param("dryRun", metav1.DryRunAll)
	}
	raw, err := request.Do(ctx).Raw()
	if err != nil {
		return nil, err
	}
	return decodeAcceptedObject(raw, ref)
}
