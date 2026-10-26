package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// A supplied typed Kubernetes client may have no generic capture companion.
// Resolve unsupported objects through discovery instead of a per-kind switch.
func (c TypedPlanClient) genericObjectRequest(ctx context.Context, ref sdk.ObjectRef, method, contentType string, body []byte, collection bool) (json.RawMessage, error) {
	if err := program.ValidateGeneralRef(ref); err != nil {
		return nil, err
	}
	if c.Client == nil {
		return nil, fmt.Errorf("Kubernetes discovery transport unavailable")
	}
	resources, err := c.Client.Discovery().ServerResourcesForGroupVersion(ref.APIVersion)
	if err != nil {
		return nil, err
	}
	for _, resource := range resources.APIResources {
		if resource.Kind != ref.Kind || resource.Name == "secrets" || strings.Contains(resource.Name, "/") {
			continue
		}
		if resource.Namespaced != (ref.Namespace != "") {
			return nil, fmt.Errorf("object scope differs from discovery")
		}
		path := "/apis/" + ref.APIVersion
		if !strings.Contains(ref.APIVersion, "/") {
			path = "/api/" + ref.APIVersion
		}
		if resource.Namespaced {
			path += "/namespaces/" + url.PathEscape(ref.Namespace)
		}
		path += "/" + resource.Name
		if !collection {
			path += "/" + url.PathEscape(ref.Name)
		}
		return c.KubernetesRequest(ctx, program.KubernetesRequest{Ref: ref, Method: method, Path: path, ContentType: contentType, Body: body, Reason: "discovered object operation"})
	}
	return nil, fmt.Errorf("object kind is not discoverable: %s/%s", ref.APIVersion, ref.Kind)
}

func (c TypedPlanClient) genericObjectGet(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	raw, err := c.genericObjectRequest(ctx, ref, "GET", "", nil, false)
	if err != nil {
		return nil, err
	}
	object := &unstructured.Unstructured{}
	if err := object.UnmarshalJSON(raw); err != nil {
		return nil, err
	}
	if !samePlanRef(object, ref) {
		return nil, fmt.Errorf("generic read returned a different object")
	}
	return object, nil
}

// These are transport capabilities, independent of the API server's RBAC.
func (c TypedPlanClient) unavailableMenuActions() map[string]bool {
	unavailable := map[string]bool{}
	if c.ScriptImage == "" {
		unavailable["RunScript"] = true
	}
	if !c.hasAPITransport() {
		unavailable["KubernetesRequest"] = true
	}
	if c.ExecConfig == nil {
		for _, action := range []string{"Exec", "EnsureExec", "EnsureHTTP", "EnsureDatabaseUser"} {
			unavailable[action] = true
		}
	}
	return unavailable
}
func (c TypedPlanClient) hasAPITransport() bool {
	if c.ExecConfig != nil {
		return true
	}
	if c.Client == nil {
		return false
	}
	transport, ok := c.Client.Discovery().RESTClient().(*rest.RESTClient)
	return ok && transport != nil && transport.Client != nil
}
