package runtime

import (
	"fmt"
	"k8s.io/client-go/discovery"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
)

// Named DELETE only: a collection or subresource response cannot prove that a
// particular whole object disappeared. Discovery supplies the canonical kind.
func (e PlanExecutor) resolveDeleteTarget(request program.KubernetesRequest) (sdk.ObjectRef, error) {
	if err := request.Validate(); err != nil {
		return sdk.ObjectRef{}, err
	}
	resolver, ok := e.Client.(interface {
		ResolveDeleteTarget(program.KubernetesRequest) (sdk.ObjectRef, error)
	})
	if !ok {
		return sdk.ObjectRef{}, fmt.Errorf("named DELETE target discovery unavailable")
	}
	ref, err := resolver.ResolveDeleteTarget(request)
	if err != nil {
		return ref, err
	}
	if request.Ref != (sdk.ObjectRef{}) && request.Ref != ref {
		return ref, fmt.Errorf("DELETE Ref conflicts with API path target")
	}
	return ref, nil
}
func (c TypedPlanClient) ResolveDeleteTarget(request program.KubernetesRequest) (sdk.ObjectRef, error) {
	if c.Client == nil {
		return sdk.ObjectRef{}, ErrKubernetesAPITransportUnavailable
	}
	return resolveDeletePath(c.Client.Discovery(), request)
}
func (c GenericPlanClient) ResolveDeleteTarget(request program.KubernetesRequest) (sdk.ObjectRef, error) {
	if c.Objects != nil {
		return resolveDeletePath(c.Objects.Discovery, request)
	}
	return c.TypedPlanClient.ResolveDeleteTarget(request)
}
func resolveDeletePath(discover discovery.DiscoveryInterface, request program.KubernetesRequest) (sdk.ObjectRef, error) {
	if err := request.Validate(); err != nil {
		return sdk.ObjectRef{}, err
	}
	parts := strings.Split(strings.TrimPrefix(request.Path, "/"), "/")
	index := 2
	version := parts[1]
	if parts[0] == "apis" {
		index = 3
		version = parts[1] + "/" + parts[2]
	}
	namespace := ""
	if len(parts) > index+2 && parts[index] == "namespaces" {
		namespace = parts[index+1]
		index += 2
	}
	if len(parts) != index+2 {
		return sdk.ObjectRef{}, fmt.Errorf("DELETE requires a named whole-object API path")
	}
	if discover == nil {
		return sdk.ObjectRef{}, fmt.Errorf("DELETE target discovery unavailable")
	}
	list, err := discover.ServerResourcesForGroupVersion(version)
	if err != nil {
		return sdk.ObjectRef{}, fmt.Errorf("discover DELETE target: %w", err)
	}
	for _, resource := range list.APIResources {
		if resource.Name != parts[index] {
			continue
		}
		if resource.Namespaced != (namespace != "") {
			return sdk.ObjectRef{}, fmt.Errorf("DELETE target namespace disagrees with discovery")
		}
		ref := sdk.ObjectRef{APIVersion: version, Kind: resource.Kind, Namespace: namespace, Name: parts[index+1]}
		return ref, program.ValidateGeneralRef(ref)
	}
	return sdk.ObjectRef{}, fmt.Errorf("DELETE target resource is unavailable in discovery")
}
