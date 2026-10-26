package program

import (
	"context"
	"fmt"
	"net/url"
	"sdo.dev/operator/sdk"
	"sort"

	"sdo.dev/operator/sdk/kubeclient"
)

// Kubernetes constructs client-go-shaped read clients for this observation view.
// Build it once per Diagnose/mitigation or detector invocation. All calls use
// KubernetesRead, including its cycle cache, failure checking and phase replay.
// Built-in fields retain Kubernetes types; Dynamic and RESTMapper cover CRDs.
// Optional bindings require each role to remain in accepted source. Omit them
// for observations of intentionally source-absent objects and derived resources.
func Kubernetes(view ReadView, sourceBindings ...Bindings) (*kubeclient.ReadClient, error) {
	if view == nil {
		return nil, fmt.Errorf("Kubernetes observation view required")
	}
	if len(sourceBindings) > 1 {
		return nil, fmt.Errorf("one source binding set is required")
	}
	if len(sourceBindings) == 1 {
		roles := make([]string, 0, len(sourceBindings[0].Objects))
		for role := range sourceBindings[0].Objects {
			roles = append(roles, role)
		}
		sort.Strings(roles)
		for _, role := range roles {
			ref := sourceBindings[0].Objects[role]
			diff, err := view.DiffAgainstSource(ref)
			if err != nil {
				return nil, fmt.Errorf("source binding %s unavailable: %w", role, err)
			}
			if diff.SourceAbsent {
				return nil, &StaleBindingError{Role: role, Resource: ref}
			}
		}
	}
	return kubeclient.NewReadClient(func(ctx context.Context, method, path string, query url.Values) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		request := KubernetesRequest{Method: method, Path: path, Query: map[string]string{}}
		for name, values := range query {
			if len(values) != 1 {
				return nil, fmt.Errorf("Kubernetes read query %s requires one value", name)
			}
			request.Query[name] = values[0]
		}
		if err := ValidateKubernetesRead(request); err != nil {
			return nil, err
		}
		return view.KubernetesRead(request)
	})
}

// StaleBindingError identifies a source-grounded role that no longer exists.
// It is not permission to rebind the role to a different live object.
type StaleBindingError struct {
	Role     string
	Resource sdk.ObjectRef
}

func (e *StaleBindingError) Error() string {
	return fmt.Sprintf("source binding %s is stale: %s/%s/%s absent from accepted source", e.Role, e.Resource.Kind, e.Resource.Namespace, e.Resource.Name)
}
