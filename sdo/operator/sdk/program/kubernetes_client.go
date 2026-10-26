package program

import (
	"context"
	"encoding/json"
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
	read, err := kubernetesAccess(view, sourceBindings...)
	if err != nil {
		return nil, err
	}
	return kubeclient.NewReadClient(read)
}

// KubernetesExecutor is provided only during admitted execution. ReadView does
// not implement it: planning and detectors cannot acquire mutation authority.
type KubernetesExecutor interface {
	KubernetesRequest(context.Context, KubernetesRequest) (json.RawMessage, error)
}

// KubernetesForExecution exposes upstream mutations through the owning executor,
// preserving its recording, before-images and undo. It never executes a write
// during construction and never substitutes predicted mutation responses.
func KubernetesForExecution(view ReadView, executor KubernetesExecutor, sourceBindings ...Bindings) (*kubeclient.ExecutionClient, error) {
	if executor == nil {
		return nil, fmt.Errorf("Kubernetes execution capability required")
	}
	read, err := kubernetesAccess(view, sourceBindings...)
	if err != nil {
		return nil, err
	}
	return kubeclient.NewExecutionClient(read, func(ctx context.Context, method, path string, query url.Values, content string, body []byte) ([]byte, error) {
		q := KubernetesRequest{Method: method, Path: path, ContentType: content, Body: body, Reason: "recorded client-go execution", Query: map[string]string{}}
		for k, values := range query {
			if len(values) != 1 {
				return nil, fmt.Errorf("Kubernetes query %s requires one value", k)
			}
			q.Query[k] = values[0]
		}
		return executor.KubernetesRequest(ctx, q)
	})
}
func kubernetesAccess(view ReadView, sourceBindings ...Bindings) (kubeclient.ReadFunc, error) {
	if view == nil {
		return nil, fmt.Errorf("Kubernetes observation view required")
	}
	fail := func(err error) error {
		if checked, ok := view.(*checkedReadView); ok {
			checked.check(err)
		}
		return err
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
				return nil, fail(&StaleBindingError{Role: role, Resource: ref})
			}
		}
	}
	return func(ctx context.Context, method, path string, query url.Values) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, fail(err)
		}
		request := KubernetesRequest{Method: method, Path: path, Query: map[string]string{}}
		for name, values := range query {
			if len(values) != 1 {
				return nil, fail(fmt.Errorf("Kubernetes read query %s requires one value", name))
			}
			request.Query[name] = values[0]
		}
		if err := ValidateKubernetesRead(request); err != nil {
			return nil, fail(err)
		}
		return view.KubernetesRead(request)
	}, nil
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
