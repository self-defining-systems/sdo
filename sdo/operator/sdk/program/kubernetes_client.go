package program

import (
	"context"
	"fmt"
	"net/url"

	"sdo.dev/operator/sdk/kubeclient"
)

// Kubernetes constructs client-go-shaped read clients for this observation view.
// Build it once per Diagnose/mitigation or detector invocation. All calls use
// KubernetesRead, including its cycle cache, failure checking and phase replay.
// Built-in fields retain Kubernetes types; Dynamic and RESTMapper cover CRDs.
func Kubernetes(view ReadView) (*kubeclient.ReadClient, error) {
	if view == nil {
		return nil, fmt.Errorf("Kubernetes observation view required")
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
