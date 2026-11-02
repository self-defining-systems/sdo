package runtime

import (
	"context"
	"encoding/json"
	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk/program"
	"strings"
)

// ErrKubernetesAPITransportUnavailable means no request crossed the API boundary.
var ErrKubernetesAPITransportUnavailable = kubereads.ErrAPITransportUnavailable

func (c TypedPlanClient) KubernetesRequest(ctx context.Context, r program.KubernetesRequest) (json.RawMessage, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	observation := program.ValidateKubernetesRead(r) == nil
	if err := c.checkPlanNamespace(r.Ref.Namespace); !observation && err != nil {
		return nil, err
	}
	parts := strings.Split(strings.TrimPrefix(r.Path, "/"), "/")
	index := 2
	if parts[0] == "apis" {
		index = 3
	}
	if len(parts) > index+1 && parts[index] == "namespaces" {
		if err := c.checkPlanNamespace(parts[index+1]); !observation && err != nil {
			return nil, err
		}
	}
	doer, base, err := kubereads.APITransport(c.Client, c.ExecConfig, r)
	if err != nil {
		return nil, err
	}
	return kubereads.APIRequest(ctx, doer, base, r)
}
