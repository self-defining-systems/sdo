package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"net/http"
	"net/url"
	"sdo.dev/operator/sdk/program"
)

func (c TypedPlanClient) KubernetesRequest(ctx context.Context, r program.KubernetesRequest) (json.RawMessage, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	if c.ExecConfig == nil {
		return nil, fmt.Errorf("Kubernetes API transport unavailable")
	}
	config := rest.CopyConfig(c.ExecConfig)
	// Never follow API redirects carrying controller authorization.
	client, err := rest.HTTPClientFor(config)
	if err != nil {
		return nil, fmt.Errorf("Kubernetes API transport unavailable")
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	base, err := url.Parse(config.Host)
	if err != nil {
		return nil, fmt.Errorf("invalid API host")
	}
	base.Path = base.Path + r.Path
	values := url.Values{}
	for k, v := range r.Query {
		values.Set(k, v)
	}
	base.RawQuery = values.Encode()
	request, err := http.NewRequestWithContext(ctx, r.Method, base.String(), bytes.NewReader(r.Body))
	if err != nil {
		return nil, fmt.Errorf("invalid API request")
	}
	if r.ContentType != "" {
		request.Header.Set("Content-Type", r.ContentType)
	}
	request.Header.Set("Accept", "application/json")
	response, err := copyClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("Kubernetes API response unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, apierrors.NewGenericServerResponse(response.StatusCode, r.Method, schema.GroupResource{Resource: "api"}, "", "Kubernetes request failed", 0, false)
	}
	// Token issuance is expressible, but token bytes must stay in a container
	// when consumed. This controller transport never reads its response body.
	if r.CredentialResponse() {
		return json.RawMessage(`{"response_omitted":"credential"}`), nil
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	if err != nil || len(raw) > 1024*1024 {
		return nil, fmt.Errorf("Kubernetes API response unavailable or exceeds limit")
	}
	if len(raw) == 0 {
		return json.RawMessage("null"), nil
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("Kubernetes API response is not JSON")
	}
	return raw, nil
}
