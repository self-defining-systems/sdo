package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"net/http"
	"net/url"
	"sdo.dev/operator/sdk/program"
	"strings"
)

// ErrKubernetesAPITransportUnavailable means no request crossed the API boundary.
var ErrKubernetesAPITransportUnavailable = errors.New("Kubernetes API transport unavailable")

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
	var client *http.Client
	var base *url.URL
	var err error
	// Prefer the process-wide client transport; ExecConfig also exists for
	// remotecommand and must not cause a new wrapper/client for each API read.
	if c.Client != nil {
		if transport, ok := c.Client.Discovery().RESTClient().(*rest.RESTClient); ok && transport != nil && transport.Client != nil {
			client = transport.Client
			base = transport.Verb(r.Method).AbsPath(r.Path).URL()
		}
	}
	if client == nil && c.ExecConfig != nil {
		client, err = rest.HTTPClientFor(c.ExecConfig)
		if err != nil {
			return nil, ErrKubernetesAPITransportUnavailable
		}
		base, err = url.Parse(c.ExecConfig.Host)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid API host", ErrKubernetesAPITransportUnavailable)
		}
		base.Path += r.Path
	}
	if client == nil {
		return nil, ErrKubernetesAPITransportUnavailable
	}
	// Reuse the supplied client's authenticated transport, with bounded bodies
	// and redirects disabled just as for an explicitly supplied rest.Config.
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	values := url.Values{}
	for k, v := range r.Query {
		values.Set(k, v)
	}
	base.RawQuery = values.Encode()
	request, err := http.NewRequestWithContext(ctx, r.Method, base.String(), bytes.NewReader(r.Body))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid API request", ErrKubernetesAPITransportUnavailable)
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
	if strings.HasSuffix(r.Path, "/log") || r.Query["watch"] == "true" {
		return json.Marshal(string(raw))
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("Kubernetes API response is not JSON")
	}
	return raw, nil
}
