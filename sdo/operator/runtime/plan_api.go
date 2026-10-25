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
	var client *http.Client
	var base *url.URL
	var err error
	if c.ExecConfig != nil {
		config := rest.CopyConfig(c.ExecConfig)
		client, err = rest.HTTPClientFor(config)
		if err != nil {
			return nil, fmt.Errorf("Kubernetes API transport unavailable")
		}
		base, err = url.Parse(config.Host)
		if err != nil {
			return nil, fmt.Errorf("invalid API host")
		}
		base.Path += r.Path
	} else if c.Client != nil {
		transport, ok := c.Client.Discovery().RESTClient().(*rest.RESTClient)
		if !ok || transport == nil || transport.Client == nil {
			return nil, fmt.Errorf("Kubernetes API transport unavailable")
		}
		client = transport.Client
		base = transport.Verb(r.Method).AbsPath(r.Path).URL()
	} else {
		return nil, fmt.Errorf("Kubernetes API transport unavailable")
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
