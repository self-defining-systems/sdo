package kubereads

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk/program"
)

// ErrAPITransportUnavailable means no request crossed the API boundary.
var ErrAPITransportUnavailable = errors.New("Kubernetes API transport unavailable")

// APIDoer sends one authenticated API request. Redirects must not be followed.
type APIDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// APITransport resolves the authenticated transport and absolute URL for one
// request. It prefers the process-wide client transport; the exec config also
// exists for remotecommand and must not cause a new client for each read.
func APITransport(client kubernetes.Interface, config *rest.Config, r program.KubernetesRequest) (APIDoer, *url.URL, error) {
	if client != nil {
		if transport, ok := client.Discovery().RESTClient().(*rest.RESTClient); ok && transport != nil && transport.Client != nil {
			copied := *transport.Client
			copied.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
			return &copied, transport.Verb(r.Method).AbsPath(r.Path).URL(), nil
		}
	}
	if config == nil {
		return nil, nil, ErrAPITransportUnavailable
	}
	configured, err := rest.HTTPClientFor(config)
	if err != nil {
		return nil, nil, ErrAPITransportUnavailable
	}
	base, err := url.Parse(config.Host)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: invalid API host", ErrAPITransportUnavailable)
	}
	base.Path += r.Path
	copied := *configured
	copied.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copied, base, nil
}

// APIRequest sends a validated request through doer with bounded bodies and
// returns the JSON response. Credential-issuing responses are never read.
func APIRequest(ctx context.Context, doer APIDoer, base *url.URL, r program.KubernetesRequest) (json.RawMessage, error) {
	values := url.Values{}
	for k, v := range r.Query {
		values.Set(k, v)
	}
	base.RawQuery = values.Encode()
	request, err := http.NewRequestWithContext(ctx, r.Method, base.String(), bytes.NewReader(r.Body))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid API request", ErrAPITransportUnavailable)
	}
	if r.ContentType != "" {
		request.Header.Set("Content-Type", r.ContentType)
	}
	request.Header.Set("Accept", "application/json")
	response, err := doer.Do(request)
	if err != nil {
		return nil, fmt.Errorf("Kubernetes API response unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, apierrors.NewGenericServerResponse(response.StatusCode, r.Method, schema.GroupResource{Resource: "api"}, "", "Kubernetes request failed", 0, false)
	}
	// Token issuance is expressible, but token bytes must stay in a container
	// when consumed. This transport never reads its response body.
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
	if strings.HasSuffix(r.Path, "/log") || r.Query["watch"] == "true" || strings.HasSuffix(r.Path, "/proxy") || strings.Contains(r.Path, "/proxy/") {
		return json.Marshal(string(raw))
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("Kubernetes API response is not JSON")
	}
	return raw, nil
}
