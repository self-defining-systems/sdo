package kubeclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// WriteFunc executes one admitted operation through the owning executor's
// existing recording boundary. It must return the actual API response.
type WriteFunc func(context.Context, string, string, url.Values, string, []byte) ([]byte, error)

// ExecutionClient exposes the complete pinned client-go clientset and dynamic
// mutation APIs. It is an execution capability, never part of a detector ReadView.
// All clients share one callback transport; no credentials or sockets live here.
type ExecutionClient struct {
	kubernetes.Interface
	reader *ReadClient
}

func NewExecutionClient(read ReadFunc, write WriteFunc) (*ExecutionClient, error) {
	if read == nil || write == nil {
		return nil, fmt.Errorf("recorded reader and execution writer required")
	}
	reader, err := newReadClient(&http.Client{Transport: executionTransport{read: read, write: write}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }})
	if err != nil {
		return nil, err
	}
	return &ExecutionClient{Interface: reader.builtin, reader: reader}, nil
}
func (c *ExecutionClient) Dynamic() dynamic.Interface  { return c.reader.dynamic }
func (c *ExecutionClient) RESTMapper() meta.RESTMapper { return c.reader.mapper }

type executionTransport struct {
	read  ReadFunc
	write WriteFunc
}

func (t executionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return (readTransport{t.read}).RoundTrip(r)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	var body []byte
	var err error
	if r.Body != nil {
		body, err = io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
	}
	query := r.URL.Query()
	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/apply-patch+") && query.Get("fieldManager") == "" {
		query.Set("fieldManager", "sdo")
	}
	raw, err := t.write(ctx, r.Method, r.URL.Path, query, r.Header.Get("Content-Type"), body)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(raw)), ContentLength: int64(len(raw)), Request: r}, nil
}
