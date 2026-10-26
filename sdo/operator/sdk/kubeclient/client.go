// Package kubeclient exposes upstream Kubernetes read APIs over SDO's recorded
// observation boundary. It owns no credentials, sockets or independent history.
package kubeclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	discoverycache "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

// ReadFunc is implemented by the existing recorded ReadView, including replay.
// JSON string results represent log bytes or a bounded watch stream.
type ReadFunc func(context.Context, string, string, url.Values) ([]byte, error)

// ReadClient has client-go's group/resource shape with mutation methods omitted.
// Every request still crosses the read-only transport, including request builders.
type ReadClient struct {
	metrics   metricsclient.Interface
	builtin   kubernetes.Interface
	dynamic   dynamic.Interface
	discovery discovery.CachedDiscoveryInterface
	mapper    meta.RESTMapper
}

// NewReadClient builds clients once for an observation view. The callback reuses
// its process-wide authenticated transport; no network connections are made here.
func NewReadClient(read ReadFunc) (*ReadClient, error) {
	if read == nil {
		return nil, fmt.Errorf("Kubernetes observation reader required")
	}
	config := &rest.Config{Host: "https://recorded.invalid", QPS: 50, Burst: 100, Timeout: 5 * time.Second}
	httpClient := &http.Client{Transport: readTransport{read}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	builtin, err := kubernetes.NewForConfigAndClient(config, httpClient)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfigAndClient(config, httpClient)
	if err != nil {
		return nil, err
	}
	disc, err := discovery.NewDiscoveryClientForConfigAndClient(config, httpClient)
	if err != nil {
		return nil, err
	}
	metrics, err := metricsclient.NewForConfigAndClient(config, httpClient)
	if err != nil {
		return nil, err
	}
	cached := discoverycache.NewMemCacheClient(disc)
	return &ReadClient{metrics: metrics, builtin: builtin, dynamic: dyn, discovery: cached, mapper: restmapper.NewDeferredDiscoveryRESTMapper(cached)}, nil
}

// Metrics exposes upstream metrics.k8s.io clients and resource.Quantity values.
func (c *ReadClient) Metrics() metricsclient.Interface { return c.metrics }

// RESTMapper caches API discovery and resolves built-in kinds and CRDs alike.
func (c *ReadClient) RESTMapper() meta.RESTMapper { return c.mapper }

// Dynamic returns a read-only dynamic client for any server-discovered resource.
func (c *ReadClient) Dynamic() ReadDynamic { return ReadDynamic{client: c.dynamic} }

type ReadDynamic struct{ client dynamic.Interface }

func (c ReadDynamic) Resource(gvr schema.GroupVersionResource) NamespaceableResource {
	return namespaceableResource{c.client.Resource(gvr)}
}

type Resource interface {
	Get(context.Context, string, metav1.GetOptions, ...string) (*unstructured.Unstructured, error)
	List(context.Context, metav1.ListOptions) (*unstructured.UnstructuredList, error)
	Watch(context.Context, metav1.ListOptions) (watch.Interface, error)
}
type NamespaceableResource interface {
	Resource
	Namespace(string) Resource
}
type namespaceableResource struct {
	dynamic.NamespaceableResourceInterface
}

func (r namespaceableResource) Namespace(ns string) Resource {
	return r.NamespaceableResourceInterface.Namespace(ns)
}

type readTransport struct{ read ReadFunc }

func (t readTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != "GET" && r.Method != "HEAD" {
		return nil, fmt.Errorf("Kubernetes observation client is read-only")
	}
	// Build a finite observation even if a caller forgot a deadline.
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	raw, err := t.read(ctx, r.Method, r.URL.Path, r.URL.Query())
	if err != nil {
		return nil, err
	}
	contentType := "application/json"
	if strings.HasSuffix(r.URL.Path, "/log") || r.URL.Query().Get("watch") == "true" || strings.HasSuffix(r.URL.Path, "/proxy") || strings.Contains(r.URL.Path, "/proxy/") {
		var stream string
		if err := json.Unmarshal(raw, &stream); err == nil {
			raw = []byte(stream)
		}
		if strings.HasSuffix(r.URL.Path, "/log") {
			contentType = "text/plain"
		}
	}
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(bytes.NewReader(raw)), ContentLength: int64(len(raw)), Request: r}, nil
}
