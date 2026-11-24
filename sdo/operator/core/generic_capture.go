package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	discoverymemory "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"

	"sdo.dev/operator/core/internal/kubeio"
	"sdo.dev/operator/sdk"
)

// GenericCapture is the thin Kubernetes boundary for versioned object capture.
// Discovery, rather than a kind inventory, defines its readable universe.
// Secrets and subresources are never requested by this client.
type GenericCapture struct {
	Context   context.Context
	Namespace string
	Client    dynamic.Interface
	Discovery discovery.DiscoveryInterface
}

func NewGenericCapture(namespace string) (*GenericCapture, error) {
	config, err := kubeio.Config()
	if err != nil {
		return nil, err
	}
	if err = applyClientRateLimits(config, kubeio.LookupEnv); err != nil {
		return nil, err
	}
	config.Timeout = 30 * time.Second
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	discover, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, err
	}
	return &GenericCapture{Namespace: namespace, Client: client, Discovery: discoverymemory.NewMemCacheClient(discover)}, nil
}

type captureResource struct {
	gvr      schema.GroupVersionResource
	resource metav1.APIResource
}

func (g *GenericCapture) resources() ([]captureResource, error) {
	if g == nil || g.Client == nil || g.Discovery == nil {
		return nil, errors.New("generic Kubernetes reader unavailable")
	}
	_, lists, err := g.Discovery.ServerGroupsAndResources()
	if err != nil {
		return nil, fmt.Errorf("discover capture resources: %w", err)
	}
	var out []captureResource
	for _, list := range lists {
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			return nil, err
		}
		for _, resource := range list.APIResources {
			if (resource.Kind == "Secret" || (gv.Group == "" && resource.Name == "secrets")) || strings.Contains(resource.Name, "/") {
				continue
			}
			out = append(out, captureResource{gvr: gv.WithResource(resource.Name), resource: resource})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].gvr.String() < out[j].gvr.String() })
	return out, nil
}
func captureVerb(r metav1.APIResource, verb string) bool {
	for _, v := range r.Verbs {
		if v == verb {
			return true
		}
	}
	return false
}
func captureMatch(r captureResource, ref sdk.ObjectRef) bool {
	if ref.APIVersion != "" && ref.APIVersion != r.gvr.GroupVersion().String() {
		return false
	}
	name, group, qualified := strings.Cut(strings.ToLower(ref.Kind), ".")
	if qualified && group != r.gvr.Group {
		return false
	}
	for _, alias := range append([]string{r.resource.Kind, r.resource.Name, r.resource.SingularName}, r.resource.ShortNames...) {
		if strings.EqualFold(name, alias) {
			return true
		}
	}
	return false
}
func (g *GenericCapture) context() context.Context {
	if g.Context != nil {
		return g.Context
	}
	return context.Background()
}

func (g *GenericCapture) CaptureObjects(refs []sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	if g != nil {
		if cached, ok := g.Discovery.(discovery.CachedDiscoveryInterface); ok {
			cached.Invalidate()
		}
	}
	resources, err := g.resources()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(g.context(), 45*time.Second)
	defer cancel()
	namespaceSet := map[string]bool{g.Namespace: true}
	for _, ref := range refs {
		namespaceSet[ref.Namespace] = true
	}
	var namespaces []string
	for namespace := range namespaceSet {
		if namespace != "" {
			namespaces = append(namespaces, namespace)
		}
	}
	sort.Strings(namespaces)

	// Each collection is independent. Limit outstanding requests while
	// preserving each collection's pagination and the client's rate limit.
	slots := make(chan struct{}, 8)
	results := make([][]unstructured.Unstructured, len(resources))
	var workers sync.WaitGroup
	var firstFailure sync.Once
	var captureErr error
collect:
	for i, r := range resources {
		if !captureVerb(r.resource, "list") {
			continue
		}
		select {
		case <-ctx.Done():
			break collect
		case slots <- struct{}{}:
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-slots }()
			if ctx.Err() != nil {
				return
			}
			objects, err := g.captureResourceObjects(ctx, r, namespaces, refs)
			if err != nil {
				firstFailure.Do(func() {
					captureErr = err
					cancel()
				})
				return
			}
			results[i] = objects
		}()
	}
	workers.Wait()
	if captureErr != nil {
		return nil, captureErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := slices.Concat(results...)
	sdk.SortObjects(out)
	return out, nil
}

func (g *GenericCapture) captureResourceObjects(ctx context.Context, r captureResource, namespaces []string, refs []sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	resource := g.Client.Resource(r.gvr)
	var out []unstructured.Unstructured
	if r.resource.Namespaced {
		for _, ns := range namespaces {
			items, err := r.captureList(ctx, resource.Namespace(ns))
			if err != nil {
				return nil, fmt.Errorf("capture %s in namespace %s: %w", r.gvr, ns, err)
			}
			out = append(out, items...)
		}
		return out, nil
	}

	// Cluster resources require an explicit incident reference; no cluster sweep.
	names := map[string]bool{}
	for _, ref := range refs {
		if ref.Namespace == "" && captureMatch(r, ref) {
			names[ref.Name] = true
		}
	}
	if names[""] {
		items, err := r.captureList(ctx, resource)
		if err != nil {
			return nil, fmt.Errorf("capture %s: %w", r.gvr, err)
		}
		return items, nil
	}
	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)
	for _, name := range sorted {
		o, err := resource.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("capture %s/%s: %w", r.gvr, name, err)
		}
		o.SetAPIVersion(r.gvr.GroupVersion().String())
		o.SetKind(r.resource.Kind)
		out = append(out, *o)
	}
	return out, nil
}

func (r captureResource) captureList(ctx context.Context, client dynamic.ResourceInterface) ([]unstructured.Unstructured, error) {
	var out []unstructured.Unstructured
	token := ""
	for {
		list, err := client.List(ctx, metav1.ListOptions{Limit: 500, Continue: token})
		if err != nil {
			return nil, err
		}
		for _, o := range list.Items {
			o.SetAPIVersion(r.gvr.GroupVersion().String())
			o.SetKind(r.resource.Kind)
			out = append(out, o)
		}
		token = list.GetContinue()
		if token == "" {
			return out, nil
		}
	}
}

func (g *GenericCapture) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if strings.EqualFold(ref.Kind, "Secret") {
		return nil, errors.New("Secret reads forbidden")
	}
	resources, err := g.resources()
	if err != nil {
		return nil, err
	}
	if ref.Name == "" || ref.APIVersion == "" {
		return nil, errors.New("object read requires version and name")
	}
	for _, r := range resources {
		if captureMatch(r, ref) {
			if r.resource.Namespaced != (ref.Namespace != "") {
				return nil, errors.New("object scope does not match discovery")
			}
			var object *unstructured.Unstructured
			if r.resource.Namespaced {
				object, err = g.Client.Resource(r.gvr).Namespace(ref.Namespace).Get(g.context(), ref.Name, metav1.GetOptions{})
			} else {
				object, err = g.Client.Resource(r.gvr).Get(g.context(), ref.Name, metav1.GetOptions{})
			}
			if err != nil {
				return nil, err
			}
			object.SetAPIVersion(ref.APIVersion)
			object.SetKind(r.resource.Kind)
			return object, nil
		}
	}
	return nil, fmt.Errorf("object kind is not discoverable: %+v", ref)
}

// Resource resolves exact version and scope through the same discovery used by
// capture. It is also the single resource resolver for generic plan transports.
func (g *GenericCapture) Resource(ref sdk.ObjectRef) (dynamic.ResourceInterface, error) {
	if ref.Name == "" || ref.APIVersion == "" || strings.EqualFold(ref.Kind, "Secret") {
		return nil, errors.New("generic resource requires a versioned nonsecret identity")
	}
	resources, err := g.resources()
	if err != nil {
		return nil, err
	}
	for _, r := range resources {
		if captureMatch(r, ref) {
			if r.resource.Namespaced != (ref.Namespace != "") {
				return nil, errors.New("resource scope does not match discovery")
			}
			if r.resource.Namespaced {
				return g.Client.Resource(r.gvr).Namespace(ref.Namespace), nil
			}
			return g.Client.Resource(r.gvr), nil
		}
	}
	return nil, fmt.Errorf("resource is not discoverable: %+v", ref)
}
