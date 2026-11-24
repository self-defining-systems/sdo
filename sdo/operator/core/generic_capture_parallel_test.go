package core

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	discoveryfake "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/dynamic"
	clienttesting "k8s.io/client-go/testing"

	"sdo.dev/operator/sdk"
)

type captureQuery struct {
	resource, namespace, continuation string
}

type scriptedCaptureClient struct {
	dynamic.Interface
	list func(context.Context, captureQuery) (*unstructured.UnstructuredList, error)
}

func (c scriptedCaptureClient) Resource(resource schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return scriptedCaptureResource{client: c, query: captureQuery{resource: resource.Resource}}
}

type scriptedCaptureResource struct {
	dynamic.NamespaceableResourceInterface
	client scriptedCaptureClient
	query  captureQuery
}

func (r scriptedCaptureResource) Namespace(namespace string) dynamic.ResourceInterface {
	r.query.namespace = namespace
	return r
}

func (r scriptedCaptureResource) List(ctx context.Context, options metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	r.query.continuation = options.Continue
	return r.client.list(ctx, r.query)
}

func captureTestDiscovery() *discoveryfake.FakeDiscovery {
	return &discoveryfake.FakeDiscovery{Fake: &clienttesting.Fake{Resources: []*metav1.APIResourceList{{
		GroupVersion: "example.test/v1",
		APIResources: []metav1.APIResource{
			{Name: "alphas", Kind: "Alpha", Namespaced: true, Verbs: []string{"list"}},
			{Name: "betas", Kind: "Beta", Namespaced: true, Verbs: []string{"list"}},
			{Name: "alphas/status", Kind: "Alpha", Namespaced: true, Verbs: []string{"list"}},
			{Name: "secrets", Kind: "Secret", Namespaced: true, Verbs: []string{"list"}},
			{Name: "globals", Kind: "Global", Verbs: []string{"list"}},
		},
	}}}}
}

func TestCaptureOverlapsCollectionsAndKeepsAllPages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		blocked := make(chan struct{})
		queries := make(chan captureQuery, 20)
		client := scriptedCaptureClient{list: func(_ context.Context, q captureQuery) (*unstructured.UnstructuredList, error) {
			queries <- q
			if q.resource == "alphas" && q.namespace == "app" && q.continuation == "" {
				<-blocked
			}
			list := &unstructured.UnstructuredList{}
			name := "first"
			if q.continuation == "" && q.resource == "alphas" {
				list.SetContinue("next")
			}
			if q.continuation != "" {
				name = "second"
			}
			item := unstructured.Unstructured{}
			item.SetName(name)
			item.SetNamespace(q.namespace)
			list.Items = append(list.Items, item)
			return list, nil
		}}
		capture := GenericCapture{Namespace: "app", Client: client, Discovery: captureTestDiscovery()}
		var objects []unstructured.Unstructured
		var err error
		done := make(chan struct{})
		go func() {
			defer close(done)
			objects, err = capture.CaptureObjects([]sdk.ObjectRef{{Kind: "Alpha", Namespace: "other"}})
		}()
		synctest.Wait()
		seen := map[captureQuery]int{}
		for len(queries) > 0 {
			seen[<-queries]++
		}
		if seen[captureQuery{"betas", "app", ""}] != 1 {
			t.Error("independent beta collection could not start while alpha was blocked")
		}
		close(blocked)
		<-done
		for len(queries) > 0 {
			seen[<-queries]++
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(objects) != 6 || len(seen) != 6 {
			t.Fatalf("wanted both alpha pages and beta in each of two namespaces, got %d objects from %v", len(objects), seen)
		}
		for _, ns := range []string{"app", "other"} {
			for _, q := range []captureQuery{{"alphas", ns, ""}, {"alphas", ns, "next"}, {"betas", ns, ""}} {
				if seen[q] != 1 {
					t.Errorf("query %+v observed %d times", q, seen[q])
				}
			}
		}
	})
}

func TestCaptureRejectsPartialCollection(t *testing.T) {
	lost := errors.New("collection unavailable")
	client := scriptedCaptureClient{list: func(_ context.Context, q captureQuery) (*unstructured.UnstructuredList, error) {
		if q.resource == "betas" {
			return nil, lost
		}
		return &unstructured.UnstructuredList{}, nil
	}}
	capture := GenericCapture{Namespace: "app", Client: client, Discovery: captureTestDiscovery()}
	if objects, err := capture.CaptureObjects(nil); objects != nil || !errors.Is(err, lost) {
		t.Fatalf("incomplete capture returned %v, %v", objects, err)
	}
}
