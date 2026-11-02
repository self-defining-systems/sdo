package runtime

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"sdo.dev/operator/sdk"
)

type cleanupInventory struct {
	client dynamic.Interface
	refs   map[sdk.ObjectRef]schema.GroupVersionResource
}

func (s cleanupInventory) TakeObjectChanges() []ObjectChange { return nil }

func (s cleanupInventory) SourceDriftObjects() []*unstructured.Unstructured {
	var result []*unstructured.Unstructured
	for ref, gvr := range s.refs {
		resource := s.client.Resource(gvr)
		var object *unstructured.Unstructured
		var err error
		if ref.Namespace != "" {
			object, err = resource.Namespace(ref.Namespace).Get(context.Background(), ref.Name, metav1.GetOptions{})
		} else {
			object, err = resource.Get(context.Background(), ref.Name, metav1.GetOptions{})
		}
		if err == nil {
			result = append(result, object)
		}
	}
	return result
}

// client-go v0.30's dynamic fake drops DeleteOptions. Check them before that
// boundary instead of pretending its reactor enforces Kubernetes preconditions.
type checkedCleanupDynamic struct {
	dynamic.Interface
	t *testing.T
}

func (c checkedCleanupDynamic) Resource(gvr schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return checkedCleanupResource{NamespaceableResourceInterface: c.Interface.Resource(gvr), t: c.t}
}

type checkedCleanupResource struct {
	dynamic.NamespaceableResourceInterface
	t *testing.T
}

func (c checkedCleanupResource) Namespace(ns string) dynamic.ResourceInterface {
	return checkedCleanupNamespaced{ResourceInterface: c.NamespaceableResourceInterface.Namespace(ns), t: c.t}
}

func (c checkedCleanupResource) Delete(ctx context.Context, name string, options metav1.DeleteOptions, sub ...string) error {
	checkCleanupPreconditions(c.t, options)
	return c.NamespaceableResourceInterface.Delete(ctx, name, options, sub...)
}

type checkedCleanupNamespaced struct {
	dynamic.ResourceInterface
	t *testing.T
}

func (c checkedCleanupNamespaced) Delete(ctx context.Context, name string, options metav1.DeleteOptions, sub ...string) error {
	checkCleanupPreconditions(c.t, options)
	return c.ResourceInterface.Delete(ctx, name, options, sub...)
}

func checkCleanupPreconditions(t *testing.T, options metav1.DeleteOptions) {
	t.Helper()
	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != "injected-uid" || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != "7" {
		t.Error("delete lacked exact UID/RV")
	}
}

