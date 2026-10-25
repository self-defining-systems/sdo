package runtime

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
)

// This client is captured only by the checked callback, never by PlanExecutor.
func newUnexplainedRemovalClient(config *rest.Config, capture *core.GenericCapture, namespace string) (PlanLifecycleClient, error) {
	if config == nil || capture == nil || namespace == capture.Namespace {
		return nil, nil
	}
	removalConfig := rest.CopyConfig(config)
	removalConfig.Impersonate = rest.ImpersonationConfig{UserName: "sdo-unexplained-removal:" + namespace}
	client, err := dynamic.NewForConfig(removalConfig)
	if err != nil {
		return nil, fmt.Errorf("create removal-only client: %w", err)
	}
	reader, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create removal ownership reader: %w", err)
	}
	return ownershipCheckingRemovalClient{PlanLifecycleClient: GenericPlanClient{Objects: &core.GenericCapture{Namespace: capture.Namespace, Client: client, Discovery: capture.Discovery}}, Reader: reader, Namespace: capture.Namespace}, nil
}

type ownershipCheckingRemovalClient struct {
	PlanLifecycleClient
	Reader    dynamic.Interface
	Namespace string
}

func (c ownershipCheckingRemovalClient) CheckRemovalOwnership(ctx context.Context, ref sdk.ObjectRef, eligible map[sdk.ObjectRef]bool) error {
	if ref.Kind != "ClusterRole" && ref.Kind != "ClusterRoleBinding" {
		return nil
	}
	if c.Reader == nil {
		return fmt.Errorf("removal ownership reader unavailable")
	}
	resource := "clusterroles"
	if ref.Kind == "ClusterRoleBinding" {
		resource = "clusterrolebindings"
	}
	live, err := c.Reader.Resource(schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: resource}).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read removal ownership: %w", err)
	}
	var objects []*unstructured.Unstructured
	if ref.Kind == "ClusterRole" {
		for _, resource := range []string{"clusterroles", "clusterrolebindings", "rolebindings"} {
			list, err := c.Reader.Resource(schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: resource}).List(ctx, metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("read complete removal ownership %s: %w", resource, err)
			}
			for i := range list.Items {
				objects = append(objects, &list.Items[i])
			}
		}
	}
	if !exclusiveRemovalOwnership(live, c.Namespace, objects, eligible) {
		return fmt.Errorf("shared_ownership: object is not exclusively application-owned")
	}
	return nil
}
