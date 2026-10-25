package runtime

import (
	"fmt"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/core"
)

// This client is captured only by the checked callback, never by PlanExecutor.
func newUnexplainedRemovalClient(config *rest.Config, capture *core.GenericCapture, namespace string) (PlanLifecycleClient, error) {
	if config == nil || capture == nil {
		return nil, nil
	}
	removalConfig := rest.CopyConfig(config)
	removalConfig.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:" + namespace + ":sdo-unexplained-removal"}
	client, err := dynamic.NewForConfig(removalConfig)
	if err != nil {
		return nil, fmt.Errorf("create removal-only client: %w", err)
	}
	return GenericPlanClient{Objects: &core.GenericCapture{Namespace: capture.Namespace, Client: client, Discovery: capture.Discovery}}, nil
}
