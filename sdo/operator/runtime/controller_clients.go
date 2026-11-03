package runtime

import (
	"fmt"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/core"
)

type controllerClientSet struct {
	snapshot                   *core.KubernetesSnapshotProvider
	state, lease, incidentRead kubernetes.Interface
}

// Native broker initialization supplies a configured client too; only the
// caller's original injection establishes an embedded API boundary. Production
// state, leadership and incident observations each own a token bucket so steady
// detector normalization cannot starve a responder's fresh reads.
func newControllerClients(namespace string, configured kubernetes.Interface, embedded bool, create func() (kubernetes.Interface, error)) (*controllerClientSet, error) {
	if namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}
	if embedded {
		if configured == nil {
			return nil, fmt.Errorf("embedded controller requires its injected Kubernetes client")
		}
		return &controllerClientSet{snapshot: &core.KubernetesSnapshotProvider{Namespace: namespace, Client: configured}, state: configured, lease: configured, incidentRead: configured}, nil
	}
	clients := make([]kubernetes.Interface, 4)
	for i, role := range []string{"snapshot", "controller-state", "leader-election", "incident-read"} {
		client, err := create()
		if err != nil {
			return nil, fmt.Errorf("create dedicated %s client: %w", role, err)
		}
		clients[i] = client
	}
	return &controllerClientSet{snapshot: &core.KubernetesSnapshotProvider{Namespace: namespace, Client: clients[0]}, state: clients[1], lease: clients[2], incidentRead: clients[3]}, nil
}
