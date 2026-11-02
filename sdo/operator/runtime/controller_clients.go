package runtime

import (
	"fmt"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/core"
)

// Controller state and leadership must not queue behind incident observation,
// worker or validation traffic. Each production client owns its token bucket;
// injected clients keep embedded runs entirely on their supplied API boundary.
func controllerKubernetesClients(namespace string, injected kubernetes.Interface) (*core.KubernetesSnapshotProvider, kubernetes.Interface, kubernetes.Interface, error) {
	if injected != nil {
		return &core.KubernetesSnapshotProvider{Namespace: namespace, Client: injected}, injected, injected, nil
	}
	provider, err := core.NewKubernetesSnapshotProvider(namespace)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create Kubernetes snapshot provider: %w", err)
	}
	state, err := core.NewKubernetesClient()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create dedicated controller-state client: %w", err)
	}
	lease, err := core.NewKubernetesClient()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create dedicated leader-election client: %w", err)
	}
	return provider, lease, state, nil
}
