package runtime

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"sync"
)

// Discovery describes kinds, not individual objects. Share it within one
// snapshot without caching live SSA/admission answers or carrying stale API
// availability into the next observation.
type cycleDiscovery struct {
	discovery.DiscoveryInterface
	mu      sync.Mutex
	entries map[string]*cycleResources
}
type cycleResources struct {
	once sync.Once
	list *metav1.APIResourceList
	err  error
}

func (d *cycleDiscovery) ServerResourcesForGroupVersion(version string) (*metav1.APIResourceList, error) {
	d.mu.Lock()
	entry := d.entries[version]
	if entry == nil {
		entry = &cycleResources{}
		d.entries[version] = entry
	}
	d.mu.Unlock()
	entry.once.Do(func() { entry.list, entry.err = d.DiscoveryInterface.ServerResourcesForGroupVersion(version) })
	return entry.list.DeepCopy(), entry.err
}

type cycleClient struct {
	kubernetes.Interface
	discovery discovery.DiscoveryInterface
}

func (c cycleClient) Discovery() discovery.DiscoveryInterface { return c.discovery }
func snapshotClient(client kubernetes.Interface) kubernetes.Interface {
	if client == nil {
		return nil
	}
	return cycleClient{Interface: client, discovery: &cycleDiscovery{DiscoveryInterface: client.Discovery(), entries: map[string]*cycleResources{}}}
}
