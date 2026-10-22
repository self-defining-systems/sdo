package runtime

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"

	"sdo.dev/operator/sdk"
)

// ObservedWrite is an object version a plan application wrote to the API server.
type ObservedWrite struct {
	Ref             sdk.ObjectRef
	ResourceVersion string
}

// WriteObserver is implemented by snapshot providers that serve reads from a
// watch cache. AwaitWrites returns once the cache shows each written object at
// the written version or later, so a verification that follows a write cannot
// read the object as it was before the write.
type WriteObserver interface {
	AwaitWrites(context.Context, []ObservedWrite) error
}

// awaitObserved waits until the executor's observer has seen every write of the
// application. Without an observer reads are already consistent with the writes.
func (e PlanExecutor) awaitObserved(ctx context.Context, patches []PlanPatch) error {
	if e.Observer == nil {
		return nil
	}
	writes := make([]ObservedWrite, 0, len(patches))
	for _, patch := range patches {
		if version := patch.After.GetResourceVersion(); version != "" {
			writes = append(writes, ObservedWrite{Ref: patch.Ref, ResourceVersion: version})
		}
	}
	if err := e.Observer.AwaitWrites(ctx, writes); err != nil {
		return fmt.Errorf("wait for the cache to observe plan writes: %w", err)
	}
	return nil
}

// cacheObservationLimit bounds how long a verification waits for the watch
// cache; a cache that is not delivering events must not stall an incident.
const cacheObservationLimit = 5 * time.Second

// AwaitWrites implements WriteObserver over the informer stores. Kinds the cache
// does not watch are skipped, and a cache that does not catch up within
// cacheObservationLimit is read as it is: verification then judges the state the
// cache shows, as it did before this wait existed.
func (c *KubernetesCache) AwaitWrites(ctx context.Context, writes []ObservedWrite) error {
	ctx, cancel := context.WithTimeout(ctx, cacheObservationLimit)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !c.observed(writes) {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
	return nil
}

func (c *KubernetesCache) observed(writes []ObservedWrite) bool {
	c.informerMu.RLock()
	defer c.informerMu.RUnlock()
	for _, write := range writes {
		informer, watched := c.informers[write.Ref.APIVersion+"/"+write.Ref.Kind]
		if !watched {
			continue
		}
		stored, exists, err := informer.GetStore().GetByKey(write.Ref.Namespace + "/" + write.Ref.Name)
		if err != nil || !exists {
			return false
		}
		accessor, err := meta.Accessor(stored)
		if err != nil || !resourceVersionAtLeast(accessor.GetResourceVersion(), write.ResourceVersion) {
			return false
		}
	}
	return true
}

// resourceVersionAtLeast compares versions numerically, as etcd-backed servers
// order them; versions that are not numbers must match exactly.
func resourceVersionAtLeast(have, want string) bool {
	h, herr := strconv.ParseUint(have, 10, 64)
	w, werr := strconv.ParseUint(want, 10, 64)
	if herr != nil || werr != nil {
		return have == want
	}
	return h >= w
}
