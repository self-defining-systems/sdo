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
	Strict          bool
	UID             string
	Absent          bool
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
		// These kinds have no cached verifier input: HPA/PDB snapshots list the
		// API currently; scoped webhook reads are direct. Keep watched barriers.
		if mode, ok := e.Observer.(interface{ DirectPlanReadSuffices(sdk.ObjectRef) bool }); ok && patch.DirectReadConfirmed && mode.DirectPlanReadSuffices(patch.Ref) {
			continue
		}
		if patch.Operation == "delete" {
			writes = append(writes, ObservedWrite{Ref: patch.Ref, UID: string(patch.Before.GetUID()), Absent: true, Strict: true})
			continue
		}
		if patch.Structured && patch.After.GetResourceVersion() == "" {
			return fmt.Errorf("write observation unknown: resource version is absent")
		}
		if version := patch.After.GetResourceVersion(); version != "" {
			writes = append(writes, ObservedWrite{Ref: patch.Ref, ResourceVersion: version, Strict: patch.Structured, UID: string(patch.After.GetUID())})
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
// does not watch or cannot observe fail closed for structured actions. Legacy
// writes retain their existing best-effort cache behavior.
func (c *KubernetesCache) AwaitWrites(ctx context.Context, writes []ObservedWrite) error {
	ctx, cancel := context.WithTimeout(ctx, cacheObservationLimit)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !c.observed(writes) {
		select {
		case <-ctx.Done():
			for _, write := range writes {
				if write.Strict {
					return fmt.Errorf("write observation unknown: %w", ctx.Err())
				}
			}
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
			if write.Strict {
				return false
			}
			continue
		}
		key := write.Ref.Name
		if write.Ref.Namespace != "" {
			key = write.Ref.Namespace + "/" + key
		}
		stored, exists, err := informer.GetStore().GetByKey(key)
		if write.Absent {
			if err != nil || exists {
				return false
			}
			continue
		}
		if err != nil || !exists {
			return false
		}
		accessor, err := meta.Accessor(stored)
		if err != nil || (write.UID != "" && string(accessor.GetUID()) != write.UID) || !resourceVersionAtLeast(accessor.GetResourceVersion(), write.ResourceVersion) {
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

// DirectPlanReadSuffices only exempts known uncached kinds after the executor
// has independently proved the current effect. Unknown and watched kinds wait.
func (c *KubernetesCache) DirectPlanReadSuffices(ref sdk.ObjectRef) bool {
	c.informerMu.RLock()
	defer c.informerMu.RUnlock()
	if _, watched := c.informers[ref.APIVersion+"/"+ref.Kind]; watched {
		return false
	}
	switch ref.APIVersion + "/" + ref.Kind {
	case "autoscaling/v2/HorizontalPodAutoscaler", "policy/v1/PodDisruptionBudget", "admissionregistration.k8s.io/v1/MutatingWebhookConfiguration", "admissionregistration.k8s.io/v1/ValidatingWebhookConfiguration":
		return true
	}
	return false
}
