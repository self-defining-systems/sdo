// Package sim provides a seeded, in-memory Kubernetes world for Go simulation tests.
// Mutable cluster state and fault decisions are owned by the test driver goroutine.
package sim

import (
	"context"
	"encoding/json"
	"errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"math/rand/v2"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sort"
	"time"
)

// Faults is the fault profile of one run, drawn from the seed. Counts are
// bounded so that a correct executor can always make progress unless a test
// asks for a hard fault.
type Faults struct {
	// ChurnPerPatch is the number of status-only resourceVersion bumps that land
	// between the caller's read and each of its patches (0..5-1).
	ChurnPerPatch func() int
	// FailAfterCommit reports whether this patch commits and then returns a
	// transport error, as a request that times out after etcd accepted it.
	FailAfterCommit func() bool
	// StaleReads is how many cache snapshots after a write still show the
	// pre-write object.
	StaleReads func() int
}

func DrawFaults(rng *rand.Rand) Faults {
	churnP, commitP, staleMax := rng.Float64(), 0.2*rng.Float64(), rng.IntN(4)
	return Faults{
		ChurnPerPatch: func() int {
			if rng.Float64() < churnP {
				return 1 + rng.IntN(5-1)
			}
			return 0
		},
		FailAfterCommit: func() bool { return rng.Float64() < commitP },
		StaleReads:      func() int { return rng.IntN(staleMax + 1) },
	}
}

// ErrTimeout is the transport error of a request whose outcome is unknown.
var ErrTimeout = errors.New("simulated: etcdserver: request timed out")

// Kube is the simulated API server. All requests come from one goroutine.
type Kube struct {
	Namespace string
	Before    func(context.Context, string) (Fault, error)
	Clock     func() time.Time
	*serverClient
	faults Faults
	// beforeWrite runs before every patch is committed, so caches can capture
	// the pre-write state and go stale.
	beforeWrite []func()
	// patches counts Patch calls that reached the server.
	patches int
	// churned is set once a logical write has drawn its churn, until it commits.
	churned bool
	// timeouts counts writes that committed but reported a transport error.
	timeouts int
}

func NewKube(faults Faults, objects ...*unstructured.Unstructured) *Kube {
	first := &serverClient{objectClient: &objectClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}}
	kube := &Kube{serverClient: first, faults: faults}
	for _, input := range objects {
		object := input.DeepCopy()
		ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}
		object.SetUID(types.UID("uid-" + object.GetName()))
		object.SetResourceVersion("1")
		kube.objects[ref] = object
		kube.settle(object)
	}
	return kube
}

func (k *Kube) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	var fault Fault
	if k.Before != nil {
		var err error
		fault, err = k.Before(ctx, "patch")
		if err != nil {
			return err
		}
	}
	k.patches++
	if !k.churned {
		k.churned, k.raceBeforePatch = true, k.faults.ChurnPerPatch()
	}
	for _, hook := range k.beforeWrite {
		hook()
	}
	err := k.serverClient.Patch(ctx, ref, kind, patch)
	if err == nil {
		k.churned = false
		if fault.CommitThenTimeout || k.faults.FailAfterCommit() {
			k.timeouts++
			return ErrTimeout
		}
	}
	return err
}

// Cache is the informer-cache view of a Kube. After each write it keeps
// serving the pre-write objects for a seeded number of snapshots.
type Cache struct {
	kube      *Kube
	faults    Faults
	pending   int
	frozen    sdktest.Snapshot
	snapshots int
}

func NewCache(kube *Kube, faults Faults) *Cache {
	cache := &Cache{kube: kube, faults: faults}
	kube.beforeWrite = append(kube.beforeWrite, cache.wrote)
	return cache
}

// wrote freezes the cache at the pre-write state for the next few snapshots.
func (c *Cache) wrote() {
	if c.pending > 0 {
		return
	}
	if n := c.faults.StaleReads(); n > 0 {
		if snapshot, err := c.live(); err == nil {
			c.frozen, c.pending = snapshot, n
		}
	}
}

func (c *Cache) live() (sdktest.Snapshot, error) {
	snapshot := sdktest.Snapshot{NamespaceName: c.kube.Namespace}
	if c.kube.Clock != nil {
		now := c.kube.Clock()
		snapshot.ObservedAtTime = &now
	}
	if snapshot.NamespaceName == "" {
		snapshot.NamespaceName = "app"
	}
	// Decode through the SDK snapshot's public JSON shape, in stable object order.
	lists := map[string][]json.RawMessage{}
	fields := map[string]string{"ConfigMap": "configMaps", "Service": "services", "Pod": "pods", "Deployment": "deployments", "StatefulSet": "statefulSets", "ReplicaSet": "replicaSets", "HorizontalPodAutoscaler": "horizontalPodAutoscalers", "PodDisruptionBudget": "podDisruptionBudgets", "Endpoints": "endpoints", "EndpointSlice": "endpointSlices", "NetworkPolicy": "networkPolicies", "Event": "events", "DaemonSet": "daemonSets", "Job": "jobs", "CronJob": "cronJobs", "PersistentVolumeClaim": "persistentVolumeClaims", "ServiceAccount": "serviceAccounts", "Ingress": "ingresses", "Node": "nodes"}
	for _, object := range c.kube.Objects() {
		if object.GetNamespace() != snapshot.NamespaceName && object.GetKind() != "Node" {
			continue
		}
		if object.GetKind() == "Secret" {
			var secret corev1.Secret
			if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &secret); err != nil {
				return snapshot, err
			}
			snapshot.SecretList = append(snapshot.SecretList, sdk.SecretMetadataFrom(secret))
			continue
		}
		field, ok := fields[object.GetKind()]
		if !ok {
			continue
		}
		data, err := json.Marshal(object.Object)
		if err != nil {
			return snapshot, err
		}
		lists[field] = append(lists[field], data)
	}
	data, err := json.Marshal(lists)
	if err != nil {
		return snapshot, err
	}
	err = json.Unmarshal(data, &snapshot)
	return snapshot, err
}

// Snapshot implements the SnapshotProvider the responder loop reads through.
func (c *Cache) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	if c.kube.Before != nil {
		f, err := c.kube.Before(ctx, "snapshot")
		if err != nil {
			return nil, err
		}
		if f.DropWatch {
			if c.pending == 0 {
				c.frozen, _ = c.live()
			}
			c.pending = 1 << 30
		}
	}
	c.snapshots++
	if c.pending > 0 {
		c.pending--
		return c.frozen, nil
	}
	return c.live()
}

// Objects returns defensive copies in stable API/kind/namespace/name order.
func (k *Kube) Objects() []*unstructured.Unstructured {
	result := make([]*unstructured.Unstructured, 0, len(k.objects))
	for _, o := range k.objects {
		result = append(result, o.DeepCopy())
	}
	sort.Slice(result, func(i, j int) bool { return objectKey(result[i]) < objectKey(result[j]) })
	return result
}
func objectKey(o *unstructured.Unstructured) string {
	return o.GetAPIVersion() + "/" + o.GetKind() + "/" + o.GetNamespace() + "/" + o.GetName()
}

// Put models an external controller or kubelet update, preserving cache lag.
func (k *Kube) Put(o *unstructured.Unstructured) {
	for _, h := range k.beforeWrite {
		h()
	}
	o = o.DeepCopy()
	if o.GetUID() == "" {
		o.SetUID(types.UID("uid-" + o.GetName()))
	}
	k.bump(o)
	k.objects[sdk.ObjectRef{APIVersion: o.GetAPIVersion(), Kind: o.GetKind(), Namespace: o.GetNamespace(), Name: o.GetName()}] = o
}

// CatchUp delivers a relist after a write or dropped watch.
func (c *Cache) CatchUp() { c.pending = 0 }

// QuietFaults disables random write/cache faults for a directed scenario.
func QuietFaults() Faults {
	return Faults{ChurnPerPatch: func() int { return 0 }, FailAfterCommit: func() bool { return false }, StaleReads: func() int { return 0 }}
}

// Get returns an isolated live API read, with scheduled errors and partitions.
func (k *Kube) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if k.Before != nil {
		if _, err := k.Before(ctx, "get"); err != nil {
			return nil, err
		}
	}
	return k.objectClient.Get(ctx, ref)
}

// SetFaults changes the seeded API/cache profile without creating another world.
func (k *Kube) SetFaults(f Faults)  { k.faults = f }
func (c *Cache) SetFaults(f Faults) { c.faults = f }
