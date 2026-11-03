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
	"sync"
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
	// Recorder, when set, receives every controller write and cache observation.
	Recorder *Recorder
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
	// seq numbers committed controller writes for the recorder.
	seq     uint64
	api     *apiServer
	apiOnce sync.Once
	// mu serializes the store. The transport serves requests from many
	// goroutines (informers, the controller loop, agents); a scenario driver
	// that runs beside them goes through the locked methods below.
	mu sync.Mutex
	// WatchLag, when set, holds each watch event back by the returned duration
	// before the stream delivers it: an informer cache that serves stale reads.
	WatchLag func() time.Duration
}

func (k *Kube) watchLag() time.Duration {
	if k.WatchLag == nil {
		return 0
	}
	return k.WatchLag()
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
	k.mu.Lock()
	defer k.mu.Unlock()
	k.patches++
	if !k.churned {
		k.churned, k.raceBeforePatch = true, k.faults.ChurnPerPatch()
	}
	for _, hook := range k.beforeWrite {
		hook()
	}
	err := k.serverClient.Patch(ctx, ref, kind, patch)
	if k.Recorder != nil {
		if seq := k.Recorder.write(AttributionFrom(ctx), ref, string(kind), patch, err == nil); seq != 0 {
			k.seq = seq
		}
	}
	if err == nil {
		k.record(k.objects[ref], true)
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
	// frozenSeq is the write sequence the frozen snapshot reflects.
	frozenSeq uint64
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
		if snapshot, err := c.live(true); err == nil {
			c.frozen, c.pending, c.frozenSeq = snapshot, n, c.kube.seq
		}
	}
}

// live reads the store; held says the caller already holds the store lock.
func (c *Cache) live(held bool) (sdktest.Snapshot, error) {
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
	var secrets []sdk.SecretMetadata
	fields := map[string]string{"ConfigMap": "configMaps", "Service": "services", "Pod": "pods", "Deployment": "deployments", "StatefulSet": "statefulSets", "ReplicaSet": "replicaSets", "HorizontalPodAutoscaler": "horizontalPodAutoscalers", "PodDisruptionBudget": "podDisruptionBudgets", "Endpoints": "endpoints", "EndpointSlice": "endpointSlices", "NetworkPolicy": "networkPolicies", "Event": "events", "DaemonSet": "daemonSets", "Job": "jobs", "CronJob": "cronJobs", "PersistentVolumeClaim": "persistentVolumeClaims", "ServiceAccount": "serviceAccounts", "Ingress": "ingresses", "Node": "nodes"}
	objects := c.kube.Objects
	if held {
		objects = c.kube.objectsLocked
	}
	for _, object := range objects() {
		if object.GetNamespace() != snapshot.NamespaceName && object.GetKind() != "Node" {
			continue
		}
		if object.GetKind() == "Secret" {
			var secret corev1.Secret
			if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &secret); err != nil {
				return snapshot, err
			}
			secrets = append(secrets, sdk.SecretMetadataFrom(secret))
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
	namespace, observedAt := snapshot.NamespaceName, snapshot.ObservedAtTime
	err = json.Unmarshal(data, &snapshot)
	// Decoding resets the snapshot, so restore its identity and Secret metadata.
	snapshot.NamespaceName, snapshot.ObservedAtTime, snapshot.SecretList = namespace, observedAt, secrets
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
				c.frozen, _ = c.live(false)
				c.frozenSeq = c.kube.seq
			}
			c.pending = 1 << 30
		}
	}
	c.snapshots++
	if c.pending > 0 {
		c.pending--
		c.observed(c.frozenSeq)
		return c.frozen, nil
	}
	c.observed(c.kube.seq)
	return c.live(false)
}

// Objects returns defensive copies in stable API/kind/namespace/name order.
func (k *Kube) Objects() []*unstructured.Unstructured {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.objectsLocked()
}

// objectsLocked is Objects for a caller that holds the store lock.
func (k *Kube) objectsLocked() []*unstructured.Unstructured {
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
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, h := range k.beforeWrite {
		h()
	}
	o = o.DeepCopy()
	if o.GetUID() == "" {
		o.SetUID(types.UID("uid-" + o.GetName()))
	}
	k.bump(o)
	ref := sdk.ObjectRef{APIVersion: o.GetAPIVersion(), Kind: o.GetKind(), Namespace: o.GetNamespace(), Name: o.GetName()}
	_, existed := k.objects[ref]
	k.objects[ref] = o
	k.record(o, existed)
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
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.objectClient.Get(ctx, ref)
}

// SetFaults changes the seeded API/cache profile without creating another world.
func (k *Kube) SetFaults(f Faults)  { k.faults = f }
func (c *Cache) SetFaults(f Faults) { c.faults = f }

// observed records a snapshot read that reflects writes through seq.
func (c *Cache) observed(seq uint64) {
	if c.kube.Recorder != nil {
		c.kube.Recorder.observe(seq)
	}
}
