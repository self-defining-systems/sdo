// Deterministic simulation of the Kubernetes API for the incident loop.
//
// simKube wraps the in-package API-server model (serverPlanClient: defaulting,
// quantity normalization, resourceVersion preconditions) with a seeded fault
// schedule, and simCache serves the stale reads an informer cache gives a
// caller right after a write. Every fault decision comes from one seeded RNG
// consumed in call order, so a failing seed replays exactly:
//
//	SDO_SIM_SEED=<n> go test ./sdo/operator/runtime -run TestSim
//
// SDO_SIM_RUNS changes the number of seeds swept; a sweep also stops at
// simWallBudget so no profile exceeds the project's five-minute cap.
package runtime

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

const (
	simDefaultRuns = 256
	// simMaxChurn bounds the churn bumps drawn for one write.
	simMaxChurn   = 4
	simWallBudget = 45 * time.Second
)

// simSeeds returns the seeds to sweep; SDO_SIM_SEED replays one.
func simSeeds(t *testing.T, defaultRuns int) []uint64 {
	t.Helper()
	if raw := os.Getenv("SDO_SIM_SEED"); raw != "" {
		seed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatalf("invalid SDO_SIM_SEED %q: %v", raw, err)
		}
		return []uint64{seed}
	}
	runs := defaultRuns
	if raw := os.Getenv("SDO_SIM_RUNS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			t.Fatalf("invalid SDO_SIM_RUNS %q", raw)
		}
		runs = parsed
	}
	seeds := make([]uint64, runs)
	for i := range seeds {
		seeds[i] = uint64(i) + 1
	}
	return seeds
}

// sweepSimSeeds runs body for each seed until the seeds or the wall budget run out.
func sweepSimSeeds(t *testing.T, body func(t *testing.T, seed uint64)) {
	t.Helper()
	sweepSimSeedsN(t, simDefaultRuns, body)
}

// sweepSimSeedsN is sweepSimSeeds with a per-test default run count, for
// simulations whose single run is expensive.
func sweepSimSeedsN(t *testing.T, defaultRuns int, body func(t *testing.T, seed uint64)) {
	t.Helper()
	deadline := time.Now().Add(simWallBudget)
	ran := 0
	seeds := simSeeds(t, defaultRuns)
	for _, seed := range seeds {
		if ran > 0 && time.Now().After(deadline) {
			break
		}
		ran++
		t.Run("seed="+strconv.FormatUint(seed, 10), func(t *testing.T) { body(t, seed) })
	}
	t.Logf("sim sweep ran %d/%d seed(s)", ran, len(seeds))
}

func newSimRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed^0xd1b54a32d192ed03))
}

// simClock is the virtual clock: Sleep advances it and never blocks.
type simClock struct {
	now    time.Time
	sleeps []time.Duration
}

func newSimClock() *simClock { return &simClock{now: time.Unix(1_700_000_000, 0).UTC()} }

func (c *simClock) Now() time.Time { return c.now }

func (c *simClock) Sleep(_ context.Context, d time.Duration) error {
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
	return nil
}

// simFaults is the fault profile of one run, drawn from the seed. Counts are
// bounded so that a correct executor can always make progress unless a test
// asks for a hard fault.
type simFaults struct {
	// churnPerPatch is the number of status-only resourceVersion bumps that land
	// between the caller's read and each of its patches (0..simMaxChurn).
	churnPerPatch func() int
	// failAfterCommit reports whether this patch commits and then returns a
	// transport error, as a request that times out after etcd accepted it.
	failAfterCommit func() bool
	// staleReads is how many cache snapshots after a write still show the
	// pre-write object.
	staleReads func() int
}

func drawSimFaults(rng *rand.Rand) simFaults {
	churnP, commitP, staleMax := rng.Float64(), 0.2*rng.Float64(), rng.IntN(4)
	return simFaults{
		churnPerPatch: func() int {
			if rng.Float64() < churnP {
				return 1 + rng.IntN(simMaxChurn)
			}
			return 0
		},
		failAfterCommit: func() bool { return rng.Float64() < commitP },
		staleReads:      func() int { return rng.IntN(staleMax + 1) },
	}
}

// errSimTimeout is the transport error of a request whose outcome is unknown.
var errSimTimeout = errors.New("simulated: etcdserver: request timed out")

// simKube is the simulated API server. All requests come from one goroutine.
type simKube struct {
	*serverPlanClient
	faults simFaults
	// beforeWrite runs before every patch is committed, so caches can capture
	// the pre-write state and go stale.
	beforeWrite []func()
	// patches counts Patch calls that reached the server.
	patches int
	// churned is set once a logical write has drawn its churn, until it commits.
	churned bool
	// churns counts status-only resourceVersion bumps injected so far.
	churns int
	// timeouts counts writes that committed but reported a transport error.
	timeouts int
}

func newSimKube(faults simFaults, objects ...*unstructured.Unstructured) *simKube {
	first, _ := newServerPlanClient(objects[0])
	kube := &simKube{serverPlanClient: first, faults: faults}
	for _, object := range objects[1:] {
		ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}
		object.SetUID(types.UID("uid-" + object.GetName()))
		object.SetResourceVersion("1")
		kube.objects[ref] = object
		kube.settle(object)
	}
	return kube
}

func (k *simKube) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	k.patches++
	if !k.churned {
		k.churned, k.raceBeforePatch = true, k.faults.churnPerPatch()
		k.churns += k.raceBeforePatch
	}
	for _, hook := range k.beforeWrite {
		hook()
	}
	err := k.serverPlanClient.Patch(ctx, ref, kind, patch)
	if err == nil {
		k.churned = false
		if k.faults.failAfterCommit() {
			k.timeouts++
			return errSimTimeout
		}
	}
	return err
}

// simCache is the informer-cache view of a simKube. After each write it keeps
// serving the pre-write objects for a seeded number of snapshots.
type simCache struct {
	kube      *simKube
	faults    simFaults
	pending   int
	frozen    sdktest.Snapshot
	snapshots int
}

func newSimCache(kube *simKube, faults simFaults) *simCache {
	cache := &simCache{kube: kube, faults: faults}
	kube.beforeWrite = append(kube.beforeWrite, cache.wrote)
	return cache
}

// wrote freezes the cache at the pre-write state for the next few snapshots.
func (c *simCache) wrote() {
	if c.pending > 0 {
		return
	}
	if n := c.faults.staleReads(); n > 0 {
		if snapshot, err := c.live(); err == nil {
			c.frozen, c.pending = snapshot, n
		}
	}
}

func (c *simCache) live() (sdktest.Snapshot, error) {
	snapshot := sdktest.Snapshot{NamespaceName: "app"}
	for ref, object := range c.kube.objects {
		switch ref.APIVersion + "/" + ref.Kind {
		case "v1/Service":
			var service corev1.Service
			if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &service); err != nil {
				return snapshot, err
			}
			snapshot.ServiceList = append(snapshot.ServiceList, service)
		case "apps/v1/Deployment":
			var deployment appsv1.Deployment
			if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &deployment); err != nil {
				return snapshot, err
			}
			snapshot.DeploymentList = append(snapshot.DeploymentList, deployment)
		}
	}
	return snapshot, nil
}

// Snapshot implements the SnapshotProvider the responder loop reads through.
func (c *simCache) Snapshot(context.Context) (sdk.DetectionContext, error) {
	c.snapshots++
	if c.pending > 0 {
		c.pending--
		return c.frozen, nil
	}
	return c.live()
}

// AwaitWrites implements WriteObserver: the cache catches up with the server.
func (c *simCache) AwaitWrites(context.Context, []ObservedWrite) error {
	c.pending = 0
	return nil
}
