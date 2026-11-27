package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kruntime "k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// slowSnapshotProvider is a cluster snapshot whose reads (pod reads, vantage
// probes) take cost of simulated time each.
type slowSnapshotProvider struct {
	SnapshotProvider
	clock     *fakeClock
	cost      time.Duration
	snapshots atomic.Int32
}

func (p *slowSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	p.snapshots.Add(1)
	p.clock.Advance(p.cost)
	return p.SnapshotProvider.Snapshot(ctx)
}

// stageFirstWrite runs the automatic stage on a credited learned playbook with
// a corpus session and snapshots that cost 5 s each, and returns the simulated
// time from the stage's start to the playbook's first write.
func stageFirstWrite(t *testing.T) (time.Duration, string, *slowSnapshotProvider) {
	t.Helper()
	world, stage, state := keyedStageLoop(t, credits("fix", keyedSymptom, "api"))
	clock := &fakeClock{}
	clock.Advance(time.Hour)
	loop := stage.NewLoop(state.Request)
	loop.Now = clock.Now
	root := t.TempDir()
	loop.CorpusSessionRoot = root
	provider := &slowSnapshotProvider{SnapshotProvider: world, clock: clock, cost: 5 * time.Second}
	loop.Provider = provider
	var firstWrite atomic.Int64
	world.client.PrependReactor("*", "deployments", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.GetSubresource() == "" && (action.GetVerb() == "update" || action.GetVerb() == "patch") {
			firstWrite.CompareAndSwap(0, clock.Now().UnixNano())
		}
		return false, nil, nil
	})
	started := clock.Now()
	if _, err := stage.Run(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if firstWrite.Load() == 0 || world.restarts.Load() != 1 {
		t.Fatalf("the credited playbook did not write once (writes %d)", world.restarts.Load())
	}
	dir, err := memory.IncidentCorpusDir(root, state.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	return time.Unix(0, firstWrite.Load()).Sub(started), dir, provider
}

// The automatic stage writes after exactly the two reads its decision needs:
// the incident's diagnosis and the fresh diagnosis it plans on. The pre-write
// capture reuses that second snapshot (nothing is written in between), and its
// source comparisons read the boundary's own snapshot instead of taking one
// each. Before, the stage took five snapshots before its first write (25 s
// here): a pre-apply snapshot and a source-comparison snapshot for the capture,
// and one more for the diagnosis's own source comparisons.
func TestStageWritesAfterOnlyTheSnapshotsItsDecisionNeeds(t *testing.T) {
	elapsed, _, provider := stageFirstWrite(t)
	t.Logf("stage start -> first write: %v (%d snapshots of %v in the stage)", elapsed, provider.snapshots.Load(), provider.cost)
	if want := 2 * provider.cost; elapsed > want {
		t.Fatalf("stage start -> first write = %v, want at most the diagnosis and the planning diagnosis (%v)", elapsed, want)
	}
}

// An agent may write between its plan and its apply, so its apply still takes
// a fresh pre-write snapshot for the capture; the capture's source comparisons
// read that same snapshot.
func TestAgentApplyTakesAFreshPreWriteSnapshot(t *testing.T) {
	world, stage, state := keyedStageLoop(t, credits("fix", keyedSymptom, "api"))
	clock := &fakeClock{}
	loop := stage.NewLoop(state.Request)
	loop.Now = clock.Now
	loop.CorpusSessionRoot = t.TempDir()
	provider := &slowSnapshotProvider{SnapshotProvider: world, clock: clock, cost: 5 * time.Second}
	loop.Provider = provider
	bindings := program.Bindings{Objects: map[string]sdk.ObjectRef{ComponentRole: {APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}}}
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "fix", Bindings: bindings})
	if planned.Error != "" {
		t.Fatal(planned.Error)
	}
	before := provider.snapshots.Load()
	var firstWrite atomic.Int32
	world.client.PrependReactor("*", "deployments", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.GetSubresource() == "" && (action.GetVerb() == "update" || action.GetVerb() == "patch") {
			firstWrite.CompareAndSwap(0, provider.snapshots.Load())
		}
		return false, nil, nil
	})
	// The synthetic rollout is still in progress after the write; only the
	// write matters here.
	_ = loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if world.restarts.Load() != 1 {
		t.Fatalf("the agent's apply wrote %d times, want once", world.restarts.Load())
	}
	if got := firstWrite.Load() - before; got != 1 {
		t.Fatalf("snapshots between the agent's plan and its write = %d, want one fresh pre-write snapshot", got)
	}
}

// The pre-apply capture is still complete and still pre-write: the snapshot
// file holds the object as the stage diagnosed it, and the source comparisons
// of the planned objects are recorded under the same boundary.
func TestStagePreApplyCaptureStaysCompleteAndPreWrite(t *testing.T) {
	_, dir, _ := stageFirstWrite(t)
	raw, err := os.ReadFile(filepath.Join(dir, "open@pre-apply.json"))
	if err != nil {
		t.Fatalf("no pre-apply capture: %v", err)
	}
	if strings.Contains(string(raw), "restartedAt") {
		t.Fatal("the pre-apply capture holds the playbook's own write")
	}
	if !strings.Contains(string(raw), `"api"`) {
		t.Fatalf("the pre-apply capture lacks the planned Deployment: %s", raw)
	}
	recordings, _ := filepath.Glob(filepath.Join(dir, "open@pre-apply*"))
	if len(recordings) < 2 {
		t.Fatalf("pre-apply recordings = %v, want the snapshot and its source comparisons", recordings)
	}
}
