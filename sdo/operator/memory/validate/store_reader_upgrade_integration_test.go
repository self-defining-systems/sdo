//go:build integration

package validate

import (
	"context"
	"testing"

	"sdo.dev/operator/memory"
)

// upgradeStoreMap maps one redis store, which Go reads by default.
const upgradeStoreMap = "# Architecture\n\n<!-- sdo:application-map:begin -->\n```yaml\nversion: 1\ncomponents:\n  - {name: kv, workload: StatefulSet/kv, services: [kv], holds_data: true, data: {engine: redis, role: authoritative}}\nedges: []\noperations: []\nprobes: []\n```\n<!-- sdo:application-map:end -->\n"

// upgradeStoreReader is a designer-written reader of that store, accepted
// before Go read every store, with its own interval.
const upgradeStoreReader = `package kvread

import (
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/storestate"
)

func New() sdk.Detector {
	d, err := storestate.NewDetector(storestate.Config{
		Spec: sdk.DetectorSpec{
			ID:                "store-cache-kv",
			Class:             sdk.DetectorClassHealth,
			Owner:             sdk.DetectorOwnerHealthJudge,
			Persistence:       sdk.PersistencePolicy{Firing: 2, Clearing: 2},
			Batching:          sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
			Playbooks:         []string{"health-objective"},
			OriginatingCommit: "initial",
			Watches:           []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}},
			Interval:          60 * time.Second,
		},
		Component:          "kv",
		Engine:             storestate.Engine("redis"),
		Target:             podread.Target{Workload: "StatefulSet/kv"},
		ReadInterval:       60 * time.Second,
		GapWithoutBaseline: true,
	})
	if err != nil {
		panic(err)
	}
	return d
}
`

const upgradeStoreReaderManifest = `apiVersion: sdo.dev/v1alpha1
kind: DetectorManifest
sdkVersion: v0.1
id: store-cache-kv
constructor: New
class: health
owner: health_judge
watches:
    - apiVersion: v1
      kind: Pod
interval: 60s
persistence:
    firing: 2
    clearing: 2
batching:
    severity: critical
    debounce: 500ms
possiblePlaybooks:
    - health-objective
originatingCommit: initial
`

// Memory whose designer reads a store itself loads in the current controller
// after Go prepares it: Go adds no second reader of that store.
func TestDesignerStoreReaderMemoryStartsAfterUpgrade(t *testing.T) {
	if testing.Short() {
		t.Skip("real go toolchain")
	}
	root := compileTree(t)
	write(t, root, ".sdo/arch.md", upgradeStoreMap)
	write(t, root, ".sdo/diagnostics/detectors/health/store-cache-kv/detector.go", upgradeStoreReader)
	write(t, root, ".sdo/diagnostics/detectors/health/store-cache-kv/manifest.yaml", upgradeStoreReaderManifest)
	if err := memory.PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	got, err := CompileCandidate(context.Background(), compileRequest(root))
	if err != nil || len(got) != 0 {
		t.Fatalf("rejected: %v, %v", got, err)
	}
}
