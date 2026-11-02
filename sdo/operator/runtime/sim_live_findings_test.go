package runtime

import (
	"context"

	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
)

func simMemoryStructureValidator(ctx context.Context, path string, _ []string) error {
	diagnostics, err := validate.Validate(ctx, validate.Request{Root: path, Layers: validate.Structural})
	if err != nil {
		return err
	}
	return validate.Err(diagnostics)
}

// Shared schedule events operate on the simulated API rather than inserting
// detector findings. The controller remains responsible for classification.
type simScheduleEventKind string

const (
	simPause                 simScheduleEventKind = "maintenance-pause"
	simResume                simScheduleEventKind = "maintenance-resume"
	simForeignPodCreate      simScheduleEventKind = "foreign-pod-create"
	simForeignPodDelete      simScheduleEventKind = "foreign-pod-delete"
	simRestartFailingTraffic simScheduleEventKind = "restart-failing-traffic"
	simRestartChangedScope   simScheduleEventKind = "restart-changed-closure-scope"
)

type simScheduleEvent struct {
	tick int
	kind simScheduleEventKind
}

var simForeignPodRef = sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "foreign-probe"}

// The scope changes independently of the API objects and persisted closure.
type simChangedScopeProvider struct {
	SnapshotProvider
	generation string
}

type simChangedScopeSnapshot struct {
	sdk.DetectionContext
	sdk.ScopedObservationSource
}

func (p simChangedScopeProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	snapshot, err := p.SnapshotProvider.Snapshot(ctx)
	return simChangedScopeSnapshot{DetectionContext: snapshot, ScopedObservationSource: sdk.ScopedSnapshot{Version: 1, Generation: p.generation}}, err
}
