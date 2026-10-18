package core

import (
	"context"
	"fmt"

	"sdo.dev/operator/sdk"
)

type SnapshotProvider interface {
	Snapshot(context.Context) (sdk.DetectionContext, error)
}

type Sink interface {
	Emit(context.Context, sdk.Finding) error
}

type Runner struct {
	Detectors []sdk.Detector
	Provider  SnapshotProvider
	Sink      Sink
}

func (r Runner) RunOnce(ctx context.Context) error {
	if err := ValidateDetectors(r.Detectors); err != nil {
		return err
	}
	if r.Provider == nil {
		return fmt.Errorf("snapshot provider is required")
	}
	if r.Sink == nil {
		return fmt.Errorf("finding sink is required")
	}

	snapshot, err := r.Provider.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("create detection snapshot: %w", err)
	}

	for _, detector := range r.Detectors {
		spec := detector.Spec()
		findings, err := detector.Detect(ctx, snapshot)
		if err != nil {
			return fmt.Errorf("run detector %q: %w", spec.ID, err)
		}
		for _, finding := range findings {
			if finding.DetectorID == "" {
				finding.DetectorID = spec.ID
			}
			if err := ValidateFinding(spec, finding); err != nil {
				return fmt.Errorf("validate detector %q finding: %w", spec.ID, err)
			}
			if err := r.Sink.Emit(ctx, finding); err != nil {
				return fmt.Errorf("emit detector %q finding: %w", spec.ID, err)
			}
		}
	}
	return nil
}

type StaticSnapshotProvider struct {
	SnapshotValue sdk.DetectionContext
}

func (p StaticSnapshotProvider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	if p.SnapshotValue == nil {
		return nil, fmt.Errorf("snapshot value is required")
	}
	return p.SnapshotValue, nil
}
