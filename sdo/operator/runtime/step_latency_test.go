package runtime

import (
	"context"
	kclock "k8s.io/utils/clock"
	"sync"
	"time"


	"sdo.dev/operator/sdk"
)

// clearDetector records an evaluation without findings, like a health
// detector that watches Pods during a restart storm.
type clearDetector struct{ spec sdk.DetectorSpec }

func (d clearDetector) Spec() sdk.DetectorSpec { return d.spec }

func (clearDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

// absenceProbe reports the first evaluation that observes the ConfigMap gone.
type absenceProbe struct {
	configMapPresenceDetector
	once     *sync.Once
	observed chan time.Time
	clock    kclock.Clock
}

func (d absenceProbe) Detect(ctx context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := d.configMapPresenceDetector.Detect(ctx, snapshot)
	if len(findings) > 0 {
		d.once.Do(func() { d.observed <- d.clock.Now() })
	}
	return findings, err
}

type stormResult struct {
	detection time.Duration
	steps     int64
	apiCalls  int64
	// maxStep is the longest single loop iteration (StepEvents plus
	// PersistState), which bounds how long a new watch event can wait.
	maxStep time.Duration
}

