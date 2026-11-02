package runtime

import (
	"sync/atomic"

	"sdo.dev/operator/sdk"
)

// Pause a real scheduler operation, rather than relying on random goroutine
// timing, while the dispatch goroutine tries to persist its stage handoff.
type stageHandoffDetector struct {
	sdk.Detector
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (d *stageHandoffDetector) Spec() sdk.DetectorSpec {
	if d.armed.CompareAndSwap(true, false) {
		close(d.entered)
		<-d.release
	}
	return d.Detector.Spec()
}
