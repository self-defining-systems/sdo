package program

import (
	"fmt"
	"sdo.dev/operator/sdk"
	"time"
)

type snapshotReadView struct {
	sdk.DetectionContext
	sdk.ScopedObservationSource
}

func (snapshotReadView) Logs(sdk.ObjectRef, string, time.Duration) (string, error) {
	return "", fmt.Errorf("logs unavailable in detector context")
}
func (snapshotReadView) ProbeTCP(sdk.ObjectRef, int32) (ProbeResult, error) {
	return ProbeResult{}, fmt.Errorf("TCP probe unavailable in detector context")
}
func (snapshotReadView) ProbeHTTP(sdk.ObjectRef, HTTPProbe) (ProbeResult, error) {
	return ProbeResult{}, fmt.Errorf("HTTP probe unavailable in detector context")
}
func (snapshotReadView) DiffAgainstSource(sdk.ObjectRef) (Diff, error) {
	return Diff{}, fmt.Errorf("source diff unavailable in detector context")
}
func (snapshotReadView) DiffAgainstBaseline(sdk.ObjectRef) (Diff, error) {
	return Diff{}, fmt.Errorf("baseline diff unavailable in detector context")
}
