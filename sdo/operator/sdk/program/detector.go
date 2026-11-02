package program

import (
	"fmt"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"time"
)

type snapshotReadView struct{ inputs.Carrier }

func (snapshotReadView) Logs(sdk.ObjectRef, string, time.Duration) (string, error) {
	return "", fmt.Errorf("logs unavailable in detector context")
}
func (snapshotReadView) ProbeTCP(sdk.ObjectRef, int32) (ProbeResult, error) {
	return ProbeResult{}, fmt.Errorf("TCP probe unavailable in detector context")
}
func (snapshotReadView) ProbeHTTP(sdk.ObjectRef, HTTPProbe) (ProbeResult, error) {
	return ProbeResult{}, fmt.Errorf("HTTP probe unavailable in detector context")
}
func (snapshotReadView) DiffAgainstBaseline(sdk.ObjectRef) (Diff, error) {
	return Diff{}, fmt.Errorf("baseline diff unavailable in detector context")
}
