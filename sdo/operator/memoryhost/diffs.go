package memoryhost

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// diffReadView consumes only immutable inputs the controller froze (object
// diffs and accepted-memory reads), never controller callbacks.
type diffReadView struct {
	program.ReadView
	diffs *sdktest.ReplayReadView
}

func (v diffReadView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	return v.diffs.DiffAgainstSource(ref)
}
func (v diffReadView) DiffAgainstBaseline(ref sdk.ObjectRef) (program.Diff, error) {
	return v.diffs.DiffAgainstBaseline(ref)
}

// AppMap and DataChecks come from the accepted revision, which only the
// controller holds; the host's live view would otherwise answer them from an
// empty recording.
func (v diffReadView) AppMap() (program.AppMap, error) { return v.diffs.AppMap() }
func (v diffReadView) DataChecks() (map[string]string, error) {
	return v.diffs.DataChecks()
}
