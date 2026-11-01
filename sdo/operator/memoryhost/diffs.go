package memoryhost

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// diffReadView consumes only immutable diff inputs, never controller callbacks.
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
