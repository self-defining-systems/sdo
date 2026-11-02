package runtime

import (
	"errors"


	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// failingDiffView fails the source comparison of one object, as a dry run
// that succeeded during the adoption scan but fails during classification.
type failingDiffView struct {
	program.ReadView
	ref sdk.ObjectRef
}

func (v failingDiffView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	if ref == v.ref {
		return program.Diff{}, errors.New("webhook timeout")
	}
	return v.ReadView.DiffAgainstSource(ref)
}

