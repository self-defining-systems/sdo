package runtime

import (
	"sdo.dev/operator/sdk"
)

type fixedSourceImages struct{ image, tree string }

func (i fixedSourceImages) BaselineImage(sdk.ObjectRef, string, bool) (string, bool) {
	return i.image, true
}

func (i fixedSourceImages) BaselineTree(sdk.ObjectRef) string { return i.tree }
