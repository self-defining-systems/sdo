package memoryhost

import (
	"errors"
	"slices"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// diffReadView consumes only immutable inputs the controller froze (object
// diffs and accepted-memory reads), never controller callbacks.
type diffReadView struct {
	program.ReadView
	diffs *sdktest.ReplayReadView
	// snapshot is the controller's frozen snapshot as the host holds it: its
	// objects and the kinds it lists completely decide absence.
	snapshot sdktest.Snapshot
}

// AcceptedRefsMethod names the receipt that freezes the accepted inventory. The
// controller writes it (runtime.absenceInputs); it is a host input, never a
// program-visible read.
const AcceptedRefsMethod = "AcceptedRefs"

func (v diffReadView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	diff, err := v.diffs.DiffAgainstSource(ref)
	var unrecorded *sdktest.UnrecordedError
	if errors.As(err, &unrecorded) && v.undeclaredAndAbsent(ref) {
		return program.Diff{SourceAbsent: true, LiveAbsent: true}, nil
	}
	return diff, err
}

// undeclaredAndAbsent reports that ref is observed missing live (a complete list
// of its kind lacks it) and the frozen accepted inventory does not declare it:
// the same answer the controller's live view gives. A declared object that is
// gone had its diff frozen; anything else is unknown to the host, never absent.
func (v diffReadView) undeclaredAndAbsent(ref sdk.ObjectRef) bool {
	if ref.Namespace != v.snapshot.NamespaceName || !sdk.KindComplete(v.snapshot.CompleteKindList, ref) {
		return false
	}
	if _, err := v.snapshot.Object(ref); !errors.Is(err, sdk.ErrObjectAbsent) {
		return false
	}
	var declared []sdk.ObjectRef
	if err := v.diffs.Recorded(AcceptedRefsMethod, []any{}, &declared); err != nil {
		return false
	}
	return !slices.Contains(declared, ref)
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
