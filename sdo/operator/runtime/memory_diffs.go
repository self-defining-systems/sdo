package runtime

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// memoryDiffInputs freezes the source and baseline diffs the memory host
// cannot compute itself: it has no cluster, source or API access and never
// calls back. The diffs are the view's own answers, so a repaired object reads
// as repaired and server defaults are normalized away exactly as for a program
// running in the controller. Asking the view, rather than the live view it
// wraps, means no wrapper can hide the diffs from the host; and answers come
// from the per-cycle read cache, so detectors and programs share one API
// round trip per object. Read tracking and recorders are skipped: a frozen
// read the program never made must neither fail its evaluation nor enter the
// cycle's recorded reads.
func memoryDiffInputs(view sdk.DetectionContext, refs []sdk.ObjectRef) []sdktest.PrimitiveCall {
	recorder := &sdktest.RecordingReadView{ReadView: program.ReadFrom(frozenReadSource(view))}
	for _, ref := range refs {
		_, _ = recorder.DiffAgainstSource(ref)
		_, _ = recorder.DiffAgainstBaseline(ref)
	}
	return recorder.SnapshotCalls()
}

// acceptedMemoryInputs freezes the accepted-memory reads a program may make:
// the application map and data checks of the accepted revision. Only the
// controller holds that revision; the memory host's live view reads the
// cluster, so without these a program asking for them would be answered by an
// empty recording. Failures are frozen too, so the program sees the
// controller's own error.
func acceptedMemoryInputs(view sdk.DetectionContext) []sdktest.PrimitiveCall {
	recorder := &sdktest.RecordingReadView{ReadView: program.ReadFrom(frozenReadSource(view))}
	_, _ = recorder.AppMap()
	_, _ = recorder.DataChecks()
	return recorder.SnapshotCalls()
}

// frozenReadSource is the view whose own answers are frozen for the host:
// read tracking and recorders are skipped, the per-cycle read cache is kept.
func frozenReadSource(view sdk.DetectionContext) sdk.DetectionContext {
	for {
		switch recorder := view.(type) {
		case interface{ Unchecked() program.ReadView }:
			view = recorder.Unchecked()
			continue
		case *CycleReadView:
			view = recorder.RecordingReadView.ReadView
			continue
		case *sdktest.RecordingReadView:
			view = recorder.ReadView
			continue
		}
		return view
	}
}
