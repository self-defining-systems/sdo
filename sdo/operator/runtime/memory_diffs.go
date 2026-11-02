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
		break
	}
	recorder := &sdktest.RecordingReadView{ReadView: program.ReadFrom(view)}
	for _, ref := range refs {
		_, _ = recorder.DiffAgainstSource(ref)
		_, _ = recorder.DiffAgainstBaseline(ref)
	}
	return recorder.SnapshotCalls()
}
