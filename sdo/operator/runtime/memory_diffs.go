package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// memoryDiffInputs freezes the source and baseline diffs watched by a detector.
// It uses the controller's cycle cache, so detectors and programs share the
// same normalized result. Exclude these speculative reads from evidence and
// read tracking: a detector may never ask the host for them.
func memoryDiffInputs(view sdk.DetectionContext, refs []sdk.ObjectRef) []sdktest.PrimitiveCall {
	recorder := &sdktest.RecordingReadView{ReadView: program.ReadFrom(frozenReadSource(view))}
	for _, ref := range refs {
		_, _ = recorder.DiffAgainstSource(ref)
		_, _ = recorder.DiffAgainstBaseline(ref)
	}
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

// maxFrozenMissing bounds the declared-but-missing objects whose diffs are
// frozen for one host call. Over it nothing is frozen and the host answers
// those objects as unavailable: the set is never truncated silently.
const maxFrozenMissing = 256

// absenceInputs freezes what the memory host needs to answer for objects the
// snapshot does not hold, because the host never calls back: the accepted
// inventory (one receipt) and the source diff of every declared object that is
// absent live. Objects absent live and undeclared need no diff; the host answers
// them from the inventory and the complete kinds. frozen is the set of refs
// whose diffs are already frozen (the snapshot's and the bound ones); want, when
// set, limits the declared-missing diffs to the refs the caller may inspect. Without an accepted source nothing is frozen and the host
// keeps answering such objects as unavailable.
func absenceInputs(view sdk.DetectionContext, frozen map[sdk.ObjectRef]bool, want func(sdk.ObjectRef) bool) ([]*pb.MemoryReadReceipt, error) {
	source, lister := acceptedRefLister(view)
	if lister == nil {
		return nil, nil
	}
	refs, listErr := acceptedRefs(view, lister)
	call := sdktest.PrimitiveCall{Method: memoryhost.AcceptedRefsMethod, Arguments: []byte("[]")}
	if listErr != nil {
		call.Result, call.Error, call.ErrorClass = []byte("null"), listErr.Error(), sdk.ReadErrorClass(listErr)
	} else {
		encoded, err := json.Marshal(refs)
		if err != nil {
			return nil, err
		}
		call.Result = encoded
	}
	receipts, err := memoryhost.EncodeReceipts([]sdktest.PrimitiveCall{call})
	if err != nil || listErr != nil {
		return receipts, err
	}
	complete := sdk.CompleteKinds(source)
	var missing []sdk.ObjectRef
	for _, ref := range refs {
		if ref.Namespace == view.Namespace() && !frozen[ref] && (want == nil || want(ref)) && sdk.KindComplete(complete, ref) {
			missing = append(missing, ref)
		}
	}
	if len(missing) == 0 || len(missing) > maxFrozenMissing {
		return receipts, nil
	}
	sort.Slice(missing, func(i, j int) bool { return fmt.Sprint(missing[i]) < fmt.Sprint(missing[j]) })
	diffs, err := frozenMemoryReads(view, missing)
	if err != nil {
		return nil, err
	}
	return append(receipts, diffs...), nil
}

// acceptedRefs reads the accepted inventory once per cycle: listing it renders
// accepted source, and every program of the cycle needs the same answer.
func acceptedRefs(view sdk.DetectionContext, lister interface {
	AcceptedRefs(context.Context) ([]sdk.ObjectRef, error)
}) ([]sdk.ObjectRef, error) {
	read := func() ([]sdk.ObjectRef, error) {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultDiagnosisDeadline)
		defer cancel()
		return lister.AcceptedRefs(ctx)
	}
	cycle := memoryCycle(view)
	if cycle == nil {
		return read()
	}
	p := &cycle.memory
	p.acceptedOnce.Do(func() { p.accepted, p.acceptedErr = read() })
	return append([]sdk.ObjectRef(nil), p.accepted...), p.acceptedErr
}

// acceptedRefLister finds the live view beneath the cycle's wrappers that can
// list the accepted inventory, and returns it with its own lister.
func acceptedRefLister(view sdk.DetectionContext) (sdk.DetectionContext, interface {
	AcceptedRefs(context.Context) ([]sdk.ObjectRef, error)
}) {
	source := frozenReadSource(view)
	for source != nil {
		if lister, ok := source.(interface {
			AcceptedRefs(context.Context) ([]sdk.ObjectRef, error)
		}); ok {
			return source, lister
		}
		wrapper, ok := source.(interface{ UnderlyingReadView() program.ReadView })
		if !ok {
			return nil, nil
		}
		source = wrapper.UnderlyingReadView()
	}
	return nil, nil
}
