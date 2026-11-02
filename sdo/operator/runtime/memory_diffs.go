package runtime

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// MemoryDiffInputs freezes read-only diff results. In particular, source
// comparison never enters the Kubernetes dry-run PATCH normalization path.
func (v LiveReadView) MemoryDiffInputs(refs []sdk.ObjectRef) []sdktest.PrimitiveCall {
	v.Client = nil
	if v.BaselineSource != nil {
		v.Baseline = v.BaselineSource.Changes(inputs.New(v).ObservedAt())
		v.BaselineSource = nil
	}
	recorder := &sdktest.RecordingReadView{ReadView: v}
	for _, ref := range refs {
		_, _ = recorder.DiffAgainstSource(ref)
		_, _ = recorder.DiffAgainstBaseline(ref)
	}
	return recorder.SnapshotCalls()
}
func memoryDiffInputs(view any, refs []sdk.ObjectRef) []sdktest.PrimitiveCall {
	for view != nil {
		if provider, ok := view.(interface {
			MemoryDiffInputs([]sdk.ObjectRef) []sdktest.PrimitiveCall
		}); ok {
			return provider.MemoryDiffInputs(refs)
		}
		// Each wrapper the controller layers over the live view (read cache, onboarding reachability, frozen
		// metrics, traffic and other input carriers) only adds frozen inputs; the diffs come from what it wraps.
		switch wrapper := view.(type) {
		case *cachedReadView:
			view = wrapper.ReadView
		case onboardingReadView:
			view = wrapper.ReadView
		case metricsContext:
			view = wrapper.Carrier.DetectionContext
		case trafficContext:
			view = wrapper.Carrier.DetectionContext
		case inputs.Carrier:
			view = wrapper.DetectionContext
		case *inputs.Carrier:
			view = wrapper.DetectionContext
		default:
			underlying, ok := view.(interface{ UnderlyingReadView() program.ReadView })
			if !ok {
				return nil
			}
			view = underlying.UnderlyingReadView()
		}
	}
	return nil
}
