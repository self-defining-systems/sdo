package program

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
)

func (snapshotReadView) PodUsage(sdk.ObjectRef) ([]metrics.Usage, error) {
	return nil, &metrics.UsageUnavailableError{Reason: "detector context has no metrics reader"}
}
func (snapshotReadView) NodeUsage(sdk.ObjectRef) (metrics.Usage, error) {
	return metrics.Usage{}, &metrics.UsageUnavailableError{Reason: "detector context has no metrics reader"}
}
func (v *checkedReadView) PodUsage(ref sdk.ObjectRef) ([]metrics.Usage, error) {
	r, e := v.ReadView.PodUsage(ref)
	v.check(e)
	return r, e
}
func (v *checkedReadView) NodeUsage(ref sdk.ObjectRef) (metrics.Usage, error) {
	r, e := v.ReadView.NodeUsage(ref)
	v.check(e)
	return r, e
}
