package program

import (
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
)

func (snapshotReadView) PodUsage(sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error) {
	return nil, &metrics.UsageUnavailableError{Reason: "detector context has no metrics reader"}
}
func (snapshotReadView) NodeUsage(sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error) {
	return nil, &metrics.UsageUnavailableError{Reason: "detector context has no metrics reader"}
}
func (v *checkedReadView) PodUsage(ref sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error) {
	r, e := v.ReadView.PodUsage(ref)
	v.check(e)
	return r, e
}
func (v *checkedReadView) NodeUsage(ref sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error) {
	r, e := v.ReadView.NodeUsage(ref)
	v.check(e)
	return r, e
}
