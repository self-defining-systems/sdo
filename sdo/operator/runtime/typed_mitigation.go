package runtime

import (
	corev1 "k8s.io/api/core/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/sdk"
)

func (v mitigationReadView) LogsWithOptions(ref sdk.ObjectRef, options corev1.PodLogOptions) (string, error) {
	var zero string
	if err := v.ctx.Err(); err != nil {
		return zero, err
	}
	return v.live.LogsWithOptions(ref, options)
}
func (v mitigationReadView) PodUsage(ref sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error) {
	var zero *metricsv1beta1.PodMetrics
	if err := v.ctx.Err(); err != nil {
		return zero, err
	}
	return v.live.PodUsage(ref)
}
func (v mitigationReadView) NodeUsage(ref sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error) {
	var zero *metricsv1beta1.NodeMetrics
	if err := v.ctx.Err(); err != nil {
		return zero, err
	}
	return v.live.NodeUsage(ref)
}
