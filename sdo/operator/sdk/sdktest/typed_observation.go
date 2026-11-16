package sdktest

import (
	corev1 "k8s.io/api/core/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"time"
)

func (v *RecordingReadView) LogsWithOptions(ref sdk.ObjectRef, options corev1.PodLogOptions) (string, error) {
	r, err := v.ReadView.LogsWithOptions(ref, options)
	v.record("LogsWithOptions", []any{ref, options}, r, err)
	return r, err
}
func (v *RecordingReadView) PodUsage(ref sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error) {
	r, err := v.ReadView.PodUsage(ref)
	v.record("PodUsage", []any{ref}, r, err)
	return r, err
}
func (v *RecordingReadView) NodeUsage(ref sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error) {
	r, err := v.ReadView.NodeUsage(ref)
	v.record("NodeUsage", []any{ref}, r, err)
	return r, err
}
func (v *RecordingReadView) LogMatches(ref sdk.ObjectRef, container string, since time.Duration, pattern string, max int) (program.LogMatchResult, error) {
	r, err := v.ReadView.LogMatches(ref, container, since, pattern, max)
	v.record("LogMatches", []any{ref, container, since, pattern, max}, r, err)
	return r, err
}
