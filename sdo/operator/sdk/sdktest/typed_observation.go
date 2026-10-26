package sdktest

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
)

func (v *RecordingReadView) LogsWithOptions(ref sdk.ObjectRef, container string, options program.LogOptions) (string, error) {
	r, err := v.ReadView.LogsWithOptions(ref, container, options)
	v.record("LogsWithOptions", []any{ref, container, options}, r, err)
	return r, err
}
func (v *RecordingReadView) PodUsage(ref sdk.ObjectRef) ([]metrics.Usage, error) {
	r, err := v.ReadView.PodUsage(ref)
	v.record("PodUsage", []any{ref}, r, err)
	return r, err
}
func (v *RecordingReadView) NodeUsage(ref sdk.ObjectRef) (metrics.Usage, error) {
	r, err := v.ReadView.NodeUsage(ref)
	v.record("NodeUsage", []any{ref}, r, err)
	return r, err
}
