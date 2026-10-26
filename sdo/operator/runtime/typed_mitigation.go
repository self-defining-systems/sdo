package runtime

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
)

func (v mitigationReadView) LogsWithOptions(ref sdk.ObjectRef, container string, options program.LogOptions) (string, error) {
	var zero string
	if err := v.ctx.Err(); err != nil {
		return zero, err
	}
	return v.live.LogsWithOptions(ref, container, options)
}
func (v mitigationReadView) PodUsage(ref sdk.ObjectRef) ([]metrics.Usage, error) {
	var zero []metrics.Usage
	if err := v.ctx.Err(); err != nil {
		return zero, err
	}
	return v.live.PodUsage(ref)
}
func (v mitigationReadView) NodeUsage(ref sdk.ObjectRef) (metrics.Usage, error) {
	var zero metrics.Usage
	if err := v.ctx.Err(); err != nil {
		return zero, err
	}
	return v.live.NodeUsage(ref)
}
