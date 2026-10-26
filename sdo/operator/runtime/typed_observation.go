package runtime

import (
	"context"
	"fmt"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"time"
)

func (v *cachedReadView) LogsWithOptions(ref sdk.ObjectRef, container string, options program.LogOptions) (string, error) {
	var r string
	err := v.read("LogsWithOptions", []any{ref, container, options}, &r, func() (any, error) {
		if err := options.Validate(); err != nil {
			return "", err
		}
		data, err := call(context.Background(), 5*time.Second, func(context.Context) (string, error) { return v.ReadView.LogsWithOptions(ref, container, options) })
		limit := options.LimitBytes
		if limit == 0 {
			limit = 64 * 1024
		}
		if err == nil && int64(len(data)) > limit {
			return "", fmt.Errorf("logs exceed byte limit")
		}
		return data, err
	})
	return r, err
}
func (v *cachedReadView) PodUsage(ref sdk.ObjectRef) ([]metrics.Usage, error) {
	var r []metrics.Usage
	err := v.read("PodUsage", []any{ref}, &r, func() (any, error) { return v.ReadView.PodUsage(ref) })
	return r, err
}
func (v *cachedReadView) NodeUsage(ref sdk.ObjectRef) (metrics.Usage, error) {
	var r metrics.Usage
	err := v.read("NodeUsage", []any{ref}, &r, func() (any, error) { return v.ReadView.NodeUsage(ref) })
	return r, err
}
