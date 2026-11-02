package runtime

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func (v *cachedReadView) LogsWithOptions(ref sdk.ObjectRef, options corev1.PodLogOptions) (string, error) {
	var r string
	err := v.read("LogsWithOptions", []any{ref, options}, &r, func() (any, error) {
		bounded, err := program.BoundedLogOptions(options)
		if err != nil {
			return "", err
		}
		data, err := call(context.Background(), 5*time.Second, func(context.Context) (string, error) { return v.ReadView.LogsWithOptions(ref, options) })
		if err == nil && int64(len(data)) > *bounded.LimitBytes {
			return "", fmt.Errorf("logs exceed byte limit")
		}
		return data, err
	})
	return r, err
}
func (v *cachedReadView) PodUsage(ref sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error) {
	var r *metricsv1beta1.PodMetrics
	err := v.read("PodUsage", []any{ref}, &r, func() (any, error) { return v.ReadView.PodUsage(ref) })
	return r, err
}
func (v *cachedReadView) NodeUsage(ref sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error) {
	var r *metricsv1beta1.NodeMetrics
	err := v.read("NodeUsage", []any{ref}, &r, func() (any, error) { return v.ReadView.NodeUsage(ref) })
	return r, err
}
