package runtime

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func (v *cachedReadView) LogsWithOptions(ref sdk.ObjectRef, options corev1.PodLogOptions) (string, error) {
	var r string
	err := v.read("LogsWithOptions", []any{ref, options}, &r, func() (any, error) {
		_, limit, err := program.BoundedLogOptions(options)
		if err != nil {
			return "", err
		}
		data, err := call(context.Background(), 5*time.Second, func(context.Context) (string, error) { return v.ReadView.LogsWithOptions(ref, options) })
		return program.NewestLogBytes(data, limit), err
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

func (v *cachedReadView) LogMatches(ref sdk.ObjectRef, container string, since time.Duration, pattern string, max int) (program.LogMatchResult, error) {
	var r program.LogMatchResult
	err := v.read("LogMatches", []any{ref, container, since, pattern, max}, &r, func() (any, error) {
		return call(context.Background(), 15*time.Second, func(context.Context) (program.LogMatchResult, error) {
			return v.ReadView.LogMatches(ref, container, since, pattern, max)
		})
	})
	return r, err
}
