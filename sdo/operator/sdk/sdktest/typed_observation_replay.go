package sdktest

import (
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
)

func (v *ReplayReadView) LogsWithOptions(ref sdk.ObjectRef, options corev1.PodLogOptions) (string, error) {
	var r string
	err := v.replay("LogsWithOptions", []any{ref, options}, &r)
	if err != nil {
		return "", err
	}
	_, limit, err := program.BoundedLogOptions(options)
	if err != nil {
		return "", err
	}
	return program.NewestLogBytes(r, limit), nil
}
func (v *ReplayReadView) PodUsage(ref sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error) {
	var r *metricsv1beta1.PodMetrics
	err := v.replay("PodUsage", []any{ref}, &r)
	if err == nil {
		err = metrics.ValidatePodUsage(ref, r)
	}
	if err != nil {
		return nil, &metrics.UsageUnavailableError{Reason: strings.TrimPrefix(err.Error(), "resource usage unavailable: ")}
	}
	return r, nil
}
func (v *ReplayReadView) NodeUsage(ref sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error) {
	var r *metricsv1beta1.NodeMetrics
	err := v.replay("NodeUsage", []any{ref}, &r)
	if err == nil {
		err = metrics.ValidateNodeUsage(ref, r)
	}
	if err != nil {
		return nil, &metrics.UsageUnavailableError{Reason: strings.TrimPrefix(err.Error(), "resource usage unavailable: ")}
	}
	return r, nil
}
func (s Snapshot) LogsWithOptions(ref sdk.ObjectRef, options corev1.PodLogOptions) (string, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).LogsWithOptions(ref, options)
}
func (s Snapshot) PodUsage(ref sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).PodUsage(ref)
}
func (s Snapshot) NodeUsage(ref sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).NodeUsage(ref)
}
func (v *ReplayReadView) LogMatches(ref sdk.ObjectRef, container string, since time.Duration, pattern string, max int) (program.LogMatchResult, error) {
	if _, err := program.ValidateLogMatchRequest(since, pattern, max); err != nil {
		return program.LogMatchResult{}, err
	}
	var r program.LogMatchResult
	err := v.replay("LogMatches", []any{ref, container, since, pattern, max}, &r)
	return r, err
}
func (s Snapshot) LogMatches(ref sdk.ObjectRef, container string, since time.Duration, pattern string, max int) (program.LogMatchResult, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).LogMatches(ref, container, since, pattern, max)
}
