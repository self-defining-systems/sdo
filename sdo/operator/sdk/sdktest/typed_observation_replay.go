package sdktest

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"strings"
)

func (v *ReplayReadView) LogsWithOptions(ref sdk.ObjectRef, container string, options program.LogOptions) (string, error) {
	var r string
	err := v.replay("LogsWithOptions", []any{ref, container, options}, &r)
	return r, err
}
func (v *ReplayReadView) PodUsage(ref sdk.ObjectRef) ([]metrics.Usage, error) {
	var r []metrics.Usage
	err := v.replay("PodUsage", []any{ref}, &r)
	if err != nil {
		return nil, &metrics.UsageUnavailableError{Reason: strings.TrimPrefix(err.Error(), "resource usage unavailable: ")}
	}
	return r, nil
}
func (v *ReplayReadView) NodeUsage(ref sdk.ObjectRef) (metrics.Usage, error) {
	var r metrics.Usage
	err := v.replay("NodeUsage", []any{ref}, &r)
	if err != nil {
		return r, &metrics.UsageUnavailableError{Reason: strings.TrimPrefix(err.Error(), "resource usage unavailable: ")}
	}
	return r, nil
}

func (s Snapshot) LogsWithOptions(ref sdk.ObjectRef, container string, options program.LogOptions) (string, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).LogsWithOptions(ref, container, options)
}
func (s Snapshot) PodUsage(ref sdk.ObjectRef) ([]metrics.Usage, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).PodUsage(ref)
}
func (s Snapshot) NodeUsage(ref sdk.ObjectRef) (metrics.Usage, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).NodeUsage(ref)
}
