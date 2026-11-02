package sdktest

import (
	"encoding/json"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
)

type metricInput struct {
	Result    metrics.Result `json:"result"`
	Available bool           `json:"available"`
}
type alertInput struct {
	Result    metrics.AlertsResult `json:"result"`
	Available bool                 `json:"available"`
}
type trafficInput struct {
	Result    traffic.Window `json:"result"`
	Available bool           `json:"available"`
}

// RecordedReads returns a detached, concurrency-safe copy of runtime evidence.
func (v *RecordingReadView) RecordedReads() []PrimitiveCall {
	v.mu.Lock()
	defer v.mu.Unlock()
	data, _ := json.Marshal(v.Calls)
	var calls []PrimitiveCall
	_ = json.Unmarshal(data, &calls)
	return calls
}
func (v *RecordingReadView) MetricResult(id string) (metrics.Result, bool) {
	r, ok := v.ReadView.MetricResult(id)
	v.record("MetricResult", []any{id}, metricInput{r, ok}, nil)
	return r, ok
}
func (v *RecordingReadView) AlertsResult() (metrics.AlertsResult, bool) {
	r, ok := v.ReadView.AlertsResult()
	v.record("AlertsResult", []any{}, alertInput{r, ok}, nil)
	return r, ok
}
func (v *RecordingReadView) QueryMetrics(q metrics.Query) metrics.Result {
	r := v.ReadView.QueryMetrics(q)
	v.record("QueryMetrics", []any{q}, r, nil)
	return r
}
func (v *RecordingReadView) TrafficWindow(name string) (traffic.Window, bool) {
	r, ok := v.ReadView.TrafficWindow(name)
	v.record("TrafficWindow", []any{name}, trafficInput{r, ok}, nil)
	return r, ok
}
func (v *ReplayReadView) QueryMetrics(q metrics.Query) metrics.Result {
	// Frozen snapshot telemetry is reusable; primitive-only older recordings
	// are consumed normally when the snapshot has no copy.
	if r, ok := v.Snapshot.Metrics[q.ID]; ok && r.Query == q {
		return r
	}
	var r metrics.Result
	if err := v.replay("QueryMetrics", []any{q}, &r); err != nil {
		return metrics.Result{Query: q, Error: err.Error()}
	}
	return r
}
func (v *ReplayReadView) MetricResult(id string) (metrics.Result, bool) {
	if r, ok := v.Snapshot.MetricResult(id); ok {
		return r, true
	}
	var r metricInput
	if v.replay("MetricResult", []any{id}, &r) != nil {
		return metrics.Result{}, false
	}
	return r.Result, r.Available
}
func (v *ReplayReadView) AlertsResult() (metrics.AlertsResult, bool) {
	if r, ok := v.Snapshot.AlertsResult(); ok {
		return r, true
	}
	var r alertInput
	if v.replay("AlertsResult", []any{}, &r) != nil {
		return metrics.AlertsResult{}, false
	}
	return r.Result, r.Available
}
func (v *ReplayReadView) TrafficWindow(name string) (traffic.Window, bool) {
	if r, ok := v.Snapshot.TrafficWindow(name); ok {
		return r, true
	}
	var r trafficInput
	if v.replay("TrafficWindow", []any{name}, &r) != nil {
		return traffic.Window{}, false
	}
	return r.Result, r.Available
}

// captureReadInputs keeps telemetry accessible through the existing frozen
// accessors as well as the primitive record. It performs no source reads.
func (s *Snapshot) captureReadInputs(calls []PrimitiveCall) {
	s.Readings = calls
	for _, c := range calls {
		var args []json.RawMessage
		if json.Unmarshal(c.Arguments, &args) != nil {
			continue
		}
		switch c.Method {
		case "MetricResult":
			var r metricInput
			var id string
			if len(args) == 1 && json.Unmarshal(args[0], &id) == nil && json.Unmarshal(c.Result, &r) == nil && r.Available {
				if s.Metrics == nil {
					s.Metrics = map[string]metrics.Result{}
				}
				s.Metrics[id] = r.Result
			}
		case "QueryMetrics":
			var r metrics.Result
			if json.Unmarshal(c.Result, &r) == nil {
				if s.Metrics == nil {
					s.Metrics = map[string]metrics.Result{}
				}
				s.Metrics[r.Query.ID] = r
			}
		case "ServiceReachability":
			var r reachabilityInput
			if json.Unmarshal(c.Result, &r) == nil && r.Available {
				s.Reachability = &r.Result
			}
		case "AlertsResult":
			var r alertInput
			if json.Unmarshal(c.Result, &r) == nil && r.Available {
				s.Alerts = &r.Result
			}
		case "TrafficWindow":
			var r trafficInput
			var name string
			if len(args) == 1 && json.Unmarshal(args[0], &name) == nil && json.Unmarshal(c.Result, &r) == nil && r.Available {
				if s.Traffic == nil {
					s.Traffic = map[string]traffic.Window{}
				}
				s.Traffic[name] = r.Result
			}
		}
	}
}

type reachabilityInput struct {
	Result    servicehealth.ReachabilityWindow `json:"result"`
	Available bool                             `json:"available"`
}

func (v *ReplayReadView) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	if r, ok := v.Snapshot.ServiceReachability(); ok {
		return r, true
	}
	var r reachabilityInput
	if v.replay("ServiceReachability", []any{}, &r) != nil {
		return servicehealth.ReachabilityWindow{}, false
	}
	return r.Result, r.Available
}

func (v *RecordingReadView) ObservedView() (sdk.DetectionContext, func() error) {
	return program.CheckReads(v)
}
func (v *ReplayReadView) ObservedView() (sdk.DetectionContext, func() error) {
	return program.CheckReads(v)
}
