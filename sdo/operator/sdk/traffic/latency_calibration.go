package traffic

import "time"

// LatencyCalibrationFactor and MinCalibratedLatency set the latency bound SDO
// calibrates for a health-probe scenario whose workload sets none: three
// times its slowest healthy run, at least MinCalibratedLatency. The margin
// keeps an ordinary slow moment green, while a path slowed several times over
// fires.
const (
	LatencyCalibrationFactor = 3
	MinCalibratedLatency     = time.Second
	calibrationStep          = 100 * time.Millisecond
)

// CalibratedMaxLatency is the bound for a scenario whose slowest healthy run
// took slowest: LatencyCalibrationFactor times it, at least
// MinCalibratedLatency, rounded up to 100ms and at most iterationTimeout.
func CalibratedMaxLatency(slowest, iterationTimeout time.Duration) time.Duration {
	bound := max(LatencyCalibrationFactor*slowest, MinCalibratedLatency)
	bound = (bound + calibrationStep - 1) / calibrationStep * calibrationStep
	if iterationTimeout > 0 {
		bound = min(bound, iterationTimeout)
	}
	return bound
}

// LatencyBoundAuthored reports whether the workload's author set the
// scenario's maxLatency, on the scenario or on the whole workload. Without
// one, SDO calibrates the bound after install (CalibratedMaxLatency).
func (w Workload) LatencyBoundAuthored(id string) bool {
	if w.SLO.MaxLatency != 0 {
		return true
	}
	for _, scenario := range w.Scenarios {
		if scenario.ID == id && scenario.SLO != nil && scenario.SLO.MaxLatency != 0 {
			return true
		}
	}
	return false
}

// WithScenarioMaxLatency returns the workload with the scenario's maxLatency
// set to bound; the receiver is not changed.
func (w Workload) WithScenarioMaxLatency(id string, bound time.Duration) Workload {
	scenarios := make([]WorkloadScenario, len(w.Scenarios))
	copy(scenarios, w.Scenarios)
	for i := range scenarios {
		if scenarios[i].ID != id {
			continue
		}
		slo := SLO{}
		if scenarios[i].SLO != nil {
			slo = *scenarios[i].SLO
		}
		slo.MaxLatency = Duration(bound)
		scenarios[i].SLO = &slo
	}
	w.Scenarios = scenarios
	return w
}
