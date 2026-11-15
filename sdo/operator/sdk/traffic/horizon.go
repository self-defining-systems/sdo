package traffic

import "time"

// healthDetectorIntervals is how many evaluations, ten seconds apart (the
// interval Go installs for a traffic-probe detector), a violation needs before
// it is raised: the default firing persistence.
const healthDetectorIntervals = 2

const healthDetectorInterval = 10 * time.Second

// DetectionHorizon is the longest time a health-probe workload can take, from
// the moment a component its scenarios use stops answering, to raise a failing
// scenario: the requests that began before the break must time out, the SLO
// window must hold enough failed samples (at the scenario's arrival rate) for
// the verdict, the failures must span the hold, and the detector must confirm
// the violation on consecutive evaluations. A check that waits for a raise
// must wait at least this long, or a probe that works reads as one that does
// not. It is zero for a workload that is not a health probe.
func (w Workload) DetectionHorizon() time.Duration {
	if w.Purpose != PurposeHealthProbe {
		return 0
	}
	var longest time.Duration
	for _, scenario := range w.Scenarios {
		slo := w.ScenarioSLO(scenario.ID)
		arrival := w.ScenarioArrival(scenario.ID)
		samples := time.Duration(slo.MinSamples) * arrival
		span := max(slo.MaxAge.Duration(), samples)
		longest = max(longest, w.Timeout.Duration()+min(span, samples+arrival)+DefaultHealthMinDuration)
	}
	return longest + healthDetectorIntervals*healthDetectorInterval
}
