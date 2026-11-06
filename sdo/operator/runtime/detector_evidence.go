package runtime

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
)

// detectorWindow is the rolling window a detector judges, or zero when it
// judges current state. A traffic consumer that declares no window of its own,
// as every learned detector served by the memory host, judges its workloads'
// samples, so it lags a repair by the SDK SLO window of those workloads; a
// reachability consumer lags it by the dial window.
func detectorWindow(detector sdk.Detector, view sdk.DetectionContext) time.Duration {
	if windowed, ok := detector.(sdk.WindowedDetector); ok {
		return windowed.EvaluationWindow(view)
	}
	var window time.Duration
	if consumer, ok := detector.(servicehealth.ReachabilityConsumer); ok && consumer.ConsumesServiceReachability() {
		// The same holds for the controller's rolling Service dial history: the SDK's
		// reachability detector owns its window, whichever process serves the detector.
		window = servicehealth.NewReachabilityDetector(sdk.DetectorSpec{}).(sdk.WindowedDetector).EvaluationWindow(view)
	}
	if consumer, ok := detector.(traffic.Consumer); ok {
		// The SDK's SLO detector owns the window bound of a traffic workload.
		for _, workload := range consumer.TrafficWorkloads() {
			if slo, ok := traffic.NewDetector(sdk.DetectorSpec{}, workload).(sdk.WindowedDetector); ok {
				window = max(window, slo.EvaluationWindow(view))
			}
		}
	}
	return window
}

// detectorClearingHorizon is sdk.ClearingHorizon with detectorWindow, so the
// closure recheck and the recovery wait bound learned traffic detectors alike.
func detectorClearingHorizon(detector sdk.Detector, view sdk.DetectionContext) time.Duration {
	clearing := max(detector.Spec().Persistence.Clearing, 1)
	return time.Duration(clearing)*detector.Spec().Interval + detectorWindow(detector, view)
}

// trafficConfirmationReason explains why a traffic consumer without its own
// confirmation contract cannot establish health from an empty verdict: the
// SDK's SLO detector judges whether its health-probe workloads hold enough
// usable samples. Absence of failing samples is not evidence of health.
// Link-probe workloads carry no scenario samples and are not judged here.
func trafficConfirmationReason(consumer traffic.Consumer, view sdk.DetectionContext) string {
	var reasons []string
	for _, workload := range consumer.TrafficWorkloads() {
		if window, ok := traffic.WindowFrom(view, workload); ok && window.Workload.Purpose == traffic.PurposeLinkProbe {
			continue
		}
		slo, ok := traffic.NewDetector(sdk.DetectorSpec{}, workload).(confirmedDetector)
		if !ok || slo.Confirmed(view) {
			continue
		}
		reason := "observation window is incomplete"
		if explained, ok := slo.(interface {
			ConfirmationReason(sdk.DetectionContext) string
		}); ok {
			reason = explained.ConfirmationReason(view)
		}
		reasons = append(reasons, fmt.Sprintf("traffic workload %s: %s", workload, reason))
	}
	return strings.Join(reasons, "; ")
}

// recoveryTrafficEvidence explains why the traffic a detector consumes does
// not yet show recovery after since, when the write took effect; empty means
// it does. Each qualified scenario of each health-probe workload needs at least
// its SLO's minSamples usable samples taken after since and within maxAge, and
// those samples alone must meet the SLO. Samples that stopped arriving, or
// samples that predate the write, are not positive evidence.
func recoveryTrafficEvidence(detector sdk.Detector, view sdk.DetectionContext, since time.Time) string {
	consumer, ok := detector.(traffic.Consumer)
	if !ok {
		return ""
	}
	var reasons []string
	for _, workload := range consumer.TrafficWorkloads() {
		window, ok := traffic.WindowFrom(view, workload)
		if !ok {
			reasons = append(reasons, fmt.Sprintf("traffic workload %s has no observations", workload))
			continue
		}
		if window.Workload.Purpose == traffic.PurposeLinkProbe {
			continue
		}
		if window.LoadError != "" {
			reasons = append(reasons, fmt.Sprintf("traffic workload %s: %s", workload, window.LoadError))
			continue
		}
		var short []string
		qualified := 0
		for _, observed := range window.Scenarios {
			if !observed.Qualified {
				continue
			}
			qualified++
			var fresh []traffic.Sample
			for _, sample := range observed.Samples {
				if !sample.At.Before(since) {
					fresh = append(fresh, sample)
				}
			}
			id := observed.Scenario.ID
			slo := window.Workload.ScenarioSLO(id)
			verdict := traffic.Evaluate(id, slo, fresh, window.ObservedAt)
			switch {
			case !verdict.Evaluated:
				short = append(short, fmt.Sprintf("%s has %d of %d samples since the write took effect", id, verdict.Samples, slo.MinSamples))
			case !verdict.Healthy:
				short = append(short, fmt.Sprintf("%s violates its SLO since the write took effect: %s", id, strings.Join(verdict.Violations, ", ")))
			}
		}
		if qualified == 0 {
			short = append(short, "no qualified scenario")
		}
		sort.Strings(short)
		if len(short) > 0 {
			reasons = append(reasons, fmt.Sprintf("traffic workload %s: %s", workload, strings.Join(short, "; ")))
		}
	}
	return strings.Join(reasons, "; ")
}
