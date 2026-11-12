package runtime

import (
	"fmt"

	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/traffic"
)

// detectorWindow is the rolling window a detector judges, or zero when it
// judges current state. A traffic consumer that declares no window of its own,
// as every learned detector served by the memory host, judges its workloads'
// samples, so it lags a repair by the SDK SLO window of those workloads.
//
// A pod read refreshes on its own interval, so a detector over one lags a repair
// by that interval even when it judges only current state.
func detectorWindow(detector sdk.Detector, view sdk.DetectionContext) time.Duration {
	window := podread.LongestInterval(detector)
	if windowed, ok := detector.(sdk.WindowedDetector); ok {
		return max(window, windowed.EvaluationWindow(view))
	}
	if consumer, ok := detector.(traffic.Consumer); ok {
		return max(window, trafficConsumerWindow(consumer.TrafficWorkloads(), view))
	}
	return window
}

// trafficConsumerWindow is the longest SLO window among the workloads a traffic
// consumer judges; the SDK's SLO detector owns the bound of each.
func trafficConsumerWindow(workloads []string, view sdk.DetectionContext) time.Duration {
	var window time.Duration
	for _, workload := range workloads {
		if slo, ok := traffic.NewDetector(sdk.DetectorSpec{}, workload).(sdk.WindowedDetector); ok {
			window = max(window, slo.EvaluationWindow(view))
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
