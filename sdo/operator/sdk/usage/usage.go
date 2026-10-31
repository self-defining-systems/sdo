// Package usage exposes frozen kubelet resource observations to deterministic detectors.
package usage

import (
	"fmt"
	"math"
	"time"

	"sdo.dev/operator/sdk"
)

// Watch schedules resource-usage detectors without an informer.
var Watch = sdk.WatchKind{APIVersion: "sdo.dev/v1alpha1", Kind: "ResourceUsage"}

// MaxSamples bounds each identity's rolling history.
const MaxSamples = 40

// Consumer declares resource usage consumption. Throttling opts into cAdvisor reads.
type Consumer interface{ UsageRequirements() Requirements }

// Requirements selects optional expensive inputs.
type Requirements struct{ Throttling bool }

// Identity separates pod replacements and container restarts. An empty Container
// names the pod-level ephemeral-storage observation (including shared volumes).
type Identity struct {
	PodUID, Pod, Container string
	StartedAt              time.Time
}

// Sample contains optional observations (nil means unreported), in cores and bytes.
type Sample struct {
	At                                                  time.Time
	CPUCores, WorkingSetBytes, RSSBytes, EphemeralBytes *float64
	CPURequest, CPULimit, MemoryRequest, MemoryLimit    *float64
	EphemeralRequest, EphemeralLimit                    *float64
	Periods, ThrottledPeriods                           float64
	ThrottlingAvailable                                 bool
}

// VolumeSample contains a PVC's filesystem statistics; nil means unreported.
type VolumeSample struct {
	At                                                                       time.Time
	PodUID, PVC                                                              string
	CapacityBytes, UsedBytes, AvailableBytes, Inodes, InodesUsed, InodesFree *float64
}

// ContainerWindow is an oldest-first history for one running container.
type ContainerWindow struct {
	Identity Identity
	Samples  []Sample
}

// VolumeWindow is an oldest-first history for a pod's PVC.
type VolumeWindow struct {
	PodUID, PVC string
	Samples     []VolumeSample
}

// Result freezes an observation cycle. Error means unavailable, never healthy.
type Result struct {
	ObservedAt time.Time
	Containers []ContainerWindow
	Volumes    []VolumeWindow
	Error      string
}

// Source is implemented by runtime snapshots and sdktest.Snapshot.
type Source interface{ UsageResult() (Result, bool) }

// From reads a defensive copy and returns source failures as detector errors.
func From(ctx sdk.DetectionContext) (Result, error) {
	source, ok := ctx.(Source)
	if !ok {
		return Result{}, fmt.Errorf("resource usage unavailable")
	}
	result, ok := source.UsageResult()
	if !ok {
		return Result{}, fmt.Errorf("resource usage unavailable")
	}
	if result.Error != "" {
		return Result{}, fmt.Errorf("resource usage: %s", result.Error)
	}
	return Clone(result), nil
}

// Clone isolates mutable slices and optional values from detector mutation.
func Clone(r Result) Result {
	r.Containers = append([]ContainerWindow(nil), r.Containers...)
	for i := range r.Containers {
		w := &r.Containers[i]
		w.Samples = append([]Sample(nil), w.Samples...)
		for j := range w.Samples {
			s := &w.Samples[j]
			for _, p := range []**float64{&s.CPUCores, &s.WorkingSetBytes, &s.RSSBytes, &s.EphemeralBytes, &s.CPURequest, &s.CPULimit, &s.MemoryRequest, &s.MemoryLimit, &s.EphemeralRequest, &s.EphemeralLimit} {
				if *p != nil {
					v := **p
					*p = &v
				}
			}
		}
	}
	r.Volumes = append([]VolumeWindow(nil), r.Volumes...)
	for i := range r.Volumes {
		w := &r.Volumes[i]
		w.Samples = append([]VolumeSample(nil), w.Samples...)
		for j := range w.Samples {
			s := &w.Samples[j]
			for _, p := range []**float64{&s.CapacityBytes, &s.UsedBytes, &s.AvailableBytes, &s.Inodes, &s.InodesUsed, &s.InodesFree} {
				if *p != nil {
					v := **p
					*p = &v
				}
			}
		}
	}
	return r
}

// RatioToLimit reports an explicit absence for missing, zero or invalid limits.
func RatioToLimit(value float64, limit *float64) (float64, bool) {
	if limit == nil || *limit <= 0 || !finite(*limit) || !finite(value) {
		return 0, false
	}
	return value / *limit, true
}

// ThrottledFraction computes counter deltas, excluding intervals across resets.
// Callers must provide samples with available counters, from one identity.
func ThrottledFraction(samples []Sample) (float64, bool) {
	var periods, throttled float64
	for i := 1; i < len(samples); i++ {
		a, b := samples[i-1], samples[i]
		p, t := b.Periods-a.Periods, b.ThrottledPeriods-a.ThrottledPeriods
		if a.ThrottlingAvailable && b.ThrottlingAvailable && p > 0 && t >= 0 && t <= p && finite(p) && finite(t) {
			periods += p
			throttled += t
		}
	}
	if periods == 0 {
		return 0, false
	}
	return throttled / periods, true
}

// Point is one timestamped scalar used by a forecast.
type Point struct {
	At    time.Time
	Value float64
}

// TimeToThreshold fits least squares against seconds since the first point.
// False means insufficient or non-increasing data; zero means already reached.
func TimeToThreshold(points []Point, threshold float64) (time.Duration, bool) {
	if len(points) < 2 || !finite(threshold) {
		return 0, false
	}
	var x, y, xx, xy float64
	for i, p := range points {
		if !finite(p.Value) || (i > 0 && !p.At.After(points[i-1].At)) {
			return 0, false
		}
		t := p.At.Sub(points[0].At).Seconds()
		x += t
		y += p.Value
		xx += t * t
		xy += t * p.Value
	}
	n := float64(len(points))
	denom := n*xx - x*x
	if denom <= 0 {
		return 0, false
	}
	slope := (n*xy - x*y) / denom
	if points[len(points)-1].Value >= threshold {
		return 0, true
	}
	if slope <= 0 {
		return 0, false
	}
	intercept := (y - slope*x) / n
	seconds := (threshold-intercept)/slope - points[len(points)-1].At.Sub(points[0].At).Seconds()
	if !finite(seconds) || seconds > float64(math.MaxInt64)/float64(time.Second) {
		return 0, false
	}
	return time.Duration(math.Max(0, seconds) * float64(time.Second)), true
}
func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// Evidence formats the measurements underlying a helper-derived finding.
// Nil limit or forecast is explicitly unavailable, rather than zero.
func Evidence(observed float64, limit *float64, window time.Duration, forecast *time.Duration) string {
	l, f := "unavailable", "unavailable"
	if limit != nil {
		l = fmt.Sprint(*limit)
	}
	if forecast != nil {
		f = forecast.String()
	}
	return fmt.Sprintf("observed=%g limit=%s window=%s forecast=%s", observed, l, window, f)
}

// ThresholdFinding constructs a finding with complete numerical helper evidence.
// The caller chooses the rule, resource and severity, and decides when it fires.
func ThresholdFinding(ruleID string, resource sdk.ObjectRef, severity sdk.FindingSeverity, observed float64, limit *float64, window time.Duration, forecast *time.Duration) sdk.Finding {
	return sdk.Finding{RuleID: ruleID, PrimaryResource: resource, Severity: severity, Status: sdk.FindingActive, Summary: ruleID, Evidence: Evidence(observed, limit, window, forecast)}
}
