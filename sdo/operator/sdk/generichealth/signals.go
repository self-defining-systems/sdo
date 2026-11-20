package generichealth

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/usage"
	"sort"
	"time"
)

type saturation struct {
	spec       sdk.DetectorSpec
	throttling bool
}

// NewSaturation monitors memory and PVC capacity. Throttling requests one extra
// cAdvisor read per node per cycle; the bootstrap selects this explicitly.
func NewSaturation(s sdk.DetectorSpec, throttling bool) sdk.Detector {
	return saturation{s, throttling}
}
func (d saturation) Spec() sdk.DetectorSpec { return d.spec }
func (d saturation) UsageRequirements() usage.Requirements {
	return usage.Requirements{Throttling: d.throttling}
}
func (d saturation) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	r, err := usage.From(ctx)
	if err != nil {
		return nil, err
	}
	e := evaluation{ctx: ctx}
	for _, w := range r.Containers {
		if len(w.Samples) == 0 {
			continue
		}
		s := w.Samples[len(w.Samples)-1]
		resource := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: ctx.Namespace(), Name: w.Identity.Pod}
		points := []usage.Point{}
		for _, p := range w.Samples {
			if p.WorkingSetBytes != nil {
				points = append(points, usage.Point{At: p.At, Value: *p.WorkingSetBytes})
			}
		}
		if s.WorkingSetBytes != nil {
			e.memory(resource, s, points, w.Identity.Container)
		}
		if d.throttling && s.CPULimit != nil && *s.CPULimit > 0 {
			if f, ok := usage.ThrottledFraction(w.Samples); ok && f > 0.25 {
				e.emit("cpu-throttling", resource, fmt.Sprintf("container=%s throttled fraction=%g threshold=0.25 window=%s", w.Identity.Container, f, s.At.Sub(w.Samples[0].At)), false)
				e.findings[len(e.findings)-1].Fingerprint += "/" + w.Identity.Container
			}
		}
	}
	claims := map[string]corev1.PersistentVolumeClaim{}
	for _, pvc := range ctx.PersistentVolumeClaims() {
		if pvc.Namespace == ctx.Namespace() || pvc.Namespace == "" {
			claims[pvc.Name] = pvc
		}
	}
	for _, w := range r.Volumes {
		if len(w.Samples) == 0 {
			continue
		}
		s := w.Samples[len(w.Samples)-1]
		resource := sdk.ObjectRef{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: ctx.Namespace(), Name: w.PVC}
		// A shared filesystem's growth is every tenant's, not the application's.
		forecast := !sharedFilesystem(claims[w.PVC], s.CapacityBytes)
		bytes, inodes := []usage.Point{}, []usage.Point{}
		for _, p := range w.Samples {
			if p.UsedBytes != nil {
				bytes = append(bytes, usage.Point{At: p.At, Value: *p.UsedBytes})
			}
			if p.InodesUsed != nil {
				inodes = append(inodes, usage.Point{At: p.At, Value: *p.InodesUsed})
			}
		}
		if s.UsedBytes != nil {
			e.capacity("pvc-bytes-saturation", resource, *s.UsedBytes, s.CapacityBytes, bytes, forecast, "")
		}
		if s.InodesUsed != nil {
			e.capacity("pvc-inodes-saturation", resource, *s.InodesUsed, s.Inodes, inodes, forecast, "")
		}
	}
	return finish(e.findings, d.spec), nil
}
func finish(fs []sdk.Finding, s sdk.DetectorSpec) []sdk.Finding {
	// Several ports, ingress paths or mounted replicas can explain one binding.
	// Emit one stable fingerprint and retain bounded deterministic evidence.
	sort.Slice(fs, func(i, j int) bool {
		if fs[i].Fingerprint == fs[j].Fingerprint {
			return fs[i].Evidence < fs[j].Evidence
		}
		return fs[i].Fingerprint < fs[j].Fingerprint
	})
	result := []sdk.Finding{}
	for _, f := range fs {
		f.DetectorID = s.ID
		f.Playbooks = append([]string(nil), s.Playbooks...)
		if len(result) > 0 && result[len(result)-1].Fingerprint == f.Fingerprint {
			last := &result[len(result)-1]
			if last.Evidence != f.Evidence && len(last.Evidence) < 4096 {
				last.Evidence += "; " + f.Evidence
			}
			continue
		}
		result = append(result, f)
	}
	return result
}

// Memory saturation needs a hurt signal beyond a high working set, which
// includes reclaimable page cache: anonymous (RSS) memory above the threshold,
// or an OOMKilled termination within the window. A source with neither is
// silent. Shared memory is neither anonymous RSS nor reclaimable without swap,
// so such growth is missed until the first OOM kill.
const (
	memoryThreshold = 0.9
	memoryOOMWindow = 10 * time.Minute
)

func (e *evaluation) memory(r sdk.ObjectRef, s usage.Sample, points []usage.Point, container string) {
	ratio, ok := usage.RatioToLimit(*s.WorkingSetBytes, s.MemoryLimit)
	if !ok || ratio <= memoryThreshold {
		return
	}
	signal := ""
	if s.RSSBytes != nil {
		if rr, ok := usage.RatioToLimit(*s.RSSBytes, s.MemoryLimit); ok && rr > memoryThreshold {
			signal = fmt.Sprintf("rss ratio=%g", rr)
		}
	}
	if signal == "" && !s.LastOOMKilledAt.IsZero() && s.At.Sub(s.LastOOMKilledAt) <= memoryOOMWindow {
		signal = "oom-killed"
	}
	if signal == "" {
		return
	}
	window := time.Duration(0)
	if len(points) > 0 {
		window = points[len(points)-1].At.Sub(points[0].At)
	}
	e.emit("memory-saturation", r, "container="+container+" signal="+signal+" "+usage.Evidence(*s.WorkingSetBytes, s.MemoryLimit, window, nil)+fmt.Sprintf(" ratio=%g threshold=%g", ratio, memoryThreshold), false)
	e.findings[len(e.findings)-1].Fingerprint += "/" + container
}

// Storage forecasts extrapolate a growth rate forecastHorizon ahead. Only
// storage is forecast: unlike stored bytes and inodes, memory is reclaimable,
// and memory reports observed pressure above 90% only. The rate is the median
// pairwise slope (usage.TimeToThreshold) over at least minForecastWindow of
// samples: at the usage observer's 15 s cadence that is 20 or more samples, so
// a burst must last over a minute to move the median, and a rate is never
// projected more than 48 times the window it was measured over.
const (
	forecastHorizon   = 4 * time.Hour
	minForecastWindow = 5 * time.Minute
	// sharedCapacityFactor is how much larger than its claim a volume's
	// filesystem may be before it counts as shared with other tenants.
	sharedCapacityFactor = 2
)

// sharedFilesystem reports a volume whose filesystem is far larger than its
// claim requested: a hostPath or local-path volume on the node's own disk,
// whose used bytes and inodes include every other tenant's.
func sharedFilesystem(pvc corev1.PersistentVolumeClaim, capacity *float64) bool {
	request, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if !ok || capacity == nil || request.IsZero() {
		return false
	}
	return *capacity > sharedCapacityFactor*request.AsApproximateFloat64()
}

// capacity reports a volume above 90% of its capacity, observed now, or, when
// forecast is allowed, one whose sustained growth fills it within
// forecastHorizon. A forecast-only finding is predicted (sdk.MetadataPredicted):
// a trend, never a condition that holds now.
func (e *evaluation) capacity(rule string, r sdk.ObjectRef, value float64, limit *float64, points []usage.Point, forecast bool, container string) {
	ratio, ok := usage.RatioToLimit(value, limit)
	if !ok {
		return
	}
	var fill *time.Duration
	window := time.Duration(0)
	if len(points) > 0 {
		window = points[len(points)-1].At.Sub(points[0].At)
	}
	if forecast && len(points) >= 3 && window >= minForecastWindow {
		if t, ok := usage.TimeToThreshold(points, *limit); ok {
			fill = &t
		}
	}
	observed := ratio > 0.9
	if !observed && (fill == nil || *fill > forecastHorizon) {
		return
	}
	e.emit(rule, r, "container="+container+" "+usage.Evidence(value, limit, window, fill)+fmt.Sprintf(" ratio=%g threshold=0.9 forecast horizon=%s", ratio, forecastHorizon), false)
	finding := &e.findings[len(e.findings)-1]
	finding.Fingerprint += "/" + container
	if !observed {
		finding.Metadata = map[string]any{sdk.MetadataPredicted: true}
	}
}
