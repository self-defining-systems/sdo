package generichealth

import (
	"context"
	"fmt"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
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
			e.capacity("memory-saturation", resource, *s.WorkingSetBytes, s.MemoryLimit, points, time.Hour, w.Identity.Container)
		}
		if d.throttling && s.CPULimit != nil && *s.CPULimit > 0 {
			if f, ok := usage.ThrottledFraction(w.Samples); ok && f > 0.25 {
				e.emit("cpu-throttling", resource, fmt.Sprintf("container=%s throttled fraction=%g threshold=0.25 window=%s", w.Identity.Container, f, s.At.Sub(w.Samples[0].At)), false)
				e.findings[len(e.findings)-1].Fingerprint += "/" + w.Identity.Container
			}
		}
	}
	for _, w := range r.Volumes {
		if len(w.Samples) == 0 {
			continue
		}
		s := w.Samples[len(w.Samples)-1]
		resource := sdk.ObjectRef{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: ctx.Namespace(), Name: w.PVC}
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
			e.capacity("pvc-bytes-saturation", resource, *s.UsedBytes, s.CapacityBytes, bytes, 4*time.Hour, "")
		}
		if s.InodesUsed != nil {
			e.capacity("pvc-inodes-saturation", resource, *s.InodesUsed, s.Inodes, inodes, 4*time.Hour, "")
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
func (e *evaluation) capacity(rule string, r sdk.ObjectRef, value float64, limit *float64, points []usage.Point, horizon time.Duration, container string) {
	ratio, ok := usage.RatioToLimit(value, limit)
	if !ok {
		return
	}
	var forecast *time.Duration
	// Forecast storage growth only: unlike stored bytes and inodes, memory is
	// reclaimable. Heap warm-up and GC cycles cannot predict an OOM from a
	// short working-set slope; memory still reports observed pressure above 90%.
	// Two minutes and three observations reject one-point storage slopes.
	window := time.Duration(0)
	if len(points) > 0 {
		window = points[len(points)-1].At.Sub(points[0].At)
	}
	if rule != "memory-saturation" && len(points) >= 3 && window >= 2*time.Minute {
		if t, ok := usage.TimeToThreshold(points, *limit); ok {
			forecast = &t
		}
	}
	if ratio <= 0.9 && (forecast == nil || *forecast > horizon) {
		return
	}
	e.emit(rule, r, "container="+container+" "+usage.Evidence(value, limit, window, forecast)+fmt.Sprintf(" ratio=%g threshold=0.9 forecast horizon=%s", ratio, horizon), false)
	e.findings[len(e.findings)-1].Fingerprint += "/" + container
}

type crashes struct{ spec sdk.DetectorSpec }

// NewCrashes reports failed-container crash tails and newly appearing runtime
// signatures in surviving containers, never generic error lines.
func NewCrashes(s sdk.DetectorSpec) sdk.Detector { return crashes{s} }
func (d crashes) Spec() sdk.DetectorSpec         { return d.spec }

// LogQueries requests previous-container evidence as well as bounded current logs.
func (d crashes) LogQueries() []logs.Query {
	return []logs.Query{{ID: "generic-crashes", Previous: true, Terminations: true, Patterns: []logs.Pattern{
		{ID: "go-panic", RE2: `^panic:`}, {ID: "go-fatal", RE2: `^fatal error:`}, {ID: "python-traceback", RE2: `^Traceback \(most recent call last\):`}, {ID: "java-oom", RE2: `\bjava\.lang\.OutOfMemoryError\b`}, {ID: "java-thread", RE2: `^Exception in thread "`}, {ID: "segfault", RE2: `(?i)\bsegmentation fault\b`},
		// Evidence only: never used as a firing signature.
		{ID: "previous-tail", RE2: `.+`},
	}}}
}
func (d crashes) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	r, err := logs.Require(ctx, "generic-crashes")
	if err != nil {
		return nil, err
	}
	e := evaluation{ctx: ctx}
	for _, w := range r.Windows {
		if w.Namespace != ctx.Namespace() {
			continue
		}
		for _, t := range w.Terminations {
			if t.ExitCode == 0 || t.Tail.Error != "" {
				continue
			}
			for _, p := range d.LogQueries()[0].Patterns {
				if p.ID == "previous-tail" || t.Tail.Matches[p.ID].Count == 0 {
					continue
				}
				e.emit("log-"+p.ID, sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: w.Namespace, Name: w.Pod}, fmt.Sprintf("container=%s restarts=%d exit=%d finished=%s pattern=%s tail=%+v", w.Container, t.RestartCount, t.ExitCode, t.FinishedAt, p.RE2, t.Tail), false)
				e.findings[len(e.findings)-1].Fingerprint += fmt.Sprintf("/%s/%s/%d", w.UID, w.Container, t.RestartCount)
				break // One finding per completed crash, even with multiple signatures.
			}
		}
		if len(w.Cycles) == 0 || !w.Cycles[len(w.Cycles)-1].Running {
			continue
		}
		for _, p := range d.LogQueries()[0].Patterns {
			if p.ID == "previous-tail" {
				continue
			}
			if yes, ev := logs.Appeared(w, p.ID); yes {
				previous := ""
				if len(w.Cycles) > 0 {
					if prev := w.Cycles[len(w.Cycles)-1].Previous; prev != nil {
						previous = fmt.Sprintf("%+v", prev.Matches)
					}
				}
				e.emit("log-"+p.ID, sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: w.Namespace, Name: w.Pod}, fmt.Sprintf("container=%s restarts=%d pattern=%s evidence=%+v previous-container=%s", w.Container, w.RestartCount, p.RE2, ev, previous), false)
				e.findings[len(e.findings)-1].Fingerprint += fmt.Sprintf("/%s/%s/%d", w.UID, w.Container, w.RestartCount)
			}
		}
	}
	return finish(e.findings, d.spec), nil
}
