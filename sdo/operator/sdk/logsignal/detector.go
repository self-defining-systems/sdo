package logsignal

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/redact"
)

// Rule IDs of log signal findings.
const (
	RuleLines  = "log-signal.lines"
	RuleAbsent = "log-signal.absent"
)

// MaxSampleBytes bounds the redacted sample line a finding carries.
const MaxSampleBytes = 160

// Interval is the evaluation interval of a log signal detector.
const Interval = 15 * time.Second

type detector struct {
	spec      sdk.DetectorSpec
	id        string
	signal    Spec
	window    time.Duration
	workloads map[string]string
	broken    error
}

// NewDetector returns the deterministic detector of the log signal id, whose
// YAML text is source. workloads maps each component the file names to its
// application-map workload ("Kind/name"). A file that does not validate, or a
// component without a workload, yields a detector that always errors.
func NewDetector(spec sdk.DetectorSpec, id, source string, workloads map[string]string) sdk.Detector {
	if !slices.Contains(spec.Watches, logs.Watch) {
		spec.Watches = append(slices.Clone(spec.Watches), logs.Watch)
	}
	d := detector{spec: spec, id: id, workloads: workloads}
	parsed, err := Parse([]byte(source))
	if err == nil {
		err = parsed.Validate(id)
	}
	for _, c := range parsed.Components {
		if err == nil && workloads[c] == "" {
			err = fmt.Errorf("component %s has no workload", c)
		}
	}
	if err != nil {
		d.broken = fmt.Errorf("log signal %s: %w", id, err)
		return d
	}
	d.signal = parsed
	d.window, _ = parsed.WindowDuration()
	return d
}

func (d detector) Spec() sdk.DetectorSpec { return d.spec }

// LogQueries declares the file's patterns as one query.
func (d detector) LogQueries() []logs.Query {
	if d.broken != nil {
		return nil
	}
	return []logs.Query{d.signal.Query(d.id)}
}

// measure is one component's log activity in a window ending at end.
type measure struct {
	containers int
	// covered is the shortest stretch of the window any container's
	// retained cycles span.
	covered   time.Duration
	counts    map[string]int
	truncated bool
	running   bool
	samples   map[string]string
	err       string
}

func measureWindows(windows []logs.Window, namespace, workload string, end time.Time, window time.Duration) measure {
	m := measure{counts: map[string]int{}, samples: map[string]string{}}
	from := end.Add(-window)
	for _, w := range windows {
		if w.Namespace != namespace || w.Workload != workload || len(w.Cycles) == 0 {
			continue
		}
		start := w.Cycles[0].Start
		if start.Before(from) {
			start = from
		}
		covered := end.Sub(start)
		if m.containers == 0 || covered < m.covered {
			m.covered = covered
		}
		m.containers++
		m.running = m.running || w.Cycles[len(w.Cycles)-1].Running
		for _, c := range w.Cycles {
			if !c.End.After(from) || c.End.After(end) {
				continue
			}
			if c.Error != "" && m.err == "" {
				m.err = fmt.Sprintf("Pod %s container %s: %s", w.Pod, w.Container, c.Error)
			}
			m.truncated = m.truncated || c.Truncated
			for id, match := range c.Matches {
				m.counts[id] += match.Count
				if m.samples[id] == "" && len(match.Excerpts) > 0 {
					m.samples[id] = match.Excerpts[0]
				}
			}
		}
	}
	return m
}

// scaled is a count over covered as a count over window.
func scaled(count int, covered, window time.Duration) float64 {
	if covered <= 0 {
		return 0
	}
	return float64(count) * float64(window) / float64(covered)
}

// Detect judges each component's current window against its healthy history.
// A failed read of a component's container is a detector error, never a
// clear; a component with no healthy history yet is not judged.
func (d detector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.broken != nil {
		return nil, d.broken
	}
	r, ok := logs.ResultFrom(ctx, QueryID(d.id))
	if !ok {
		return nil, fmt.Errorf("log query %q unavailable", QueryID(d.id))
	}
	if r.Error != "" {
		return nil, fmt.Errorf("log query %q: %s", QueryID(d.id), r.Error)
	}
	var findings []sdk.Finding
	components := slices.Clone(d.signal.Components)
	sort.Strings(components)
	for _, component := range components {
		workload := d.workloads[component]
		current := measureWindows(r.Windows, ctx.Namespace(), workload, r.ObservedAt, d.window)
		if current.err != "" {
			return nil, fmt.Errorf("log signal %s component %s: %s", d.id, component, current.err)
		}
		if current.containers == 0 {
			continue
		}
		var history []measure
		for _, sample := range r.History {
			m := measureWindows(sample.Windows, ctx.Namespace(), workload, sample.End, d.window)
			if m.containers > 0 && m.covered >= d.window/2 {
				history = append(history, m)
			}
		}
		if len(history) == 0 {
			continue
		}
		for _, p := range d.signal.Signals {
			most := 0.0
			for _, h := range history {
				most = max(most, scaled(h.counts[p.ID], h.covered, d.window))
			}
			count := current.counts[p.ID]
			if count >= p.Threshold() && float64(count) > HealthyFactor*most {
				summary := fmt.Sprintf("component %s logged %d lines matching %s in the last %s; healthy windows logged at most %.0f", component, count, p.ID, d.window, most)
				findings = append(findings, d.finding(RuleLines, component, workload, ctx.Namespace(), p, summary, current, map[string]any{"count": count, "healthy_max": most}))
			}
		}
		for _, p := range d.signal.Expect {
			least := -1.0
			for _, h := range history {
				if v := scaled(h.counts[p.ID], h.covered, d.window); least < 0 || v < least {
					least = v
				}
			}
			if least >= float64(p.Threshold()) && current.covered >= d.window && current.running && !current.truncated && current.counts[p.ID] == 0 {
				summary := fmt.Sprintf("component %s logged no line matching expected activity %s in the last %s; every healthy window logged at least %.0f", component, p.ID, d.window, least)
				findings = append(findings, d.finding(RuleAbsent, component, workload, ctx.Namespace(), p, summary, current, map[string]any{"count": 0, "healthy_min": least}))
			}
		}
	}
	return findings, nil
}

func (d detector) finding(rule, component, workload, namespace string, p Pattern, summary string, current measure, extra map[string]any) sdk.Finding {
	kind, name, _ := strings.Cut(workload, "/")
	evidence := fmt.Sprintf("log signal %s pattern %s (re2 %q) over %d containers", d.id, p.ID, p.RE2, current.containers)
	if line := current.samples[p.ID]; line != "" {
		clean := redact.Secrets(line)
		evidence += "; sample line: " + clean[:min(len(clean), MaxSampleBytes)]
	}
	metadata := map[string]any{"signal": d.id, "component": component, "pattern": p.ID, "window_seconds": int(d.window / time.Second)}
	for k, v := range extra {
		metadata[k] = v
	}
	return sdk.Finding{
		DetectorID: d.spec.ID, RuleID: rule, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: summary, Evidence: evidence,
		PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: kind, Namespace: namespace, Name: name},
		Playbooks:       slices.Clone(d.spec.Playbooks),
		Fingerprint:     fmt.Sprintf("%s/%s/%s/%s", d.spec.ID, rule, component, p.ID),
		Metadata:        metadata,
	}
}
