package onboardcheck

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk/metrics"
)

// ItemHarm compares the application's failures with the memory's probes
// running and paused.
const ItemHarm = "probes-do-no-harm"

// FailureCounters selects the application's own job and task failure counters
// by name; the check compares them with the probes running and paused.
const FailureCounters = `sum without (instance, pod, pod_name, container, endpoint, service, job, namespace, uid, node) ({__name__=~"(?i).*(job|task|worker|queue).*(fail|dead|error).*"} > 0)`

const (
	// harmFailureFloor is the least extra failures per minute with the probes
	// running that count as harm, and harmFailureRatio how many times the
	// paused rate they must exceed.
	harmFailureFloor = 1.0
	harmFailureRatio = 2.0
	// harmShare is the largest share of an entry's requests the probes may be.
	harmShare = 0.25
	// harmBaselineRate is the paused request rate per minute under which there
	// is no user traffic to take a share of; the probes' own rate is then
	// judged against harmAbsoluteRate per minute.
	harmBaselineRate = 6.0
	harmAbsoluteRate = 120.0
)

// noiseLabels do not identify a failure counter's series.
var noiseLabels = map[string]bool{"instance": true, "pod": true, "pod_name": true, "container": true, "endpoint": true, "service": true, "job": true, "namespace": true, "uid": true, "pod_template_hash": true, "controller_revision_hash": true, "node": true}

// HarmSample is the cumulative request, error and failure counters at one time.
type HarmSample struct {
	At     time.Time
	Counts map[string]float64
}

// HarmConfig enables the probes-do-no-harm item. Paused is the window measured
// before any probe ran; the check then compares it with the window the probes
// ran in.
type HarmConfig struct {
	PausedStart, PausedEnd HarmSample
	// MinRunning is the shortest running window judged; the item waits for it.
	MinRunning time.Duration
}

func requestsKey(service, route string) string { return "requests|" + service + "|" + route }
func errorsKey(service, route string) string   { return "errors|" + service + "|" + route }

// SampleHarm reads the counters the item compares.
func SampleHarm(ctx context.Context, prom Querier, m appmap.Map, now time.Time) (HarmSample, error) {
	out := HarmSample{At: now, Counts: map[string]float64{}}
	for _, e := range m.Entries {
		if e.Labels.Route == "" {
			continue
		}
		series, err := prom.Query(ctx, e.ByRoute())
		if err != nil {
			return HarmSample{}, fmt.Errorf("query %s: %w", e.ByRoute(), err)
		}
		for route, v := range byLabel(series, e.Labels.Route) {
			out.Counts[requestsKey(e.Service, route)] += v
		}
		if e.Labels.Status == "" || e.Errors == "" {
			continue
		}
		series, err = prom.Query(ctx, e.ErrorsByRoute())
		if err != nil {
			return HarmSample{}, fmt.Errorf("query %s: %w", e.ErrorsByRoute(), err)
		}
		for route, v := range byLabel(series, e.Labels.Route) {
			out.Counts[errorsKey(e.Service, route)] += v
		}
	}
	series, err := prom.Query(ctx, FailureCounters)
	if err != nil {
		return HarmSample{}, fmt.Errorf("query %s: %w", FailureCounters, err)
	}
	for _, s := range series {
		name := s.Labels["__name__"]
		if strings.HasSuffix(name, "_bucket") || strings.HasSuffix(name, "_sum") || strings.HasSuffix(name, "_count") || strings.Contains(name, "seconds") {
			continue
		}
		v, ok := s.Last()
		if !ok {
			continue
		}
		out.Counts["failures|"+failureSeries(name, s)] += v.Value
	}
	return out, nil
}

func failureSeries(name string, s metrics.Series) string {
	var parts []string
	for k, v := range s.Labels {
		if k != "__name__" && !noiseLabels[k] && v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	sort.Strings(parts)
	return name + "{" + strings.Join(parts, ",") + "}"
}

func perMinute(from, to HarmSample, key string) float64 {
	minutes := to.At.Sub(from.At).Minutes()
	if minutes <= 0 {
		return 0
	}
	return max(0, to.Counts[key]-from.Counts[key]) / minutes
}

// judgeHarm compares the running window with the paused one.
func judgeHarm(m appmap.Map, h HarmConfig, runStart, runEnd HarmSample) []Result {
	var out []Result
	add := func(subject, status, detail string) {
		out = append(out, Result{Item: ItemHarm, Subject: subject, Status: status, Detail: detail})
	}
	probes := probeNames(m)
	opsOf := func(service, route string) []string {
		var ids []string
		for _, op := range m.Operations {
			if op.Entry.Service == service && op.RouteValue() == route {
				ids = append(ids, op.ID)
			}
		}
		return ids
	}
	keys := map[string]bool{}
	for k := range h.PausedEnd.Counts {
		keys[k] = true
	}
	for k := range runEnd.Counts {
		keys[k] = true
	}
	var sorted []string
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	failed := 0
	requests := map[string][2]float64{}
	for _, k := range sorted {
		kind, rest, _ := strings.Cut(k, "|")
		paused, running := perMinute(h.PausedStart, h.PausedEnd, k), perMinute(runStart, runEnd, k)
		switch kind {
		case "requests":
			service, _, _ := strings.Cut(rest, "|")
			r := requests[service]
			requests[service] = [2]float64{r[0] + paused, r[1] + running}
		case "errors", "failures":
			if running-paused < harmFailureFloor || running < harmFailureRatio*paused {
				continue
			}
			failed++
			what := "job failure counter " + rest
			if strings.HasSuffix(rest, "{}") {
				what = "all job failure counters together (" + rest + ")"
			}
			if kind == "errors" {
				service, route, _ := strings.Cut(rest, "|")
				ids := opsOf(service, route)
				if len(ids) == 0 {
					ids = []string{"no declared operation"}
				}
				what = fmt.Sprintf("operation %s (route %q of %s)", strings.Join(ids, ", "), route, service)
			}
			add(rest, Red, fmt.Sprintf("%s fails %.1f/min with the probes running vs %.1f/min paused; objects the probes create or delete may be what the work fails on; probe scenarios running: %s", what, running, paused, probes))
		}
	}
	var services []string
	for s := range requests {
		services = append(services, s)
	}
	sort.Strings(services)
	for _, s := range services {
		paused, running := requests[s][0], requests[s][1]
		extra := max(0, running-paused)
		switch {
		case running <= 0:
		case paused >= harmBaselineRate && extra/running > harmShare:
			failed++
			add(s, Red, fmt.Sprintf("probes are %.0f%% of the requests of entry %s (%.1f/min running vs %.1f/min paused): lower their rate or send them to a cheaper route; scenarios: %s", 100*extra/running, s, running, paused, probes))
		case paused < harmBaselineRate && extra > harmAbsoluteRate:
			failed++
			add(s, Red, fmt.Sprintf("probes send %.0f requests/min to entry %s with almost no other traffic (%.1f/min paused): lower their rate; scenarios: %s", extra, s, paused, probes))
		}
	}
	if failed == 0 {
		add("probes", Green, fmt.Sprintf("failures and request rates with the probes running (%s window) match the paused window (%s)", runEnd.At.Sub(runStart.At).Round(time.Second), h.PausedEnd.At.Sub(h.PausedStart.At).Round(time.Second)))
	}
	return out
}

func probeNames(m appmap.Map) string {
	var names []string
	for _, p := range m.Probes {
		names = append(names, p.Scenario)
	}
	if len(names) == 0 {
		return "none declared"
	}
	return strings.Join(names, ", ")
}
