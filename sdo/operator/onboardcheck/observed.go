package onboardcheck

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/memory/appmap"
)

// Items that read what the application's own metrics show, beyond the traffic
// SDO sends. None of them accepts a reason: they measure whether the metrics
// can tell the operations apart, which a reason cannot change.
const (
	// ItemObserved is red for a declared operation whose route never appears in
	// the entry metric, and for an entry whose real traffic goes to routes no
	// operation declares.
	ItemObserved = "operations-observed"
	// ItemLatency is red for a declared operation without a request-duration
	// series carrying its route.
	ItemLatency = "latency-per-operation"
	// ItemBackground is red for background-work classes (job or queue names in
	// the app's job metrics) that no operation triggers: unattributed
	// background work. Attach the class to the operation whose data it affects
	// (triggers) or exclude it with a reason.
	ItemBackground = appmap.ItemBackgroundCovered
)

const (
	// minCoverage is the share of an entry's requests that must go to declared
	// routes; below it most of what users do is invisible to the operations.
	minCoverage = 0.5
	// minRealRequests is how many requests beyond SDO's own an entry needs
	// before its coverage is judged.
	minRealRequests = 20
)

var (
	backgroundMetric = regexp.MustCompile(`(?i)(job|queue|worker|task)`)
	durationName     = regexp.MustCompile(`(duration|latency|seconds)`)
	durationWhat     = regexp.MustCompile(`(request|transaction|http|handler|rpc|rack|endpoint|completion)`)
	durationNot      = regexp.MustCompile(`(sql|database|external|redis|gitaly|cache|queue|elasticsearch|housekeeping|enqueue)`)
)

// isRequestDuration reports whether a metric family name is a request or
// transaction duration (not a database, cache, queue or external-call one).
func isRequestDuration(name string) bool {
	return durationName.MatchString(name) && durationWhat.MatchString(name) && !durationNot.MatchString(name)
}

// background reports whether the entry counts background work: every operation
// entering through it is internal, or its metric is named for jobs or queues.
func background(m appmap.Map, es *entryState, ops []*opState) bool {
	any := false
	for _, st := range ops {
		if st.entry.Service != es.entry.Service {
			continue
		}
		any = true
		if !st.internal {
			return backgroundMetric.MatchString(es.entry.Metric)
		}
	}
	return any || backgroundMetric.MatchString(es.entry.Metric)
}

// nearest returns the observed route label closest to want that skip does not
// exclude (a label another operation already declares is not a candidate), or
// "none".
func nearest(want string, observed map[string]float64, skip map[string]bool) string {
	best, bestD := "", -1
	for label := range observed {
		if skip[label] {
			continue
		}
		d := editDistance(strings.ToLower(want), strings.ToLower(label))
		if bestD < 0 || d < bestD || (d == bestD && label < best) {
			best, bestD = label, d
		}
	}
	if best == "" {
		return "none"
	}
	return fmt.Sprintf("%q", best)
}

func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur := make([]int, len(b)+1)
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev = cur
	}
	return prev[len(b)]
}

// runObserved judges what the metrics show after the traffic SDO sent.
// journeyRed says which operations the journeys item already reports as
// missing, so they are not reported twice.
func runObserved(ctx context.Context, cfg Config, report *Report, ops []*opState, entries map[string]*entryState, journeyRed map[string]bool) error {
	var services []string
	for s := range entries {
		services = append(services, s)
	}
	sort.Strings(services)
	families := map[string][]string{}
	for _, name := range services {
		es := entries[name]
		if es.entry.Labels.Route == "" {
			continue
		}
		series, err := cfg.Prom.Query(ctx, es.entry.DurationFamilies())
		if err != nil {
			return fmt.Errorf("query %s: %w", es.entry.DurationFamilies(), err)
		}
		for _, s := range series {
			if n := s.Labels["__name__"]; isRequestDuration(n) {
				families[name] = append(families[name], n)
			}
		}
		sort.Strings(families[name])
		if len(families[name]) == 0 {
			continue
		}
	}

	sentTo := map[string]map[string]float64{}
	for _, st := range ops {
		if sentTo[st.entry.Service] == nil {
			sentTo[st.entry.Service] = map[string]float64{}
		}
		sentTo[st.entry.Service][st.route] += float64(st.sent)
	}

	for _, st := range ops {
		es := entries[st.entry.Service]
		route := st.route
		switch {
		case route == "":
			report.add(ItemObserved, st.op.ID, Red, fmt.Sprintf("no route declared for %s: set route to the %s label value this operation's requests carry", st.op.ID, st.entry.Labels.Route))
		case es.routesAfter[route] <= 0 && !journeyRed[st.op.ID]:
			report.add(ItemObserved, st.op.ID, Red, fmt.Sprintf("declared route %q never appears in %s; nearest observed %s label: %s", route, st.entry.Metric, st.entry.Labels.Route, nearest(route, es.routesAfter, es.declared)))
		case es.routesAfter[route] > 0:
			report.add(ItemObserved, st.op.ID, Green, fmt.Sprintf("route %q appears in %s (%.0f requests)", route, st.entry.Metric, es.routesAfter[route]))
		}
		if st.entry.Labels.Route == "" {
			continue
		}
		if len(families[st.entry.Service]) == 0 {
			report.add(ItemLatency, st.op.ID, Red, fmt.Sprintf("no request-duration histogram carries the %s label for entry %s: expose latency per route so a slowdown of %s is visible", st.entry.Labels.Route, st.entry.Service, st.op.ID))
			continue
		}
		have := false
		for _, r := range []string{route, strings.TrimPrefix(route, st.op.Entry.Method+" ")} {
			if r == "" || have {
				continue
			}
			series, err := cfg.Prom.Query(ctx, st.entry.DurationRoute(families[st.entry.Service], r))
			if err != nil {
				return fmt.Errorf("query %s: %w", st.entry.DurationRoute(families[st.entry.Service], r), err)
			}
			n, _ := scalar(series)
			have = n > 0
		}
		if have {
			report.add(ItemLatency, st.op.ID, Green, fmt.Sprintf("%s carries route %q", strings.Join(families[st.entry.Service], ", "), route))
		} else {
			report.add(ItemLatency, st.op.ID, Red, fmt.Sprintf("no series of %s carries %s=%q", strings.Join(families[st.entry.Service], ", "), st.entry.Labels.Route, route))
		}
	}

	for _, st := range ops {
		for _, class := range st.op.Triggers {
			seen := false
			for _, es := range entries {
				seen = seen || es.routesAfter[class] > 0
			}
			if !seen {
				report.add(ItemObserved, st.op.ID, Red, fmt.Sprintf("triggered background class %q never appears in the job metrics: fix the class name in triggers, or make the operation's probe set the work off", class))
			}
		}
	}

	for _, name := range services {
		es := entries[name]
		type routeCount struct {
			route string
			n     float64
		}
		var undeclared []routeCount
		total, covered := 0.0, 0.0
		isBackground := background(cfg.Map, es, ops)
		for route, n := range es.routesAfter {
			real := n - sentTo[name][route]
			if real <= 0 {
				continue
			}
			total += real
			if es.declared[route] && (!isBackground || es.attributed[route]) {
				covered += real
			} else {
				undeclared = append(undeclared, routeCount{route, real})
			}
		}
		sort.Slice(undeclared, func(i, j int) bool {
			if undeclared[i].n != undeclared[j].n {
				return undeclared[i].n > undeclared[j].n
			}
			return undeclared[i].route < undeclared[j].route
		})
		var list []string
		for i, u := range undeclared {
			if i == 8 {
				list = append(list, fmt.Sprintf("... %d more", len(undeclared)-8))
				break
			}
			list = append(list, fmt.Sprintf("%s (%.0f)", u.route, u.n))
		}
		if isBackground {
			switch {
			case len(undeclared) > 0:
				report.add(ItemBackground, name, Red, fmt.Sprintf("%.0f%% of the %.0f background runs in %s are unattributed background work (classes no operation triggers; an operation entering at the job metric does not count): %s; add each class to the triggers of the operation whose data it affects, or exclude it with a reason (for example periodic maintenance)", 100*(1-covered/total), total, es.entry.Metric, strings.Join(list, ", ")))
			case total > 0:
				report.add(ItemBackground, name, Green, fmt.Sprintf("every class in %s is attributed to an operation or excluded (%.0f runs)", es.entry.Metric, total))
			}
			continue
		}
		if cfg.Harm != nil {
			// The cumulative counters include the memory's own probes, which hit
			// declared routes by design; coverage is judged on the window the
			// probes were paused (see judgePausedCoverage).
			report.foreground = append(report.foreground, name)
			continue
		}
		switch {
		case total < minRealRequests:
		case covered/total < minCoverage:
			report.add(ItemObserved, name, Red, fmt.Sprintf("only %.0f%% of the %.0f requests the entry served (beyond SDO's own) go to a declared route; routes seen but undeclared: %s", 100*covered/total, total, strings.Join(list, ", ")))
		default:
			report.add(ItemObserved, name, Green, fmt.Sprintf("%.0f%% of %.0f requests go to a declared route", 100*covered/total, total))
		}
	}
	return nil
}

// judgePausedCoverage judges, per entry that serves users, the share of the
// requests of the probes-paused window that went to declared routes: the
// memory's own probes are not what users do.
func judgePausedCoverage(m appmap.Map, services []string, h HarmConfig) []Result {
	var out []Result
	window := h.PausedEnd.At.Sub(h.PausedStart.At).Round(time.Second)
	for _, service := range services {
		declared := map[string]bool{}
		for _, op := range m.Operations {
			if op.Entry.Service == service {
				declared[op.RouteValue()] = true
			}
		}
		type routeCount struct {
			route string
			n     float64
		}
		var undeclared []routeCount
		total, covered := 0.0, 0.0
		prefix := requestsKey(service, "")
		for k, end := range h.PausedEnd.Counts {
			route, ok := strings.CutPrefix(k, prefix)
			if !ok {
				continue
			}
			n := end - h.PausedStart.Counts[k]
			if n <= 0 {
				continue
			}
			total += n
			if declared[route] {
				covered += n
			} else {
				undeclared = append(undeclared, routeCount{route, n})
			}
		}
		sort.Slice(undeclared, func(i, j int) bool {
			if undeclared[i].n != undeclared[j].n {
				return undeclared[i].n > undeclared[j].n
			}
			return undeclared[i].route < undeclared[j].route
		})
		var list []string
		for i, u := range undeclared {
			if i == 8 {
				list = append(list, fmt.Sprintf("... %d more", len(undeclared)-8))
				break
			}
			list = append(list, fmt.Sprintf("%s (%.0f)", u.route, u.n))
		}
		switch {
		case total < minRealRequests:
		case covered/total < minCoverage:
			out = append(out, Result{Item: ItemObserved, Subject: service, Status: Red, Detail: fmt.Sprintf("only %.0f%% of the %.0f requests the entry served while the probes were paused (%s) go to a declared route; routes seen but undeclared: %s", 100*covered/total, total, window, strings.Join(list, ", "))})
		default:
			out = append(out, Result{Item: ItemObserved, Subject: service, Status: Green, Detail: fmt.Sprintf("%.0f%% of %.0f requests go to a declared route while the probes were paused (%s)", 100*covered/total, total, window)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Subject < out[j].Subject })
	return out
}
