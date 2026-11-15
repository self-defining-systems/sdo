// Package onboardcheck verifies, against a running environment, that the
// metrics the application map declares tell operations apart. SDO builds each
// operation's series from the declared metric, route, method and status labels,
// sends known traffic to the operation's entry and compares the counts. The
// package is pure: Prometheus, the traffic sender, the clock and waiting are
// injected.
package onboardcheck

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk/metrics"
)

// Item names. A reason in the map for ItemJourneysMoveOwnMetrics also covers
// ItemErrorsSeparable.
const (
	ItemJourneys        = appmap.ItemJourneysMoveOwnMetrics
	ItemErrorsSeparable = appmap.ItemErrorsSeparable
	ItemRoutes          = appmap.ItemRoutesMatchOperations
	ItemMap             = "map-entries-valid"
)

// Status of one check result.
const (
	Green = "green"
	Red   = "red"
	Note  = "note"
)

// Querier evaluates an instant PromQL query.
type Querier interface {
	Query(ctx context.Context, promql string) ([]metrics.Series, error)
}

// Sender sends n requests to the operation's entry and returns how many were
// delivered. It returns 0 for an operation SDO cannot drive (background work).
type Sender interface {
	Send(ctx context.Context, op appmap.Operation, n int) (int, error)
}

// Exposure is a Service reachable from outside the cluster.
type Exposure struct {
	Service string
	Via     string
}

// Config is one check run.
type Config struct {
	Map      appmap.Map
	Commit   string
	Prom     Querier
	Sender   Sender
	Exposed  []Exposure
	Requests int
	// Settle bounds the wait for scrapes after sending; Poll is the interval.
	Settle, Poll time.Duration
	// Wait pauses for d or until ctx ends.
	Wait func(ctx context.Context, d time.Duration) error
	// Now is the clock the live items measure durations and windows with.
	Now func() time.Time
	// Quiet and Break enable the live items that run the memory's detectors;
	// nil skips the item.
	Quiet *QuietConfig
	Break *BreakConfig
	// Harm enables probes-do-no-harm; it needs Quiet, whose window is the one
	// with the probes running.
	Harm *HarmConfig
	// NoDetectors says the memory SDO would deploy holds no detector at all,
	// so the live items have nothing to judge.
	NoDetectors bool
	// Quick is the fast-iteration run: the metric items and a short smoke of
	// the detectors (each judged at least once). It runs no break item and its
	// report is never clean.
	Quick bool
	// Progress, when set, receives a line as the check advances.
	Progress func(string)

	phases *phaseLog
}

func (c Config) say(format string, args ...any) {
	if c.Progress != nil {
		c.Progress(fmt.Sprintf(format, args...))
	}
}

// Result is one line of the report.
type Result struct {
	Item    string `json:"item"`
	Subject string `json:"subject"`
	Status  string `json:"status"`
	Detail  string `json:"detail"`
}

// Report is the check outcome, tagged with the commit it ran for.
type Report struct {
	Commit string `json:"commit"`
	// Mode is "full" or "quick".
	Mode    string   `json:"mode"`
	Results []Result `json:"results"`
	// Durations is how long each live item took.
	Durations []ItemDuration `json:"durations,omitempty"`

	// notInstrumented are the operations request-metrics reported as not
	// instrumented while the application was healthy, by operation ID.
	notInstrumented map[string]string
}

// Counts returns the number of green, red and note results.
func (r Report) Counts() (green, red, note int) {
	for _, x := range r.Results {
		switch x.Status {
		case Green:
			green++
		case Red:
			red++
		default:
			note++
		}
	}
	return
}

// Clean is true when nothing is red in a full run. A quick run never is: it
// proves less than the gate.
func (r Report) Clean() bool { return r.Mode != ModeQuick && r.NoRed() }

// NoRed is true when no item is red, in either mode.
func (r Report) NoRed() bool { _, red, _ := r.Counts(); return red == 0 }

// Report modes.
const (
	ModeFull  = "full"
	ModeQuick = "quick"
)

// Text renders the report, headed by the commit.
func (r Report) Text() string {
	g, red, n := r.Counts()
	var b strings.Builder
	fmt.Fprintf(&b, "commit %s\ncheck: green %d red %d note %d\n", r.Commit, g, red, n)
	if r.Mode == ModeQuick {
		verdict := "no red item"
		if red > 0 {
			verdict = "red items"
		}
		fmt.Fprintf(&b, "mode: quick (%s; a quick run is never clean, run the full check for the gate)\n", verdict)
	} else {
		fmt.Fprintf(&b, "mode: full\n")
	}
	for _, x := range r.Results {
		fmt.Fprintf(&b, "%s %s %s: %s\n", x.Status, x.Item, x.Subject, x.Detail)
	}
	for _, d := range r.Durations {
		fmt.Fprintf(&b, "time %s %s\n", d.Item, time.Duration(d.Seconds*float64(time.Second)).Round(time.Second))
	}
	if len(r.Durations) > 0 {
		fmt.Fprintf(&b, "timing: %s\n", timingSummary(r.Durations))
	}
	return b.String()
}

func (c Config) validate() error {
	switch {
	case c.Commit == "":
		return errors.New("onboardcheck: commit is required")
	case c.Prom == nil || c.Sender == nil || c.Wait == nil:
		return errors.New("onboardcheck: Prom, Sender and Wait are required")
	case c.Requests < 1:
		return errors.New("onboardcheck: Requests must be at least 1")
	case c.Poll <= 0 || c.Settle < 0:
		return errors.New("onboardcheck: Poll must be positive and Settle not negative")
	case c.Harm != nil && (c.Quiet == nil || c.Now == nil || c.Harm.MinRunning <= 0 || !c.Harm.PausedEnd.At.After(c.Harm.PausedStart.At)):
		return errors.New("onboardcheck: probes-do-no-harm needs the quiet window, a clock, a positive MinRunning and a paused window with positive length")
	case c.Quick && c.Break != nil:
		return errors.New("onboardcheck: a quick run does not run fails-under-break")
	}
	return c.validateLive()
}

type opState struct {
	op           appmap.Operation
	entry        appmap.MetricEntry
	route        string
	method       string
	internal     bool
	before       float64
	after        float64
	present      bool
	sent         int
	sendErr      error
	statusSeries []metrics.Series
	errorsErr    error
}

type entryState struct {
	entry        appmap.MetricEntry
	allBefore    float64
	allAfter     float64
	routesBefore map[string]float64
	routesAfter  map[string]float64
	sent         int
	declared     map[string]bool
	hasDrivenOps bool
}

func scalar(series []metrics.Series) (float64, bool) {
	if len(series) == 0 {
		return 0, false
	}
	total := 0.0
	for _, s := range series {
		if v, ok := s.Last(); ok {
			total += v.Value
		}
	}
	return total, true
}

func byLabel(series []metrics.Series, label string) map[string]float64 {
	out := map[string]float64{}
	for _, s := range series {
		if v, ok := s.Last(); ok {
			out[s.Labels[label]] += v.Value
		}
	}
	return out
}

func keys(m map[string]float64) string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return "none"
	}
	if len(out) > 12 {
		out = append(out[:12], fmt.Sprintf("... %d more", len(out)-12))
	}
	return strings.Join(out, ", ")
}

// Run sends known traffic and checks the metric items; the quiet-when-healthy
// item observes meanwhile (its sample window is much longer than the traffic).
// An error means the check could not run (a failed query), not that an item is
// red.
func Run(ctx context.Context, cfg Config) (Report, error) {
	if err := cfg.validate(); err != nil {
		return Report{}, err
	}
	cfg.phases = &phaseLog{}
	report := Report{Commit: cfg.Commit, Mode: ModeFull}
	if cfg.Quick {
		report.Mode = ModeQuick
	}
	var quiet Report
	var quietRed bool
	quietDone := make(chan struct{})
	qctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if cfg.Quiet != nil && !cfg.NoDetectors {
		go func() {
			defer close(quietDone)
			start := cfg.Now()
			quietRed = runQuiet(qctx, cfg, &quiet)
			quiet.Durations = append(quiet.Durations, ItemDuration{Item: ItemQuiet, Seconds: cfg.Now().Sub(start).Seconds()})
		}()
	} else {
		close(quietDone)
	}
	cfg.say("metric items: sending %d requests per operation", cfg.Requests)
	var metricsStart time.Time
	if cfg.Now != nil {
		metricsStart = cfg.Now()
	}
	err := runMetrics(ctx, cfg, &report)
	if cfg.Now != nil && err == nil {
		report.Durations = append(report.Durations, ItemDuration{Item: PhaseMetrics, Seconds: cfg.Now().Sub(metricsStart).Seconds()})
	}
	if err != nil {
		cancel()
		<-quietDone
		return Report{}, err
	}
	cfg.say("metric items done: %d results", len(report.Results))
	var harmStart HarmSample
	harmOn := cfg.Harm != nil && cfg.Quiet != nil && !cfg.NoDetectors
	if harmOn {
		// The probes have been running since the check began; the window that
		// counts starts now, after the traffic SDO sent.
		if harmStart, err = SampleHarm(ctx, cfg.Prom, cfg.Map, cfg.Now()); err != nil {
			cancel()
			<-quietDone
			return Report{}, err
		}
	}
	<-quietDone
	if harmOn {
		if wait := cfg.Harm.MinRunning - cfg.Now().Sub(harmStart.At); wait > 0 {
			cfg.say("probes-do-no-harm: waiting %s for a long enough window with the probes running", wait.Round(time.Second))
			if err := cfg.Wait(ctx, wait); err != nil {
				return Report{}, err
			}
		}
		harmEnd, herr := SampleHarm(ctx, cfg.Prom, cfg.Map, cfg.Now())
		if herr != nil {
			return Report{}, herr
		}
		report.Results = append(report.Results, judgeHarm(cfg.Map, *cfg.Harm, harmStart, harmEnd)...)
	}
	report.Results = append(report.Results, quiet.Results...)
	report.Durations = append(report.Durations, quiet.Durations...)
	for id, line := range quiet.notInstrumented {
		if report.notInstrumented == nil {
			report.notInstrumented = map[string]string{}
		}
		report.notInstrumented[id] = line
	}
	if cfg.NoDetectors {
		report.add(ItemQuiet, "detectors", Red, "SDO: no detectors were evaluated: the memory SDO would deploy holds none, so silence here proves nothing; author health detectors or traffic workloads")
		if cfg.Break != nil {
			report.add(ItemBreak, "all", Red, "SDO: not run: no detectors were evaluated")
		}
		return report, nil
	}
	if cfg.Break != nil {
		if quietRed {
			// The breaks are independent evidence: only a raise that was not
			// standing before the break counts, so a noisy quiet item does not
			// hide whether a break gets named.
			report.add(ItemBreak, "all", Note, "ran although quiet-when-healthy is red: standing raises are ignored and only raises that appear after a break count")
		}
		start := cfg.Now()
		runBreak(ctx, cfg, &report)
		report.Durations = append(report.Durations, ItemDuration{Item: ItemBreak, Seconds: cfg.Now().Sub(start).Seconds()})
		report.Durations = append(report.Durations, cfg.phases.durations()...)
	}
	report.applyNotInstrumented(cfg.Map)
	return report, nil
}

func runMetrics(ctx context.Context, cfg Config, report *Report) error {
	add := func(item, subject, status, detail string) {
		report.Results = append(report.Results, Result{Item: item, Subject: subject, Status: status, Detail: detail})
	}
	for _, f := range cfg.Map.CheckEntries() {
		add(ItemMap, f.Rule, Red, f.Message)
	}
	query := func(text string) ([]metrics.Series, error) {
		series, err := cfg.Prom.Query(ctx, text)
		if err != nil {
			return nil, fmt.Errorf("query %s: %w", text, err)
		}
		return series, nil
	}

	var ops []*opState
	entries := map[string]*entryState{}
	duplicate := map[string][]string{}
	for _, op := range cfg.Map.Operations {
		entry, ok := cfg.Map.EntryFor(op.Entry.Service)
		if !ok {
			add(ItemJourneys, op.ID, Red, fmt.Sprintf("no entries record for service %s: declare its request metric and route, method and status labels", op.Entry.Service))
			continue
		}
		st := &opState{op: op, entry: entry, route: op.RouteValue(), method: op.Entry.Method, internal: cfg.Map.Internal(op)}
		if st.internal || entry.Labels.Method == "" {
			st.method = ""
		}
		ops = append(ops, st)
		key := entry.Service + "|" + st.method + "|" + st.route
		duplicate[key] = append(duplicate[key], op.ID)
		es := entries[entry.Service]
		if es == nil {
			es = &entryState{entry: entry, declared: map[string]bool{}}
			entries[entry.Service] = es
		}
		es.declared[st.route] = true
	}

	// Baseline.
	for _, st := range ops {
		series, err := query(st.entry.RequestCount(st.route, st.method))
		if err != nil {
			return err
		}
		st.before, _ = scalar(series)
	}
	for _, es := range entries {
		all, err := query(es.entry.All())
		if err != nil {
			return err
		}
		es.allBefore, _ = scalar(all)
		routes, err := query(es.entry.ByRoute())
		if err != nil {
			return err
		}
		es.routesBefore = byLabel(routes, es.entry.Labels.Route)
	}

	// Send known traffic.
	for _, st := range ops {
		st.sent, st.sendErr = cfg.Sender.Send(ctx, st.op, cfg.Requests)
		entries[st.entry.Service].sent += st.sent
	}

	// Poll until every driven counter shows what was sent, or settle runs out.
	read := func() error {
		for _, st := range ops {
			series, err := query(st.entry.RequestCount(st.route, st.method))
			if err != nil {
				return err
			}
			st.after, st.present = scalar(series)
		}
		for _, es := range entries {
			all, err := query(es.entry.All())
			if err != nil {
				return err
			}
			es.allAfter, _ = scalar(all)
		}
		return nil
	}
	settled := func() bool {
		for _, st := range ops {
			if st.sent > 0 && st.after-st.before < float64(st.sent) {
				return false
			}
		}
		for _, es := range entries {
			if es.sent > 0 && es.allAfter-es.allBefore < float64(es.sent) {
				return false
			}
		}
		return true
	}
	for waited := time.Duration(0); ; waited += cfg.Poll {
		if err := read(); err != nil {
			return err
		}
		if settled() || waited >= cfg.Settle {
			break
		}
		if err := cfg.Wait(ctx, cfg.Poll); err != nil {
			return err
		}
	}
	for _, es := range entries {
		routes, err := query(es.entry.ByRoute())
		if err != nil {
			return err
		}
		es.routesAfter = byLabel(routes, es.entry.Labels.Route)
	}
	for _, st := range ops {
		if st.entry.Labels.Status != "" {
			series, err := query(st.entry.ByStatus(st.route, st.method))
			if err != nil {
				return err
			}
			st.statusSeries = series
			_, st.errorsErr = cfg.Prom.Query(ctx, st.entry.ErrorCount(st.route, st.method))
		}
	}

	reasons := map[[2]string]string{}
	for _, r := range cfg.Map.Reasons {
		item := r.Item
		reasons[[2]string{item, r.Subject}] = r.Reason
	}
	result := func(item, subject, status, detail string) {
		if status == Red {
			if why, ok := reasons[[2]string{item, subject}]; ok {
				status, detail = Note, detail+" (reason: "+why+")"
			} else if item == ItemErrorsSeparable {
				if why, ok := reasons[[2]string{ItemJourneys, subject}]; ok {
					status, detail = Note, detail+" (reason: "+why+")"
				}
			}
		}
		add(item, subject, status, detail)
	}

	for _, st := range ops {
		es := entries[st.entry.Service]
		// Requests: the route value carries a series, distinct, and rises by what was sent.
		switch {
		case st.sendErr != nil:
			result(ItemJourneys, st.op.ID, Red, "sending traffic failed: "+st.sendErr.Error())
		case len(duplicate[st.entry.Service+"|"+st.method+"|"+st.route]) > 1:
			result(ItemJourneys, st.op.ID, Red, fmt.Sprintf("route label value %q is shared by operations %s; operations need distinct route values", st.route, strings.Join(duplicate[st.entry.Service+"|"+st.method+"|"+st.route], ", ")))
		case !st.present:
			result(ItemJourneys, st.op.ID, Red, fmt.Sprintf("no series of %s carries %s=%q; route values on this metric: %s", st.entry.Metric, st.entry.Labels.Route, st.route, keys(es.routesAfter)))
		case st.sent == 0:
			result(ItemJourneys, st.op.ID, Green, fmt.Sprintf("series present (value %.0f); SDO cannot drive this entry, so its rise is not checked", st.after))
		case st.after-st.before < float64(st.sent):
			result(ItemJourneys, st.op.ID, Red, fmt.Sprintf("requests rose by %.0f; SDO sent %d (counts may be split across routes, or the scrape is slow)", st.after-st.before, st.sent))
		default:
			result(ItemJourneys, st.op.ID, Green, fmt.Sprintf("requests rose by %.0f; SDO sent %d", st.after-st.before, st.sent))
		}
		// Errors: the status series must carry this operation's route.
		switch {
		case st.entry.Labels.Status == "":
			result(ItemErrorsSeparable, st.op.ID, Red, fmt.Sprintf("entry %s declares no status label, so errors cannot be told apart per operation", st.entry.Service))
		case st.errorsErr != nil:
			result(ItemErrorsSeparable, st.op.ID, Red, "error series does not evaluate: "+st.errorsErr.Error())
		case len(byLabel(st.statusSeries, st.entry.Labels.Status)) == 0:
			result(ItemErrorsSeparable, st.op.ID, Red, fmt.Sprintf("no series with a %s label carries %s=%q; errors are not separable per operation (route values on the metric: %s)", st.entry.Labels.Status, st.entry.Labels.Route, st.route, keys(es.routesAfter)))
		default:
			result(ItemErrorsSeparable, st.op.ID, Green, "status series carry this route: "+keys(byLabel(st.statusSeries, st.entry.Labels.Status)))
		}
	}

	journeyRed := map[string]bool{}
	for _, r := range report.Results {
		if r.Item == ItemJourneys && r.Status == Red {
			journeyRed[r.Subject] = true
		}
	}
	if err := runObserved(ctx, cfg, report, ops, entries, journeyRed); err != nil {
		return err
	}

	var services []string
	for s := range entries {
		services = append(services, s)
	}
	sort.Strings(services)
	for _, name := range services {
		es := entries[name]
		if es.sent > 0 && es.allAfter-es.allBefore < float64(es.sent) {
			result(ItemRoutes, name, Red, fmt.Sprintf("entry metric rose by %.0f; SDO sent %d to this entry (the selector may be too narrow)", es.allAfter-es.allBefore, es.sent))
		}
		var stray []string
		for route, after := range es.routesAfter {
			if after-es.routesBefore[route] > 0 && !es.declared[route] {
				stray = append(stray, fmt.Sprintf("%s (+%.0f)", route, after-es.routesBefore[route]))
			}
		}
		sort.Strings(stray)
		if len(stray) > 0 {
			result(ItemRoutes, name, Red, "route values moved that no operation declares: "+strings.Join(stray, ", "))
		}
	}
	exposed := map[string]bool{}
	for _, e := range cfg.Exposed {
		exposed[e.Service] = true
		if _, ok := cfg.Map.EntryFor(e.Service); !ok {
			result(ItemRoutes, e.Service, Red, fmt.Sprintf("exposed entry (%s) has no entries record", e.Via))
		}
	}
	for _, name := range services {
		if !exposed[name] {
			continue
		}
		es := entries[name]
		if !(es.allAfter-es.allBefore < float64(es.sent)) && !hasRed(*report, ItemRoutes, name) {
			result(ItemRoutes, name, Green, fmt.Sprintf("entry metric rose by %.0f for %d sent; every moved route is declared", es.allAfter-es.allBefore, es.sent))
		}
	}
	return nil
}

func hasRed(r Report, item, subject string) bool {
	for _, x := range r.Results {
		if x.Item == item && x.Subject == subject && x.Status != Green {
			return true
		}
	}
	return false
}
