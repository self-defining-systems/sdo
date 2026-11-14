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
	Commit  string   `json:"commit"`
	Results []Result `json:"results"`
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

// Clean is true when nothing is red.
func (r Report) Clean() bool { _, red, _ := r.Counts(); return red == 0 }

// Text renders the report, headed by the commit.
func (r Report) Text() string {
	g, red, n := r.Counts()
	var b strings.Builder
	fmt.Fprintf(&b, "commit %s\ncheck: green %d red %d note %d\n", r.Commit, g, red, n)
	for _, x := range r.Results {
		fmt.Fprintf(&b, "%s %s %s: %s\n", x.Status, x.Item, x.Subject, x.Detail)
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
	}
	return nil
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

// Run sends known traffic and checks the metric items. An error means the
// check could not run (a failed query), not that an item is red.
func Run(ctx context.Context, cfg Config) (Report, error) {
	if err := cfg.validate(); err != nil {
		return Report{}, err
	}
	report := Report{Commit: cfg.Commit}
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
			return Report{}, err
		}
		st.before, _ = scalar(series)
	}
	for _, es := range entries {
		all, err := query(es.entry.All())
		if err != nil {
			return Report{}, err
		}
		es.allBefore, _ = scalar(all)
		routes, err := query(es.entry.ByRoute())
		if err != nil {
			return Report{}, err
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
			return Report{}, err
		}
		if settled() || waited >= cfg.Settle {
			break
		}
		if err := cfg.Wait(ctx, cfg.Poll); err != nil {
			return Report{}, err
		}
	}
	for _, es := range entries {
		routes, err := query(es.entry.ByRoute())
		if err != nil {
			return Report{}, err
		}
		es.routesAfter = byLabel(routes, es.entry.Labels.Route)
	}
	for _, st := range ops {
		if st.entry.Labels.Status != "" {
			series, err := query(st.entry.ByStatus(st.route, st.method))
			if err != nil {
				return Report{}, err
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
		if !(es.allAfter-es.allBefore < float64(es.sent)) && !hasRed(report, ItemRoutes, name) {
			result(ItemRoutes, name, Green, fmt.Sprintf("entry metric rose by %.0f for %d sent; every moved route is declared", es.allAfter-es.allBefore, es.sent))
		}
	}
	return report, nil
}

func hasRed(r Report, item, subject string) bool {
	for _, x := range r.Results {
		if x.Item == item && x.Subject == subject && x.Status != Green {
			return true
		}
	}
	return false
}
