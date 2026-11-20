// Package requestmetrics judges the application's own request metrics per user
// operation. For each operation it counts failed requests (server errors, and
// client errors that jump above their healthy share) over a rolling window and
// reports an operation whose failures are numerous, a real share of its
// requests and far above its healthy share, on consecutive evaluation steps.
//
// The detector is deterministic and does no I/O: the controller freezes the
// declared queries into the detection snapshot (so a recorded snapshot replays
// the same findings), and the package only reads those results. A declared
// request metric that lacks its series or labels is reported as "not
// instrumented", never as silence. An operation the map gives no request
// metric at all (Operation.Unmeasurable) is not applicable: the detector
// raises nothing for it and names it in its coverage note
// (ConfirmationReason while confirmed), so an application without request
// metrics never fails acceptance, opens or holds an incident here.
package requestmetrics

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
)

// Rule IDs of the findings.
const (
	RuleErrors          = "request-metrics.errors"
	RuleNotInstrumented = "request-metrics.not-instrumented"
	// RuleInsufficientBaseline is a clue: a client-error jump was visible but
	// the route has too little healthy history to call it a jump.
	RuleInsufficientBaseline = "request-metrics.insufficient-baseline"
)

// Failure classes a finding names.
const (
	ClassServer     = "server-errors"
	ClassClientJump = "client-error-jump"
)

// Rule holds the numbers of the raise rule.
type Rule struct {
	// Window is the rolling window each step counts requests over.
	Window time.Duration
	// Step is the evaluation step inside the range; it should match the scrape interval.
	Step time.Duration
	// Consecutive is how many newest steps in a row must satisfy the rule.
	Consecutive int
	// MinFailed is the least number of failed requests in the window.
	MinFailed float64
	// MinShare is the least share of the operation's requests that failed.
	MinShare float64
	// MinRatio is how many times the healthy share the failed share must be.
	MinRatio float64
	// ShareFloor is the lowest healthy share the ratio is taken against.
	ShareFloor float64
	// MinBaselineRequests is the fewest healthy requests that make a baseline
	// share believable; with fewer the healthy share is zero (so the floor applies).
	MinBaselineRequests float64
	// ThinBaselineShare is the share of a window's requests that must have
	// failed for a client-error jump to be called on a thin baseline (fewer
	// than MinBaselineRequests healthy requests). The share must also be
	// MinRatio times the thin baseline's own observed failed share. Below it
	// the jump stays the clue RuleInsufficientBaseline. Zero never calls one. ThinMinFailed adds an absolute count.
	ThinBaselineShare float64
	// ThinMinFailed is the fewest failed requests every step of the run needs
	// for a thin-baseline jump to be called; fewer stay the clue.
	ThinMinFailed float64
	// Lookback is how far back the steps reach (at least Window). A raised
	// error finding stays until a step after it confirms recovery, so a
	// stretch with few requests never clears it; a window with none does.
	Lookback time.Duration
	// MinHealthy is the fewest requests, all healthy, that confirm recovery in
	// one window; zero means MinFailed.
	MinHealthy float64
}

func (r Rule) lookback() time.Duration {
	if r.Lookback > r.Window {
		return r.Lookback
	}
	return r.Window
}

func (r Rule) minHealthy() float64 {
	if r.MinHealthy > 0 {
		return r.MinHealthy
	}
	return r.MinFailed
}

// DefaultRule is the rule the prototype replayed: at least 5 failed requests
// and 5% of the requests and 5 times the healthy share over 30 s, on two
// consecutive 5 s steps, healthy share floored at 1%.
func DefaultRule() Rule {
	return Rule{Window: 30 * time.Second, Step: 5 * time.Second, Consecutive: 2, MinFailed: 5, MinShare: 0.05, MinRatio: 5, ShareFloor: 0.01, MinBaselineRequests: 20, ThinBaselineShare: 0.5, ThinMinFailed: 8, Lookback: 5 * time.Minute, MinHealthy: 5}
}

// Entry is the request metric of one entry service, with the PromQL the
// controller evaluates. The caller builds the expressions from the application
// map's declared labels, so the detector never parses PromQL.
type Entry struct {
	Service string
	Metric  string
	// Selector is the entry's series selector as text, used in messages.
	Selector                             string
	RouteLabel, MethodLabel, StatusLabel string
	// ServerErrors is a regular expression over status values that counts a
	// request as a server error; empty means "5..".
	ServerErrors string
	// Recent returns the windowed increase per route, method and status, as a
	// range query of Rule.Window ending now at Rule.Step.
	Recent string
	// Baseline returns the same increase over a healthy period before the
	// window, as an instant query.
	Baseline string
	// Presence returns every series of the metric (zero or not).
	Presence string
}

// Operation is one user operation the detector watches.
type Operation struct {
	ID      string
	Service string
	Route   string
	// Method restricts the operation to one HTTP method; empty matches any.
	Method string
	// Unmeasurable, when set, says why the map gives this operation no request
	// metric to read (no entries record for its entry service, or one beyond
	// the entries the detector reads). The operation is then not applicable:
	// no finding, only the coverage note ConfirmationReason returns.
	Unmeasurable string

	// undeclared marks an operation the detector derived from a route of the
	// entry's metric that no declared operation covers.
	undeclared bool
	// truncated says how the undeclared routes were capped (empty when not).
	truncated string
}

// DefaultMaxUndeclaredRoutes is how many undeclared routes per entry the
// detector evaluates when Config.MaxUndeclaredRoutes is zero.
const DefaultMaxUndeclaredRoutes = 20

// Config configures the detector.
type Config struct {
	Spec       sdk.DetectorSpec
	Entries    []Entry
	Operations []Operation
	Rule       Rule
	// MaxUndeclaredRoutes bounds how many route values per entry that no
	// declared operation covers are evaluated (busiest first); zero means
	// DefaultMaxUndeclaredRoutes.
	MaxUndeclaredRoutes int
}

type entryPlan struct {
	Entry
	errors             *regexp.Regexp
	recent, base, pres metrics.Query
}

type detector struct {
	cfg     Config
	queries []metrics.Query
	entries map[string]entryPlan
	err     error
}

// NewDetector returns the request-metrics detector. An invalid configuration
// makes Detect fail and Validate report why.
func NewDetector(cfg Config) sdk.Detector {
	d := &detector{cfg: cfg, entries: map[string]entryPlan{}}
	d.err = d.build()
	return d
}

func queryID(service, kind string) string { return "reqmetrics-" + service + "-" + kind }

func (d *detector) build() error {
	r := d.cfg.Rule
	switch {
	case d.cfg.Spec.ID == "":
		return errors.New("request-metrics detector needs a spec ID")
	case r.Window <= 0 || r.Step <= 0 || r.Consecutive < 1 || r.MinFailed < 1 || r.MinShare <= 0 || r.MinRatio < 1 || r.ShareFloor < 0 || r.Lookback < 0 || r.MinHealthy < 0:
		return fmt.Errorf("request-metrics rule %+v is not valid", r)
	}
	for _, e := range d.cfg.Entries {
		if e.Service == "" || e.RouteLabel == "" || e.Recent == "" || e.Baseline == "" || e.Presence == "" {
			return fmt.Errorf("request-metrics entry %q needs a service, a route label and its three queries", e.Service)
		}
		if _, dup := d.entries[e.Service]; dup {
			return fmt.Errorf("request-metrics entry %q declared twice", e.Service)
		}
		pattern := e.ServerErrors
		if pattern == "" {
			pattern = "5.."
		}
		re, err := regexp.Compile("^(?:" + pattern + ")$")
		if err != nil {
			return fmt.Errorf("request-metrics entry %q: server errors pattern: %w", e.Service, err)
		}
		plan := entryPlan{Entry: e, errors: re,
			recent: metrics.Query{ID: queryID(e.Service, "recent"), Expr: e.Recent, Range: r.lookback(), Step: r.Step},
			base:   metrics.Query{ID: queryID(e.Service, "baseline"), Expr: e.Baseline},
			pres:   metrics.Query{ID: queryID(e.Service, "presence"), Expr: e.Presence},
		}
		d.entries[e.Service] = plan
		d.queries = append(d.queries, plan.recent, plan.base, plan.pres)
	}
	if err := metrics.ValidateQueries(d.queries); err != nil {
		return err
	}
	for _, op := range d.cfg.Operations {
		if op.ID == "" || op.Service == "" {
			return errors.New("request-metrics operation needs an ID and a service")
		}
		if _, ok := d.entries[op.Service]; !ok && op.Unmeasurable == "" {
			return fmt.Errorf("request-metrics operation %q names service %q without an entry", op.ID, op.Service)
		}
	}
	return nil
}

func (d *detector) Spec() sdk.DetectorSpec         { return d.cfg.Spec }
func (d *detector) MetricQueries() []metrics.Query { return append([]metrics.Query(nil), d.queries...) }
func (d *detector) Validate() error                { return d.err }

type counts struct{ total, server, client float64 }

func (c counts) add(o counts) counts {
	return counts{c.total + o.total, c.server + o.server, c.client + o.client}
}

func (p entryPlan) matches(op Operation, labels map[string]string) bool {
	if labels[p.RouteLabel] != op.Route {
		return false
	}
	return op.Method == "" || p.MethodLabel == "" || strings.EqualFold(labels[p.MethodLabel], op.Method)
}

func (p entryPlan) classify(labels map[string]string, v float64) counts {
	if math.IsNaN(v) || v < 0 {
		return counts{}
	}
	c := counts{total: v}
	status := labels[p.StatusLabel]
	switch {
	case p.errors.MatchString(status):
		c.server = v
	case strings.HasPrefix(status, "4"):
		c.client = v
	}
	return c
}

func (d *detector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.err != nil {
		return nil, d.err
	}
	results := map[string]metrics.Result{}
	for _, q := range d.queries {
		r, ok := metrics.ResultFrom(ctx, q.ID)
		if !ok {
			return nil, fmt.Errorf("metrics unavailable: detection context carries no result for query %s", q.ID)
		}
		if r.Error != "" {
			return nil, fmt.Errorf("metrics unavailable: query %s: %s", q.ID, r.Error)
		}
		results[q.ID] = r
	}
	var findings []sdk.Finding
	for _, op := range d.cfg.Operations {
		if op.Unmeasurable != "" {
			continue // not applicable: named in the coverage note, never a finding
		}
		plan := d.entries[op.Service]
		if why := plan.instrumentation(results); why != "" {
			if len(results[plan.pres.ID].Series) == 0 && serviceStarting(ctx, op.Service) {
				continue // the scrape target is a pod still inside its startup, not a missing instrument
			}
			findings = append(findings, d.notInstrumented(ctx, op, why))
			continue
		}
		if f, ok := d.judge(ctx, op, plan, results); ok {
			findings = append(findings, f)
		}
	}
	for _, e := range d.cfg.Entries {
		findings = append(findings, d.undeclared(ctx, d.entries[e.Service], results)...)
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].Fingerprint < findings[j].Fingerprint })
	return findings, nil
}

// Confirmed implements the observation confirmation contract: health cannot be
// established while the metrics source has not scraped a declared service's
// metric at all (no series), because that says nothing about the application.
// Query errors are reported by Detect, not pending. Not-applicable operations
// never hold confirmation.
func (d *detector) Confirmed(ctx sdk.DetectionContext) bool { return d.unscraped(ctx) == "" }

// ConfirmationReason names the declared services whose metric has no series
// while unconfirmed. Once confirmed it is the coverage note naming the
// operations the detector is not applicable to, or "".
func (d *detector) ConfirmationReason(ctx sdk.DetectionContext) string {
	if why := d.unscraped(ctx); why != "" {
		return why
	}
	return d.notApplicable()
}

// notApplicable words the operations the map gives no request metric, grouped
// by reason, or "" when every operation is measurable.
func (d *detector) notApplicable() string {
	byReason := map[string][]string{}
	var reasons []string
	for _, op := range d.cfg.Operations {
		if op.Unmeasurable == "" {
			continue
		}
		if _, seen := byReason[op.Unmeasurable]; !seen {
			reasons = append(reasons, op.Unmeasurable)
		}
		byReason[op.Unmeasurable] = append(byReason[op.Unmeasurable], op.ID)
	}
	if len(reasons) == 0 {
		return ""
	}
	sort.Strings(reasons)
	parts := make([]string, 0, len(reasons))
	for _, why := range reasons {
		ids := byReason[why]
		sort.Strings(ids)
		noun := "operation"
		if len(ids) > 1 {
			noun = "operations"
		}
		parts = append(parts, fmt.Sprintf("%s %s (%s)", noun, strings.Join(ids, ", "), why))
	}
	note := "request metrics not applicable to " + strings.Join(parts, "; ")
	if len(note) > maxNoteBytes {
		note = note[:maxNoteBytes] + "..."
	}
	return note
}

// maxNoteBytes bounds the coverage note; its head names the cause.
const maxNoteBytes = 600

// unscraped names the declared services whose metric has no series yet.
func (d *detector) unscraped(ctx sdk.DetectionContext) string {
	if d.err != nil {
		return ""
	}
	var unscraped []string
	seen := map[string]bool{}
	for _, op := range d.cfg.Operations {
		plan, ok := d.entries[op.Service]
		if !ok || op.Unmeasurable != "" || seen[op.Service] {
			continue
		}
		seen[op.Service] = true
		r, ok := metrics.ResultFrom(ctx, plan.pres.ID)
		if !ok || r.Error != "" || len(r.Series) > 0 {
			continue
		}
		unscraped = append(unscraped, fmt.Sprintf("%s (metric %s%s)", op.Service, plan.Metric, plan.Selector))
	}
	if len(unscraped) == 0 {
		return ""
	}
	return "the metrics source has not scraped " + strings.Join(unscraped, ", ") + " yet"
}

// instrumentation reports why the entry's metric cannot be judged per
// operation, or "" when it can.
func (p entryPlan) instrumentation(results map[string]metrics.Result) string {
	presence := results[p.pres.ID].Series
	if len(presence) == 0 {
		return fmt.Sprintf("metric %s%s has no series: the application does not expose it, or the selector matches nothing", p.Metric, p.Selector)
	}
	if p.StatusLabel == "" {
		return fmt.Sprintf("entry %s declares no status label, so failed requests cannot be counted per operation", p.Service)
	}
	// A grouped query leaves a label off a series whose source carries none
	// (a health check, a scrape), and such a series may sort first: the label
	// is present when any series carries it.
	for _, label := range []string{p.RouteLabel, p.StatusLabel} {
		found := false
		for _, s := range presence {
			if _, ok := s.Labels[label]; ok {
				found = true
				break
			}
		}
		if !found {
			return fmt.Sprintf("metric %s%s carries no %q label", p.Metric, p.Selector, label)
		}
	}
	return ""
}

// undeclared judges every route value of the entry's request metric that no
// declared operation covers, with the same rule, busiest routes first up to the
// configured bound (failing routes first). The application map need not be complete for the detector
// to see a failing route.
func (d *detector) undeclared(ctx sdk.DetectionContext, plan entryPlan, results map[string]metrics.Result) []sdk.Finding {
	if plan.instrumentation(results) != "" {
		return nil // declared operations report it; with none there is nothing to name
	}
	declared := map[string]bool{}
	for _, op := range d.cfg.Operations {
		if op.Service == plan.Service {
			declared[op.Route] = true
		}
	}
	// Routes that carried failed requests anywhere in the lookback rank first: a failing route must stay judged
	// (and its finding stay until recovery is confirmed) however busy the healthy routes become. Busiest first
	// among the rest.
	volume, failed := map[string]float64{}, map[string]float64{}
	for _, s := range results[plan.recent.ID].Series {
		route := s.Labels[plan.RouteLabel]
		if declared[route] {
			continue
		}
		if _, seen := volume[route]; !seen {
			volume[route] = 0
		}
		if v, ok := s.Last(); ok {
			volume[route] += plan.classify(s.Labels, v.Value).total
		}
		for _, sample := range s.Samples {
			c := plan.classify(s.Labels, sample.Value)
			failed[route] = math.Max(failed[route], c.server+c.client)
		}
	}
	routes := make([]string, 0, len(volume))
	for route := range volume {
		routes = append(routes, route)
	}
	sort.Slice(routes, func(i, j int) bool {
		if failed[routes[i]] != failed[routes[j]] {
			return failed[routes[i]] > failed[routes[j]]
		}
		if volume[routes[i]] != volume[routes[j]] {
			return volume[routes[i]] > volume[routes[j]]
		}
		return routes[i] < routes[j]
	})
	limit := d.cfg.MaxUndeclaredRoutes
	if limit <= 0 {
		limit = DefaultMaxUndeclaredRoutes
	}
	truncated := ""
	if len(routes) > limit {
		truncated = fmt.Sprintf("only the %d busiest of %d undeclared routes of %s were evaluated", limit, len(routes), plan.Service)
		routes = routes[:limit]
	}
	var out []sdk.Finding
	for _, route := range routes {
		op := Operation{ID: "undeclared route " + route, Service: plan.Service, Route: route, undeclared: true, truncated: truncated}
		if f, ok := d.judge(ctx, op, plan, results); ok {
			out = append(out, f)
		}
	}
	return out
}

// serviceStarting reports whether a pod the Service selects is not ready yet
// but still inside its own startup allowance: a metric with no series then
// says the target is restarting, not that the application is uninstrumented.
// A target missing beyond the allowance still counts.
func serviceStarting(ctx sdk.DetectionContext, service string) bool {
	for _, pod := range ctx.PodsForService(ctx.Namespace(), service) {
		if sdk.PodStarting(ctx, pod) {
			return true
		}
	}
	return false
}

func (d *detector) notInstrumented(ctx sdk.DetectionContext, op Operation, why string) sdk.Finding {
	return sdk.Finding{
		DetectorID: d.cfg.Spec.ID, RuleID: RuleNotInstrumented, Status: sdk.FindingActive, Severity: sdk.SeverityInfo,
		Summary:         fmt.Sprintf("operation %s is not instrumented for request metrics", op.ID),
		Evidence:        fmt.Sprintf("operation %s (route %q) cannot be watched: %s", op.ID, op.Route, why),
		PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: ctx.Namespace(), Name: op.Service},
		Fingerprint:     "requestmetrics/errors/" + op.ID,
		Metadata:        map[string]any{"operation": op.ID, "route": op.Route, "method": op.Method, "service": op.Service, "reason": why},
	}
}

// perStep sums the operation's series per sample time.
func (p entryPlan) perStep(op Operation, series []metrics.Series) map[time.Time]counts {
	out := map[time.Time]counts{}
	for _, s := range series {
		if !p.matches(op, s.Labels) {
			continue
		}
		for _, sample := range s.Samples {
			out[sample.At] = out[sample.At].add(p.classify(s.Labels, sample.Value))
		}
	}
	return out
}

// healthy is the operation's healthy server and client-error shares.
// enough is false when the baseline holds fewer than minRequests requests: the
// shares are then zero and a client-error jump cannot be judged.
func (p entryPlan) healthy(op Operation, series []metrics.Series, minRequests float64) (server, client float64, enough bool, thinClient float64) {
	var sum counts
	for _, s := range series {
		if v, ok := s.Last(); ok && p.matches(op, s.Labels) {
			sum = sum.add(p.classify(s.Labels, v.Value))
		}
	}
	if sum.total == 0 {
		return 0, 0, false, 0
	}
	if sum.total < minRequests {
		return 0, 0, false, sum.client / sum.total
	}
	return sum.server / sum.total, sum.client / sum.total, true, 0
}

type verdict struct {
	class                         string
	failed, total, share, healthy float64
	ok                            bool
}

func (r Rule) judgeStep(c counts, class string, healthyServer, healthyClient float64) verdict {
	v := verdict{class: class, total: c.total, failed: c.server, healthy: healthyServer}
	if class == ClassClientJump {
		v.failed, v.healthy = math.Max(0, c.client-healthyClient*c.total), healthyClient
	}
	if c.total > 0 {
		v.share = v.failed / c.total
	}
	v.ok = v.failed >= r.MinFailed && v.share >= r.MinShare && v.share >= r.MinRatio*math.Max(v.healthy, r.ShareFloor)
	return v
}

// thinJump reports whether a client-error rise on a thin baseline is large
// enough to be called anyway: at least ThinBaselineShare of the window's
// requests failed, and MinRatio times the thin baseline's observed share.
func (r Rule) thinJump(v verdict, thinClient float64) bool {
	return r.ThinBaselineShare > 0 && v.failed >= r.ThinMinFailed && v.share >= r.ThinBaselineShare && v.share >= r.MinRatio*thinClient
}

// thinAll applies thinJump to every step of the consecutive run.
func (r Rule) thinAll(steps map[time.Time]counts, run []time.Time, thinClient float64) bool {
	for _, t := range run {
		if !r.thinJump(r.judgeStep(steps[t], ClassClientJump, 0, 0), thinClient) {
			return false
		}
	}
	return true
}

// confirmsRecovery is a window with enough requests, none of them failed.
func (r Rule) confirmsRecovery(c counts, healthyClient float64) bool {
	return c.total >= r.minHealthy() && c.server == 0 && c.client-healthyClient*c.total < 1
}

// quietNewest reports that the newest evaluation window saw no requests at
// all: no failure is left to observe, so a raised finding has nothing to
// stand on. The probe scenarios, not the absent traffic, vouch for the route.
// A window with a few requests is not quiet; it needs enough healthy ones.
func quietNewest(steps map[time.Time]counts, times []time.Time, observedAt time.Time, window time.Duration) bool {
	if len(times) == 0 {
		return true
	}
	last := times[len(times)-1]
	return steps[last].total == 0 || (!observedAt.IsZero() && observedAt.Sub(last) >= window)
}

func (d *detector) judge(ctx sdk.DetectionContext, op Operation, plan entryPlan, results map[string]metrics.Result) (sdk.Finding, bool) {
	r := d.cfg.Rule
	recent := results[plan.recent.ID]
	steps := plan.perStep(op, recent.Series)
	times := make([]time.Time, 0, len(steps))
	for t := range steps {
		times = append(times, t)
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	// A finding that holds the open incident is judged against the share
	// floor, not the baseline: an ongoing fault must never become its own
	// healthy share, and it leaves only through confirmed recovery.
	held := sdk.HeldFingerprints(ctx, d.cfg.Spec.ID)[errorsFingerprint(op)]
	if len(times) < r.Consecutive && !held {
		return sdk.Finding{}, false
	}
	quiet := quietNewest(steps, times, recent.ObservedAt, r.Window)
	healthyServer, healthyClient, enough, thinClient := plan.healthy(op, results[plan.base.ID].Series, r.MinBaselineRequests)
	if held {
		healthyServer, healthyClient, enough = 0, 0, true
	}
	// The newest step at which the rule held on Consecutive steps in a row.
	var best *verdict
	raisedAt := -1
	// unjudged marks a client-error jump held on thin history: it is kept
	// (a clue under the error fingerprint) rather than dropped, so an incident
	// it opened or joined holds until recovery is confirmed.
	unjudged := false
	for end := len(times) - 1; end >= r.Consecutive-1 && (best == nil || unjudged); end-- {
		for _, class := range []string{ClassServer, ClassClientJump} {
			held := true
			for _, t := range times[end-r.Consecutive+1 : end+1] {
				if !r.judgeStep(steps[t], class, healthyServer, healthyClient).ok {
					held = false
					break
				}
			}
			if !held {
				continue
			}
			if class == ClassClientJump && !enough {
				v := r.judgeStep(steps[times[end]], class, healthyServer, healthyClient)
				if r.thinAll(steps, times[end-r.Consecutive+1:end+1], thinClient) {
					if best == nil || unjudged || v.failed > best.failed {
						best, raisedAt, unjudged = &v, end, false
					}
					continue
				}
				if best == nil {
					best, raisedAt, unjudged = &v, end, true
				}
				continue
			}
			if v := r.judgeStep(steps[times[end]], class, healthyServer, healthyClient); best == nil || unjudged || v.failed > best.failed {
				best, raisedAt, unjudged = &v, end, false
			}
		}
	}
	onset := time.Time{}
	if best != nil {
		onset = times[raisedAt]
	}
	heldUnconfirmed := ""
	if best == nil && held {
		if quiet {
			return sdk.Finding{}, false
		}
		for _, t := range times {
			if r.confirmsRecovery(steps[t], 0) {
				return sdk.Finding{}, false
			}
		}
		best, raisedAt, onset = &verdict{class: ClassServer}, len(times)-1, recent.ObservedAt
		heldUnconfirmed = " The incident this finding holds is open and no recent step had enough requests with none failed, so recovery is not confirmed."
	}
	if best == nil {
		return sdk.Finding{}, false
	}
	// A finding that stopped holding stays until a later step has enough
	// requests and none failed, or the newest window has no requests at all
	// (a route nobody calls cannot be failing). Few requests are not recovery.
	unconfirmed := ""
	if raisedAt < len(times)-1 {
		if quiet {
			return sdk.Finding{}, false
		}
		for _, t := range times[raisedAt+1:] {
			if r.confirmsRecovery(steps[t], healthyClient) {
				return sdk.Finding{}, false
			}
		}
		newest := steps[times[len(times)-1]]
		if newest.total < r.minHealthy() {
			unconfirmed = fmt.Sprintf(" Insufficient recent traffic to confirm recovery: %.0f requests in the newest window, at least %.0f healthy ones are needed.", newest.total, r.minHealthy())
		} else {
			unconfirmed = fmt.Sprintf(" Recovery is not confirmed: the newest window still has failed requests (%.0f of %.0f).", newest.server+math.Max(0, newest.client-healthyClient*newest.total), newest.total)
		}
	}
	method := op.Method
	if method == "" {
		method = "any"
	}
	window := r.Window.String()
	fingerprint, note := "requestmetrics/errors/"+op.ID, unconfirmed+heldUnconfirmed
	meta := map[string]any{}
	if unconfirmed != "" || heldUnconfirmed != "" {
		meta["recovery_unconfirmed"] = true
	}
	samples := failingSamples(ctx, op, best.class)
	meta["samples"] = samples.metadata()
	if op.undeclared {
		fingerprint = "requestmetrics/errors/undeclared/" + op.Service + "/" + op.Route
		note = " This is an undeclared route: the application map declares no operation for it."
		meta["undeclared_route"] = true
		if op.truncated != "" {
			note += " Truncated: " + op.truncated + "."
			meta["undeclared_truncated"] = op.truncated
		}
	}
	rule, severity := RuleErrors, sdk.SeverityWarning
	if unjudged {
		rule, severity = RuleInsufficientBaseline, sdk.SeverityInfo
		note = fmt.Sprintf(" The baseline holds fewer than %.0f healthy requests, so the rise is not called a jump: this is a clue, and it holds an incident it already belongs to until recovery is confirmed.", r.MinBaselineRequests) + note
	}
	summary := fmt.Sprintf("operation %s: %.0f of %.0f requests failed in %s (%s)", op.ID, best.failed, best.total, window, best.class)
	if unjudged {
		summary = fmt.Sprintf("operation %s: insufficient baseline to judge a client-error jump (%.0f of %.0f requests failed in %s)", op.ID, best.failed, best.total, window)
	}
	f := sdk.Finding{
		DetectorID: d.cfg.Spec.ID, RuleID: rule, Status: sdk.FindingActive, Severity: severity,
		Summary: summary,
		Evidence: fmt.Sprintf("operation %s (route %q, method %s) had %.0f failed of %.0f requests over %s, %.1f%% against a healthy %.1f%% (%s), on %d consecutive %s steps; newest step at %s.",
			op.ID, op.Route, method, best.failed, best.total, window, best.share*100, best.healthy*100, best.class, r.Consecutive, r.Step, onset.UTC().Format(time.RFC3339)) + note + samples.text(),
		PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: ctx.Namespace(), Name: op.Service},
		Playbooks:       append([]string(nil), d.cfg.Spec.Playbooks...),
		Fingerprint:     fingerprint,
		Metadata: map[string]any{
			"operation": op.ID, "route": op.Route, "method": op.Method, "service": op.Service, "class": best.class,
			"failed": best.failed, "total": best.total, "share": best.share, "healthy_share": best.healthy,
			"window": window, "step": r.Step.String(), "consecutive": r.Consecutive,
			"observed_at": recent.ObservedAt.UTC().Format(time.RFC3339),
		},
	}
	for k, v := range meta {
		f.Metadata[k] = v
	}
	return f, true
}

// errorsFingerprint is the key an operation's error finding, and every clue
// that stands in for it, carries.
func errorsFingerprint(op Operation) string {
	if op.undeclared {
		return "requestmetrics/errors/undeclared/" + op.Service + "/" + op.Route
	}
	return "requestmetrics/errors/" + op.ID
}
