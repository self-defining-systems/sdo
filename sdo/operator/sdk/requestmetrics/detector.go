// Package requestmetrics judges the application's own request metrics per user
// operation. For each operation it counts failed requests (server errors, and
// client errors that jump above their healthy share) over a rolling window and
// reports an operation whose failures are numerous, a real share of its
// requests and far above its healthy share, on consecutive evaluation steps.
//
// The detector is deterministic and does no I/O: the controller freezes the
// declared queries into the detection snapshot (so a recorded snapshot replays
// the same findings), and the package only reads those results. A request
// metric that lacks the declared series or labels is reported as "not
// instrumented", never as silence.
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
}

// DefaultRule is the rule the prototype replayed: at least 5 failed requests
// and 5% of the requests and 5 times the healthy share over 30 s, on two
// consecutive 5 s steps, healthy share floored at 1%.
func DefaultRule() Rule {
	return Rule{Window: 30 * time.Second, Step: 5 * time.Second, Consecutive: 2, MinFailed: 5, MinShare: 0.05, MinRatio: 5, ShareFloor: 0.01, MinBaselineRequests: 20}
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
	// Unmeasurable, when set, says why the map cannot tell this operation's
	// requests apart; the detector then reports it as not instrumented.
	Unmeasurable string
}

// Config configures the detector.
type Config struct {
	Spec       sdk.DetectorSpec
	Entries    []Entry
	Operations []Operation
	Rule       Rule
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
	case r.Window <= 0 || r.Step <= 0 || r.Consecutive < 1 || r.MinFailed < 1 || r.MinShare <= 0 || r.MinRatio < 1 || r.ShareFloor < 0:
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
			recent: metrics.Query{ID: queryID(e.Service, "recent"), Expr: e.Recent, Range: r.Window, Step: r.Step},
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
		plan, ok := d.entries[op.Service]
		if !ok || op.Unmeasurable != "" {
			findings = append(findings, d.notInstrumented(ctx, op, op.Unmeasurable))
			continue
		}
		if why := plan.instrumentation(results); why != "" {
			findings = append(findings, d.notInstrumented(ctx, op, why))
			continue
		}
		if f, ok := d.judge(ctx, op, plan, results); ok {
			findings = append(findings, f)
		}
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].Fingerprint < findings[j].Fingerprint })
	return findings, nil
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
	for _, label := range []string{p.RouteLabel, p.StatusLabel} {
		if _, ok := presence[0].Labels[label]; !ok {
			return fmt.Sprintf("metric %s%s carries no %q label", p.Metric, p.Selector, label)
		}
	}
	return ""
}

func (d *detector) notInstrumented(ctx sdk.DetectionContext, op Operation, why string) sdk.Finding {
	return sdk.Finding{
		DetectorID: d.cfg.Spec.ID, RuleID: RuleNotInstrumented, Status: sdk.FindingActive, Severity: sdk.SeverityInfo,
		Summary:         fmt.Sprintf("operation %s is not instrumented for request metrics", op.ID),
		Evidence:        fmt.Sprintf("operation %s (route %q) cannot be watched: %s", op.ID, op.Route, why),
		PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: ctx.Namespace(), Name: op.Service},
		Fingerprint:     "requestmetrics/not-instrumented/" + op.ID,
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
func (p entryPlan) healthy(op Operation, series []metrics.Series, minRequests float64) (server, client float64) {
	var sum counts
	for _, s := range series {
		if v, ok := s.Last(); ok && p.matches(op, s.Labels) {
			sum = sum.add(p.classify(s.Labels, v.Value))
		}
	}
	if sum.total < minRequests || sum.total == 0 {
		return 0, 0
	}
	return sum.server / sum.total, sum.client / sum.total
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

func (d *detector) judge(ctx sdk.DetectionContext, op Operation, plan entryPlan, results map[string]metrics.Result) (sdk.Finding, bool) {
	r := d.cfg.Rule
	recent := results[plan.recent.ID]
	steps := plan.perStep(op, recent.Series)
	times := make([]time.Time, 0, len(steps))
	for t := range steps {
		times = append(times, t)
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	if len(times) < r.Consecutive {
		return sdk.Finding{}, false
	}
	healthyServer, healthyClient := plan.healthy(op, results[plan.base.ID].Series, r.MinBaselineRequests)
	var best *verdict
	for _, class := range []string{ClassServer, ClassClientJump} {
		held := true
		for _, t := range times[len(times)-r.Consecutive:] {
			if !r.judgeStep(steps[t], class, healthyServer, healthyClient).ok {
				held = false
				break
			}
		}
		if !held {
			continue
		}
		if v := r.judgeStep(steps[times[len(times)-1]], class, healthyServer, healthyClient); best == nil || v.failed > best.failed {
			best = &v
		}
	}
	if best == nil {
		return sdk.Finding{}, false
	}
	method := op.Method
	if method == "" {
		method = "any"
	}
	window := r.Window.String()
	return sdk.Finding{
		DetectorID: d.cfg.Spec.ID, RuleID: RuleErrors, Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
		Summary: fmt.Sprintf("operation %s: %.0f of %.0f requests failed in %s (%s)", op.ID, best.failed, best.total, window, best.class),
		Evidence: fmt.Sprintf("operation %s (route %q, method %s) had %.0f failed of %.0f requests over %s, %.1f%% against a healthy %.1f%% (%s), on %d consecutive %s steps; newest step at %s.",
			op.ID, op.Route, method, best.failed, best.total, window, best.share*100, best.healthy*100, best.class, r.Consecutive, r.Step, times[len(times)-1].UTC().Format(time.RFC3339)),
		PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: ctx.Namespace(), Name: op.Service},
		Playbooks:       append([]string(nil), d.cfg.Spec.Playbooks...),
		Fingerprint:     "requestmetrics/errors/" + op.ID,
		Metadata: map[string]any{
			"operation": op.ID, "route": op.Route, "method": op.Method, "service": op.Service, "class": best.class,
			"failed": best.failed, "total": best.total, "share": best.share, "healthy_share": best.healthy,
			"window": window, "step": r.Step.String(), "consecutive": r.Consecutive,
			"observed_at": recent.ObservedAt.UTC().Format(time.RFC3339),
		},
	}, true
}
