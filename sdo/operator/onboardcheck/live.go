package onboardcheck

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"sdo.dev/operator/memory/appmap"
)

// Names of the two items that run the memory's detectors against the live
// application.
const (
	ItemQuiet = "quiet-when-healthy"
	ItemBreak = "fails-under-break"
)

// Raise is one detector finding that is firing.
type Raise struct {
	DetectorID string
	Rule       string
	Summary    string
	Evidence   string
	// Resources are the names of the objects the finding is about.
	Resources   []string
	Fingerprint string
}

func (r Raise) key() string {
	if r.Fingerprint != "" {
		return r.Fingerprint
	}
	return r.DetectorID + "|" + r.Rule + "|" + r.Summary
}

func (r Raise) text() string {
	parts := []string{r.DetectorID, r.Rule, r.Summary, r.Evidence}
	parts = append(parts, r.Resources...)
	return strings.ToLower(strings.Join(parts, " "))
}

func (r Raise) describe() string {
	out := r.DetectorID
	if r.Rule != "" {
		out += " (" + r.Rule + ")"
	}
	if r.Summary != "" {
		out += ": " + r.Summary
	}
	if r.Evidence != "" {
		out += "; evidence: " + clip(r.Evidence, 300)
	}
	return out
}

func clip(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// Verdict is one evaluation of every detector of the memory against the live
// application. Status is "clear", "firing", "pending" or "unavailable".
type Verdict struct {
	Status      string
	Raised      []Raise
	Unavailable []string
	// Skipped are detectors this environment cannot judge (they need the
	// cluster's own network); they are noted, never red.
	Skipped []string
	// Pending are detectors that have not been able to judge yet, with why
	// ("id: reason"); one still pending when the quiet window ends has not
	// shown the application quiet.
	Pending []string
}

// Evaluator evaluates the memory's detectors, test-request scenarios and the
// request-metrics detector once against the live application.
type Evaluator interface {
	Evaluate(ctx context.Context) (Verdict, error)
}

// QuietConfig is the quiet-when-healthy item.
type QuietConfig struct {
	Eval     Evaluator
	Window   time.Duration
	Interval time.Duration
}

// Breaker breaks and restores one component through Kubernetes.
type Breaker interface {
	// Mode says how components are broken, including any fallback.
	Mode() string
	// Break makes the component unavailable.
	Break(ctx context.Context, c appmap.Component) error
	// Restore undoes Break and verifies the component is back. An error means
	// the restore is uncertain.
	Restore(ctx context.Context, c appmap.Component) error
}

// BreakConfig is the fails-under-break item.
type BreakConfig struct {
	Eval    Evaluator
	Breaker Breaker
	Bound   time.Duration
	// Horizon is the longest the memory's own probes can take to raise a
	// failing scenario; Bound is at least this long. Zero without probes.
	Horizon  time.Duration
	Interval time.Duration
}

// ItemDuration is how long one item took.
type ItemDuration struct {
	Item    string  `json:"item"`
	Seconds float64 `json:"seconds"`
}

func (c Config) validateLive() error {
	if c.Quiet == nil && c.Break == nil {
		return nil
	}
	if c.Now == nil {
		return fmt.Errorf("onboardcheck: Now is required for the live items")
	}
	if q := c.Quiet; q != nil && (q.Eval == nil || q.Window <= 0 || q.Interval <= 0) {
		return fmt.Errorf("onboardcheck: quiet-when-healthy needs an evaluator, a window and an interval")
	}
	if b := c.Break; b != nil && (b.Eval == nil || b.Breaker == nil || b.Bound <= 0 || b.Interval <= 0) {
		return fmt.Errorf("onboardcheck: fails-under-break needs an evaluator, a breaker, a bound and an interval")
	}
	return nil
}

// runLive runs the live items after the metric items and appends their results.
func runLive(ctx context.Context, cfg Config, report *Report) {
	timed := func(item string, f func()) {
		start := cfg.Now()
		f()
		report.Durations = append(report.Durations, ItemDuration{Item: item, Seconds: cfg.Now().Sub(start).Seconds()})
	}
	if cfg.NoDetectors {
		report.add(ItemQuiet, "detectors", Red, "SDO: no detectors were evaluated: the memory SDO would deploy holds none, so silence here proves nothing; author health detectors or traffic workloads")
		if cfg.Break != nil {
			report.add(ItemBreak, "all", Red, "SDO: not run: no detectors were evaluated")
		}
		return
	}
	quietRed := false
	if cfg.Quiet != nil {
		timed(ItemQuiet, func() { quietRed = runQuiet(ctx, cfg, report) })
	}
	if cfg.Break == nil {
		return
	}
	if quietRed {
		report.add(ItemBreak, "all", Note, "not run: the healthy application already raises, so a raise under a break would prove nothing")
		return
	}
	timed(ItemBreak, func() { runBreak(ctx, cfg, report) })
}

func (r *Report) add(item, subject, status, detail string) {
	r.Results = append(r.Results, Result{Item: item, Subject: subject, Status: status, Detail: detail})
}

// runQuiet evaluates the detectors for the whole window with no fault and
// reports whether the healthy application raised or could not be evaluated at all.
func runQuiet(ctx context.Context, cfg Config, report *Report) bool {
	q := cfg.Quiet
	start := cfg.Now()
	end := start.Add(q.Window)
	samples, pending := 0, 0
	var skipped []string
	var stillPending []string
	unavailable := ""
	pendingRed := false
	for {
		verdict, err := q.Eval.Evaluate(ctx)
		if err != nil {
			report.add(ItemQuiet, "detectors", Red, fmt.Sprintf("could not evaluate the detectors after %d samples: %v", samples, err))
			return true
		}
		samples++
		skipped, stillPending = verdict.Skipped, verdict.Pending
		elapsed := cfg.Now().Sub(start).Round(time.Second)
		if len(verdict.Raised) > 0 {
			for _, r := range verdict.Raised {
				report.add(ItemQuiet, r.DetectorID, Red, fmt.Sprintf("raised on the healthy application at sample %d (%s in): %s", samples, elapsed, r.describe()))
			}
			return true
		}
		if verdict.Status == "unavailable" || len(verdict.Unavailable) > 0 {
			// Not a raise: keep sampling the window for the detectors that evaluate,
			// and the fails-under-break item can still run on them.
			if unavailable == "" {
				unavailable = fmt.Sprintf("detectors could not be evaluated (from sample %d): %s", samples, strings.Join(verdict.Unavailable, "; "))
			}
		} else if verdict.Status == "pending" {
			pending++
		}
		remaining := end.Sub(cfg.Now())
		if remaining <= 0 {
			break
		}
		if err := cfg.Wait(ctx, min(q.Interval, remaining)); err != nil {
			report.add(ItemQuiet, "detectors", Red, "interrupted: "+err.Error())
			return true
		}
	}
	if len(skipped) > 0 {
		report.add(ItemQuiet, "skipped", Note, "not judged from here: "+strings.Join(skipped, "; "))
	}
	detail := fmt.Sprintf("no detector raised in %d evaluations over %s", samples, cfg.Now().Sub(start).Round(time.Second))
	for _, p := range stillPending {
		id, reason, _ := strings.Cut(p, ": ")
		report.add(ItemQuiet, id, Red, "still pending when the window ended, so it never judged the healthy application: "+clip(reason, 400))
		pendingRed = true
	}
	if unavailable != "" {
		report.add(ItemQuiet, "detectors", Red, unavailable+"; "+detail+" among the others")
		return false
	}
	if pendingRed {
		return false
	}
	if pending > 0 {
		detail += fmt.Sprintf(" (%d still pending a window)", pending)
	}
	report.add(ItemQuiet, "detectors", Green, detail)
	return false
}

// named reports whether text mentions name as a whole word.
func named(text, name string) bool {
	if name == "" {
		return false
	}
	re := regexp.MustCompile(`(^|[^a-z0-9])` + regexp.QuoteMeta(strings.ToLower(name)) + `($|[^a-z0-9])`)
	return re.MatchString(text)
}

// breakOutcome is what one component's break produced.
type breakOutcome struct {
	// names are what a raise must mention to name the broken component.
	names []string
	how   string
	err   string
	found []foundRaise
	// hit is the first raise that names the component or one of its operations.
	hit *foundRaise
	// opHit is, per operation, the first raise that names the operation or
	// one of its test scenarios: what a test of the operation noticed.
	opHit map[string]*foundRaise
}

type foundRaise struct {
	raise Raise
	after time.Duration
}

// runBreak breaks each component on an operation's path once, in order, and
// reports per component and per operation whether a detector noticed.
func runBreak(ctx context.Context, cfg Config, report *Report) {
	b := cfg.Break
	byName := map[string]appmap.Component{}
	for _, c := range cfg.Map.Components {
		byName[c.Name] = c
	}
	opsOf := map[string][]appmap.Operation{}
	var order []string
	for _, op := range cfg.Map.Operations {
		for _, name := range op.Path {
			if _, seen := opsOf[name]; !seen {
				order = append(order, name)
			}
			opsOf[name] = append(opsOf[name], op)
		}
	}
	outcomes := map[string]*breakOutcome{}
	baseline := map[string]bool{}
	haveBaseline := false
	stopped := false
	for _, name := range order {
		c, ok := byName[name]
		out := &breakOutcome{names: []string{name, c.Workload}, opHit: map[string]*foundRaise{}}
		outcomes[name] = out
		switch {
		case stopped:
			out.err = "not tried: an earlier restore was uncertain"
			continue
		case !ok:
			out.err = "not a declared component"
			continue
		case c.Workload == "":
			report.add(ItemBreak, name, Note, "declares no workload, so it cannot be broken through Kubernetes")
			out.how = "no workload"
			continue
		}
		if !haveBaseline {
			v, err := b.Eval.Evaluate(ctx)
			if err != nil {
				out.err = "could not evaluate before the break: " + err.Error()
				stopped = true
				continue
			}
			haveBaseline = true
			baseline = fingerprints(v.Raised)
		}
		var restoreFailed bool
		baseline, restoreFailed = breakOne(ctx, cfg, c, opsOf[name], baseline, out)
		if restoreFailed || out.err != "" && strings.HasPrefix(out.err, "evaluation failed") {
			stopped = true
		}
	}
	for _, name := range order {
		out := outcomes[name]
		switch {
		case out.how == "no workload":
		case out.err != "":
			report.add(ItemBreak, name, Red, out.err)
		case out.hit != nil:
			f := out.hit
			report.add(ItemBreak, name, Green, fmt.Sprintf("%s: %s noticed after %s: %s", out.how, f.raise.DetectorID, f.after.Round(time.Second), clip(f.raise.Summary, 200)))
		default:
			report.add(ItemBreak, name, Red, fmt.Sprintf("%s: no detector named %s or its operations (%s) within %s%s", out.how, name, strings.Join(opIDs(opsOf[name]), ", "), b.Bound, otherRaises(out)))
		}
	}
	for _, op := range cfg.Map.Operations {
		report.add(ItemBreak, op.ID, opStatus(op, outcomes), opDetail(cfg.Map, op, outcomes, b.Bound, b.Horizon))
	}
}

// otherRaises lists the detectors that raised under the break without naming
// the component or its operations.
func otherRaises(out *breakOutcome) string {
	var ids []string
	for _, f := range out.found {
		ids = append(ids, f.raise.DetectorID)
	}
	if len(ids) == 0 {
		return ""
	}
	return "; raised without naming it: " + strings.Join(ids, ", ")
}

func fingerprints(raised []Raise) map[string]bool {
	out := map[string]bool{}
	for _, r := range raised {
		out[r.key()] = true
	}
	return out
}

func opIDs(ops []appmap.Operation) []string {
	ids := make([]string, len(ops))
	for i, op := range ops {
		ids[i] = op.ID
	}
	return ids
}

// scenariosOf lists the probe scenarios that exercise the operation.
func scenariosOf(m appmap.Map, op appmap.Operation) []string {
	var out []string
	for _, p := range m.Probes {
		for _, id := range p.Operations {
			if id == op.ID {
				out = append(out, p.Scenario)
			}
		}
	}
	return out
}

// namesOperation reports whether a raise's text names the operation or one of
// the scenarios that test it.
func namesOperation(text string, m appmap.Map, op appmap.Operation) bool {
	if named(text, op.ID) {
		return true
	}
	for _, s := range scenariosOf(m, op) {
		if named(text, s) {
			return true
		}
	}
	return false
}

// opStatus is green when breaking some component on the operation's path made
// a raise that names the operation or one of its test scenarios: a generic
// detector that only names the broken component does not show that a test of
// the operation fails.
func opStatus(op appmap.Operation, outcomes map[string]*breakOutcome) string {
	for _, name := range op.Path {
		if out := outcomes[name]; out != nil && out.opHit[op.ID] != nil {
			return Green
		}
	}
	return Red
}

func opDetail(m appmap.Map, op appmap.Operation, outcomes map[string]*breakOutcome, bound, horizon time.Duration) string {
	var tried, generic []string
	for _, name := range op.Path {
		out := outcomes[name]
		if out == nil || out.how == "no workload" || out.err != "" {
			continue
		}
		if f := out.opHit[op.ID]; f != nil {
			return fmt.Sprintf("breaking %s (%s) was noticed by %s after %s: %s", name, out.how, f.raise.DetectorID, f.after.Round(time.Second), clip(f.raise.Summary, 160))
		}
		tried = append(tried, name)
		if out.hit != nil {
			generic = append(generic, fmt.Sprintf("%s by %s", name, out.hit.raise.DetectorID))
		}
	}
	if len(tried) == 0 {
		return "no component on its path could be broken"
	}
	scenarios := "none declared"
	if s := scenariosOf(m, op); len(s) > 0 {
		scenarios = strings.Join(s, ", ")
	}
	detail := fmt.Sprintf("breaking %s raised nothing that names %s or its test scenarios (%s)", strings.Join(tried, ", "), op.ID, scenarios)
	if len(generic) > 0 {
		detail += "; only generic detectors noticed the component (" + strings.Join(generic, "; ") + ")"
	}
	detail += fmt.Sprintf(": no test or request metric of this operation catches it (waited up to %s per component", bound.Round(time.Second))
	if horizon > 0 {
		detail += fmt.Sprintf("; the memory's probes need up to %s to judge a failure", horizon.Round(time.Second))
	}
	return detail + ")"
}

// breakOne breaks c, waits up to the bound for a raise that names it or one
// of its operations, and always restores it. It returns the baseline for the
// next component and whether the restore was uncertain.
func breakOne(ctx context.Context, cfg Config, c appmap.Component, ops []appmap.Operation, baseline map[string]bool, out *breakOutcome) (next map[string]bool, restoreFailed bool) {
	b := cfg.Break
	out.how = b.Breaker.Mode()
	next = baseline
	if err := b.Breaker.Break(ctx, c); err != nil {
		// A failed break may have changed the cluster; restore regardless.
		if rerr := b.Breaker.Restore(context.WithoutCancel(ctx), c); rerr != nil {
			out.err = fmt.Sprintf("break failed (%v) and restore is uncertain: %v", err, rerr)
			return next, true
		}
		out.err = "could not break: " + err.Error()
		return next, false
	}
	defer func() {
		if rerr := b.Breaker.Restore(context.WithoutCancel(ctx), c); rerr != nil {
			out.err = fmt.Sprintf("restore of %s is uncertain, stopping: %v", c.Name, rerr)
			restoreFailed = true
			return
		}
		next = waitClear(ctx, cfg, next)
	}()
	start := cfg.Now()
	seen := map[string]bool{}
	for {
		v, err := b.Eval.Evaluate(ctx)
		if err != nil {
			out.err = "evaluation failed while broken: " + err.Error()
			return next, false
		}
		after := cfg.Now().Sub(start)
		for _, r := range v.Raised {
			if baseline[r.key()] || seen[r.key()] {
				continue
			}
			seen[r.key()] = true
			f := foundRaise{raise: r, after: after}
			out.found = append(out.found, f)
			text := r.text()
			mine := named(text, c.Name) || named(text, c.Workload)
			for _, op := range ops {
				if namesOperation(text, cfg.Map, op) {
					mine = true
					if out.opHit[op.ID] == nil {
						out.opHit[op.ID] = &f
					}
				}
			}
			if mine && out.hit == nil {
				out.hit = &f
			}
		}
		// A generic detector names the component within seconds; the tests of
		// its operations need their own window, so wait for those up to the bound.
		tested := true
		for _, op := range ops {
			tested = tested && out.opHit[op.ID] != nil
		}
		if tested || after >= b.Bound {
			return next, false
		}
		if err := cfg.Wait(ctx, b.Interval); err != nil {
			out.err = "interrupted: " + err.Error()
			return next, false
		}
	}
}

// waitClear waits up to the bound for the detectors to clear after a restore
// and returns the raises still standing, which later breaks must not count.
func waitClear(ctx context.Context, cfg Config, fallback map[string]bool) map[string]bool {
	b := cfg.Break
	start := cfg.Now()
	for {
		v, err := b.Eval.Evaluate(ctx)
		if err != nil {
			return fallback
		}
		if len(v.Raised) == 0 || cfg.Now().Sub(start) >= b.Bound {
			return fingerprints(v.Raised)
		}
		if err := cfg.Wait(ctx, b.Interval); err != nil {
			return fingerprints(v.Raised)
		}
	}
}
