package onboardcheck

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
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
	// Operation is the declared operation a request-metrics finding is about,
	// when the detector says so.
	Operation string
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

// warmupRules are raises of SDO's own detectors that say they cannot judge yet
// (a request-metrics baseline that has not filled on a fresh install). They
// are SDO behaviour, not a fault in the memory: the quiet item notes them and
// a break never counts one as naming an operation.
var warmupRules = map[string]bool{"request-metrics.insufficient-baseline": true}

func (r Raise) warmup() bool { return warmupRules[r.Rule] }

var absenceSignal = regexp.MustCompile(`(?i)(not[ -]instrumented|no traffic|no[- ]ready|no ready|missing|absent|vanish|disappear|no series|no data|not found)`)

// Kinds of signal that named an operation under a break.
const (
	// SignalFailure is evidence that requests of the operation failed (an error
	// rate, a failing probe scenario).
	SignalFailure = "failure"
	// SignalAbsence is evidence that the operation, its series or its endpoints
	// are gone: it names the operation but not a failing request.
	SignalAbsence = "absence"
)

// Rules whose kind does not depend on their wording: a scenario finding is a
// failing probe (whatever its steps say, "not found" included), the
// request-metrics instrumentation rule is only an absence.
const (
	notInstrumentedRule = "request-metrics.not-instrumented"
	scenarioRulePrefix  = "scenario-slo."
)

// signalKind says which kind of signal a raise is.
func signalKind(r Raise) string {
	switch {
	case r.Rule == notInstrumentedRule:
		return SignalAbsence
	case strings.HasPrefix(r.Rule, scenarioRulePrefix):
		return SignalFailure
	case absenceSignal.MatchString(r.Rule + " " + r.Summary):
		return SignalAbsence
	}
	return SignalFailure
}

// signalNote says which failing-request signal named the operation.
func signalNote(out *breakOutcome, opID string) string {
	f := out.opFailure[opID]
	if f == nil {
		// A generic break whose own fault is a silence (a quiet worker) is
		// judged on absence; component breaks never reach here without a failure.
		return "; absence signal only: no detector reported failing requests of this operation"
	}
	return fmt.Sprintf("; failing-request signal: %s after %s", f.raise.DetectorID, f.after.Round(time.Second))
}

// notInstrumentedOps lists the declared operations a request-metrics
// not-instrumented raise is about, with the line that names it. The map is
// keyed by operation ID.
func notInstrumentedOps(m appmap.Map, raised []Raise) map[string]string {
	out := map[string]string{}
	for _, r := range raised {
		if r.Rule != notInstrumentedRule {
			continue
		}
		for _, op := range m.Operations {
			if r.Operation == op.ID || (r.Operation == "" && named(r.text(), op.ID)) {
				if _, seen := out[op.ID]; !seen {
					out[op.ID] = fmt.Sprintf("request-metrics reports operation %s is not instrumented for request metrics, so no error or latency series can tell it apart: %s", op.ID, clip(r.Evidence+" "+r.Summary, 300))
				}
			}
		}
	}
	return out
}

// markNotInstrumented records the operations the raises report as not
// instrumented on the healthy application; Run turns them red in
// operations-observed and latency-per-operation, reasons notwithstanding.
func (r *Report) markNotInstrumented(m appmap.Map, raised []Raise) {
	for id, line := range notInstrumentedOps(m, raised) {
		if r.notInstrumented == nil {
			r.notInstrumented = map[string]string{}
		}
		r.notInstrumented[id] = line
	}
}

// applyNotInstrumented makes the reported operations red in both items,
// replacing a green line the metric items wrote.
func (r *Report) applyNotInstrumented(m appmap.Map) {
	for _, op := range m.Operations {
		line, bad := r.notInstrumented[op.ID]
		if !bad {
			continue
		}
		for _, item := range []string{ItemObserved, ItemLatency} {
			set := false
			for i := range r.Results {
				if x := &r.Results[i]; x.Item == item && x.Subject == op.ID {
					x.Status, x.Detail, set = Red, line, true
				}
			}
			if !set {
				r.add(item, op.ID, Red, line)
			}
		}
	}
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
	// Progress is how far each detector that has judged got, when the
	// evaluator can say. Without it the quiet window runs to its full length.
	Progress []DetectorProgress
}

// DetectorProgress is one detector's judging so far: its cadence, how long
// after a condition resolves a fresh judgement may still report it (Horizon),
// how often it judged and how often at least a Horizon after its first.
type DetectorProgress struct {
	DetectorID  string
	Interval    time.Duration
	Horizon     time.Duration
	Evaluations int
	PastHorizon int
}

// DefaultQuietEvaluations is how many times each detector must have judged the
// healthy application past its own horizon for the quiet window to end.
const DefaultQuietEvaluations = 3

// Evaluator evaluates the memory's detectors, test-request scenarios and the
// request-metrics detector once against the live application.
type Evaluator interface {
	Evaluate(ctx context.Context) (Verdict, error)
}

// QuietConfig is the quiet-when-healthy item.
type QuietConfig struct {
	Eval Evaluator
	// Window is the longest the item observes. With progress from the evaluator
	// the item ends as soon as every detector has judged Evaluations times past
	// its own horizon (a quick run: once each has judged at all).
	Window   time.Duration
	Interval time.Duration
	// Evaluations is that count; zero means DefaultQuietEvaluations.
	Evaluations int
	// DetectorKey keys a detector by the inputs that decide its healthy
	// verdict. With Cache, a detector that judged the healthy application
	// quiet on the same key needs only one judgement past its horizon (a
	// short window for the whole set); a raise is red whatever was recorded.
	DetectorKey func(id string) string
	// Cache holds the detectors that judged quiet in earlier runs; Reuse lets
	// this run take them (--full clears it). A green run records its detectors.
	Cache *Cache
	Reuse bool
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
	// Faults are the generic breaks that keep every component up, judged after
	// the component breaks.
	Faults []Fault
	// Keys keys each item (a component, a generic fault) by the inputs that
	// decide its verdict; an item without a key is always broken.
	Keys map[string]string
	// Cache holds the verdicts of earlier runs. A green verdict recorded for
	// the same key is reused when Reuse is set; every fresh green one is
	// recorded either way. Nil records nothing.
	Cache *Cache
	// Reuse lets a run take verdicts from Cache (the default; --full clears it).
	Reuse bool
	// OneAtATime breaks every component and fault in a wave of its own, even
	// ones that share no operation: the slower path the waves must agree with.
	OneAtATime bool
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
	if q := c.Quiet; q != nil && (q.Eval == nil || q.Window <= 0 || q.Interval <= 0 || q.Evaluations < 0) {
		return fmt.Errorf("onboardcheck: quiet-when-healthy needs an evaluator, a window and an interval")
	}
	if b := c.Break; b != nil && (b.Eval == nil || b.Breaker == nil || b.Bound <= 0 || b.Interval <= 0) {
		return fmt.Errorf("onboardcheck: fails-under-break needs an evaluator, a breaker, a bound and an interval")
	}
	return nil
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
	need := needs{n: q.Evaluations}
	if need.n < 1 {
		need.n = DefaultQuietEvaluations
	}
	if cfg.Quick {
		need.n = 1
	}
	var progress []DetectorProgress
	warm := map[string]string{}
	announced := false
	var lastSaid string
	var lastSaidAt time.Duration
	for {
		verdict, err := q.Eval.Evaluate(ctx)
		if err != nil {
			report.add(ItemQuiet, "detectors", Red, fmt.Sprintf("could not evaluate the detectors after %d samples: %v", samples, err))
			return true
		}
		samples++
		skipped, stillPending = verdict.Skipped, verdict.Pending
		progress = verdict.Progress
		need.reused = reusedDetectors(q, progress)
		if len(need.reused) > 0 {
			report.Mode = ModeIncremental
		}
		if len(progress) > 0 && !announced {
			announced = true

			cfg.say("quiet: %s", derivedQuietWindow(progress, need, q.Window, cfg.Quick))
		}
		elapsed := cfg.Now().Sub(start).Round(time.Second)
		verdict.Raised, warm = splitWarmup(verdict.Raised, warm)
		if len(verdict.Raised) > 0 {
			report.markNotInstrumented(cfg.Map, verdict.Raised)
			for _, r := range verdict.Raised {
				if q.Cache != nil {
					delete(q.Cache.Detectors, r.DetectorID)
				}
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
		if len(verdict.Unavailable) == 0 && verdict.Status != "unavailable" && len(verdict.Pending) == 0 && judgedEnough(progress, need, cfg.Quick) {
			break
		}
		if summary := progressSummary(progress, need, cfg.Quick); summary != lastSaid || elapsed-lastSaidAt >= 30*time.Second {
			cfg.say("quiet: sample %d at %s; %s", samples, elapsed, summary)
			lastSaid, lastSaidAt = summary, elapsed
		}
		remaining := end.Sub(cfg.Now())
		if remaining <= 0 {
			break
		}
		wait := q.Interval
		if len(progress) > 0 {
			wait = min(wait, 5*time.Second)
		}
		if err := cfg.Wait(ctx, min(wait, remaining)); err != nil {
			report.add(ItemQuiet, "detectors", Red, "interrupted: "+err.Error())
			return true
		}
	}
	for _, w := range sortedValues(warm) {
		report.add(ItemQuiet, "sdo-warm-up", Note, "SDO's own warm-up, not a fault in the memory (it clears as baselines fill): "+w)
	}
	if len(skipped) > 0 {
		report.add(ItemQuiet, "skipped", Note, "not judged from here: "+strings.Join(skipped, "; "))
	}
	detail := fmt.Sprintf("no detector raised in %d evaluations over %s", samples, cfg.Now().Sub(start).Round(time.Second))
	if len(progress) > 0 {
		detail += "; " + progressSummary(progress, need, cfg.Quick)
	}
	insufficient := false
	for _, p := range progress {
		if !judgedEnough([]DetectorProgress{p}, need, cfg.Quick) {
			report.add(ItemQuiet, p.DetectorID, Red, fmt.Sprintf("judged %d times (%d past its %s horizon) in the %s window; needs %d past the horizon: raise --quiet-window", p.Evaluations, p.PastHorizon, p.Horizon.Round(time.Second), q.Window, need.of(p.DetectorID)))
			insufficient = true
			if q.Cache != nil {
				delete(q.Cache.Detectors, p.DetectorID)
			}
		}
	}
	if cfg.Quick && len(progress) == 0 && len(stillPending) == 0 && unavailable == "" {
		report.add(ItemQuiet, "detectors", Red, "no detector was evaluated")
		return false
	}
	for _, p := range stillPending {
		id, reason, _ := strings.Cut(p, ": ")
		pendingRed = true
		if scenarios := pendingScenarios(reason); len(scenarios) > 0 {
			for _, sc := range scenarios {
				report.add(ItemQuiet, sc.id, Red, fmt.Sprintf("probe scenario %s (detector %s) %s through the healthy window, so it cannot give a verdict: %s%s", sc.id, id, sc.why, sc.remedy, sc.detail))
			}
			continue
		}
		report.add(ItemQuiet, id, Red, "still pending when the window ended, so it never judged the healthy application: "+clip(reason, 400))
	}
	if unavailable != "" {
		report.add(ItemQuiet, "detectors", Red, unavailable+"; "+detail+" among the others")
		return false
	}
	if pendingRed || insufficient {
		return false
	}
	if pending > 0 {
		detail += fmt.Sprintf(" (%d still pending a window)", pending)
	}
	if len(need.reused) > 0 {
		report.Mode = ModeIncremental
		detail += fmt.Sprintf("; %d detectors unchanged since their clean healthy window judged once (%s)", len(need.reused), reusedLabel(oldestOf(need.reused)))
	}
	recordDetectors(q, progress, need.reused, cfg.Now())
	report.add(ItemQuiet, "detectors", Green, detail)
	return false
}

// splitWarmup moves SDO's warm-up raises out of the list into warm (keyed by
// fingerprint) and returns the rest.
func splitWarmup(raised []Raise, warm map[string]string) ([]Raise, map[string]string) {
	var rest []Raise
	for _, r := range raised {
		if r.warmup() {
			warm[r.key()] = r.describe()
			continue
		}
		rest = append(rest, r)
	}
	return rest, warm
}

func sortedValues(m map[string]string) []string {
	var out []string
	for _, v := range m {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// needs is how many judgements past its horizon each detector must make: n,
// or one for a detector whose verdict is reused.
type needs struct {
	n      int
	reused map[string]time.Time
}

func (n needs) of(id string) int {
	if _, ok := n.reused[id]; ok {
		return 1
	}
	return n.n
}

// judgedEnough reports whether every detector has judged need times past its own
// horizon (a quick run: need times at all). No progress means nothing is known.
func judgedEnough(progress []DetectorProgress, need needs, quick bool) bool {
	if len(progress) == 0 {
		return false
	}
	for _, p := range progress {
		got := p.PastHorizon
		if quick {
			got = p.Evaluations
		}
		if got < need.of(p.DetectorID) {
			return false
		}
	}
	return true
}

func progressSummary(progress []DetectorProgress, need needs, quick bool) string {
	done := 0
	for _, p := range progress {
		if judgedEnough([]DetectorProgress{p}, need, quick) {
			done++
		}
	}
	what := "past its horizon"
	if quick {
		what = "at all"
	}
	if len(need.reused) > 0 {
		return fmt.Sprintf("%d of %d detectors judged %d times %s (%d reused detectors need one)", done, len(progress), need.n, what, len(need.reused))
	}
	return fmt.Sprintf("%d of %d detectors judged %d times %s", done, len(progress), need.n, what)
}

// derivedQuietWindow says how long the item expects to observe: the longest
// horizon plus the extra evaluations at that detector's cadence, capped by the
// window.
func derivedQuietWindow(progress []DetectorProgress, need needs, window time.Duration, quick bool) string {
	var longest time.Duration
	var by string
	for _, p := range progress {
		d := p.Horizon + time.Duration(need.of(p.DetectorID)-1)*p.Interval
		if quick {
			d = 0
		}
		if d > longest || by == "" {
			longest, by = d, p.DetectorID
		}
	}
	if quick {
		return fmt.Sprintf("quick run, ends once each of %d detectors has judged (at most %s)", len(progress), window)
	}
	text := fmt.Sprintf("derived window %s: %d judgements past the horizon of %d detectors, longest %s", longest.Round(time.Second), need.n, len(progress), by)
	if len(need.reused) > 0 {
		text += fmt.Sprintf("; %d detectors unchanged since a clean run need one", len(need.reused))
	}
	if longest > window {
		text += fmt.Sprintf(" (more than the %s cap; detectors that fall short are red)", window)
	}
	return text
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
	// opFailure is, per operation, the first raise that names it with a
	// failing-request signal (not only an absence).
	opFailure map[string]*foundRaise
	// stray is, per operation whose path does not list this component, the
	// first failing-request signal that named it while the component was
	// broken: a missing edge. wave lists the components broken together.
	stray map[string]*foundRaise
	wave  []string
	// reusedAt is when a reused outcome was recorded; zero for a fresh one.
	reusedAt time.Time
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
	dirty := false // a restored wave's raises may still stand

	// The generic faults join the plan as members that hurt the operations they
	// name: one that shares no operation with a component wave is judged in it.
	items := make([]planItem, 0, len(order))
	reused := 0
	for _, name := range order {
		if out := cachedOutcome(b, name); out != nil && byName[name].Workload != "" {
			outcomes[name] = out
			reused++
			continue
		}
		items = append(items, planItem{name: name, ops: opIDs(opsOf[name])})
	}
	faultOut := map[string]*breakOutcome{}
	faultOps := map[string][]string{}
	var faults []Fault
	for _, f := range b.Faults {
		subject := f.Name()
		ids := f.Operations()
		var ops []appmap.Operation
		for _, id := range ids {
			for _, op := range cfg.Map.Operations {
				if op.ID == id {
					ops = append(ops, op)
				}
			}
		}
		if len(ops) == 0 {
			report.add(ItemBreak, subject, Note, "no operation depends on what this break targets, so nothing could be hurt")
			continue
		}
		if why := f.NotApplicable(ctx); why != "" {
			report.add(ItemBreak, subject, Note, "not run: "+why)
			continue
		}
		faults = append(faults, f)
		faultOps[subject] = ids
		if out := cachedOutcome(b, subject); out != nil {
			faultOut[subject] = out
			reused++
			continue
		}
		items = append(items, planItem{name: subject, ops: opIDs(ops), fault: f})
	}
	waves := planWaves(items, b.OneAtATime)
	cfg.say("fails-under-break: %d components and %d generic breaks in %d waves (breaks that share an operation are never made together); %d verdicts reused from an earlier run", len(order), len(faults), len(waves), reused)
	if reused > 0 && report.Mode == ModeFull {
		report.Mode = ModeIncremental
	}
	step := 0
	// doWave breaks the planned items together. Components that cannot be
	// broken get their outcome here.
	doWave := func(wave []planItem) {
		step++
		var members []waveMember
		var names []string
		for _, it := range wave {
			names = append(names, it.name)
			if it.fault != nil {
				if stopped {
					report.add(ItemBreak, it.name, Red, "not tried: an earlier restore was uncertain")
					continue
				}
				out := &breakOutcome{names: []string{it.name}, opHit: map[string]*foundRaise{}}
				faultOut[it.name] = out
				var ops []appmap.Operation
				for _, op := range cfg.Map.Operations {
					if slices.Contains(it.ops, op.ID) {
						ops = append(ops, op)
					}
				}
				members = append(members, waveMember{c: appmap.Component{Name: it.name}, ops: ops, out: out, breaker: faultBreaker{it.fault}})
				continue
			}
			c, ok := byName[it.name]
			out := &breakOutcome{names: []string{it.name, c.Workload}, opHit: map[string]*foundRaise{}}
			outcomes[it.name] = out
			switch {
			case stopped:
				out.err = "not tried: an earlier restore was uncertain"
			case !ok:
				out.err = "not a declared component"
			case c.Workload == "":
				report.add(ItemBreak, it.name, Note, "declares no workload, so it cannot be broken through Kubernetes")
				out.how = "no workload"
			default:
				members = append(members, waveMember{c: c, ops: opsOf[it.name], out: out})
			}
		}
		if len(members) == 0 {
			return
		}
		if !haveBaseline {
			v, err := b.Eval.Evaluate(ctx)
			if err != nil {
				for _, m := range members {
					m.out.err = "could not evaluate before the break: " + err.Error()
				}
				stopped = true
				return
			}
			haveBaseline = true
			baseline = fingerprints(v.Raised)
			report.markNotInstrumented(cfg.Map, v.Raised)
		}
		cfg.say("fails-under-break: wave %d of %d: breaking %s", step, len(waves), strings.Join(names, ", "))
		if dirty {
			baseline = settleBaseline(ctx, cfg, baseline)
		}
		dirty = true
		var restoreFailed bool
		baseline, restoreFailed = breakWave(ctx, cfg, members, baseline)
		for _, m := range members {
			if strings.HasPrefix(m.out.err, "evaluation failed") {
				restoreFailed = true
			}
		}
		if restoreFailed {
			stopped = true
		}
	}
	for _, wave := range waves {
		doWave(wave)
	}
	for _, f := range faults {
		if out := faultOut[f.Name()]; out != nil {
			reportFault(report, f, out, faultOps[f.Name()], b.Bound)
		}
	}
	recordBreaks(b, outcomes, faultOut, cfg.Now())
	for _, name := range order {
		out := outcomes[name]
		switch {
		case out.how == "no workload":
		case out.err != "":
			report.add(ItemBreak, name, Red, out.err)
		case out.hit != nil:
			f := out.hit
			report.add(ItemBreak, name, Green, fmt.Sprintf("%s: %s noticed after %s (%s signal): %s%s", out.how, f.raise.DetectorID, f.after.Round(time.Second), signalKind(f.raise), clip(f.raise.Summary, 200), reusedSuffix(out)))
		default:
			report.add(ItemBreak, name, Red, fmt.Sprintf("%s: no detector named %s or its operations (%s) within %s%s", out.how, name, strings.Join(opIDs(opsOf[name]), ", "), b.Bound, otherRaises(out)))
		}
	}
	for _, op := range cfg.Map.Operations {
		report.add(ItemBreak, op.ID, opStatus(op, outcomes), opDetail(cfg.Map, op, outcomes, b.Bound, b.Horizon)+opReuseNote(op, outcomes))
	}
	judgeMapAccuracy(cfg.Map, order, outcomes, report)
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
// a failure signal name the operation or one of its test scenarios (failing
// requests, a failing probe step). A generic detector that only names the
// broken component, or a raise that only says the operation's series or
// endpoints are absent, does not show that a test of the operation fails.
func opStatus(op appmap.Operation, outcomes map[string]*breakOutcome) string {
	for _, name := range op.Path {
		if out := outcomes[name]; out != nil && out.opFailure[op.ID] != nil {
			return Green
		}
	}
	return Red
}

func opDetail(m appmap.Map, op appmap.Operation, outcomes map[string]*breakOutcome, bound, horizon time.Duration) string {
	var tried, generic, absent []string
	for _, name := range op.Path {
		out := outcomes[name]
		if out == nil || out.how == "no workload" || out.err != "" {
			continue
		}
		if f := out.opFailure[op.ID]; f != nil {
			return fmt.Sprintf("breaking %s (%s) was noticed by %s after %s: %s%s", name, out.how, f.raise.DetectorID, f.after.Round(time.Second), clip(f.raise.Summary, 160), signalNote(out, op.ID))
		}
		if f := out.opHit[op.ID]; f != nil {
			absent = append(absent, fmt.Sprintf("%s after %s on breaking %s: %s", f.raise.DetectorID, f.after.Round(time.Second), name, clip(f.raise.Summary, 160)))
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
	if len(absent) > 0 {
		detail += "; absence signal only, no detector reported failing requests or a failing probe step of this operation (" + strings.Join(absent, "; ") + ")"
	}
	detail += fmt.Sprintf(": no test or request metric of this operation catches it (waited up to %s per component", bound.Round(time.Second))
	if horizon > 0 {
		detail += fmt.Sprintf("; the memory's probes need up to %s to judge a failure", horizon.Round(time.Second))
	}
	return detail + ")"
}

// BreakWaves groups the components on operation paths into waves that are
// broken together: components in one wave share no operation, so a raise that
// names an operation still names one broken component. Order within and across
// waves follows first appearance in the operations.
func BreakWaves(m appmap.Map) [][]string {
	var items []planItem
	for _, name := range pathComponents(m) {
		items = append(items, planItem{name: name, ops: OperationsThrough(m, name)})
	}
	var out [][]string
	for _, wave := range planWaves(items, false) {
		var names []string
		for _, it := range wave {
			names = append(names, it.name)
		}
		out = append(out, names)
	}
	return out
}

// pathComponents lists the components on operation paths in first-appearance
// order.
func pathComponents(m appmap.Map) []string {
	var order []string
	seen := map[string]bool{}
	for _, op := range m.Operations {
		for _, name := range op.Path {
			if !seen[name] {
				seen[name] = true
				order = append(order, name)
			}
		}
	}
	return order
}

// planItem is one break the check makes: a component or a generic fault, with
// the operations it can move.
type planItem struct {
	name  string
	ops   []string
	fault Fault
}

// planWaves groups items into waves, first fit: an item joins the earliest wave
// none of whose items shares an operation with it, so a raise naming an
// operation is attributable to the one broken item on it. Items that share an
// operation are never broken together: a guess that they can be (and a
// fallback to one break each when an operation fails) costs a whole extra wave
// whenever the guess is wrong, and breaking a component on an operation's path
// nearly always fails the operation. oneAtATime puts every item in a wave of
// its own.
func planWaves(items []planItem, oneAtATime bool) [][]planItem {
	var waves [][]planItem
	var used []map[string]bool
	for _, it := range items {
		placed := false
		for i := range waves {
			if oneAtATime || slices.ContainsFunc(it.ops, func(id string) bool { return used[i][id] }) {
				continue
			}
			waves[i] = append(waves[i], it)
			for _, id := range it.ops {
				used[i][id] = true
			}
			placed = true
			break
		}
		if !placed {
			waves = append(waves, []planItem{it})
			ids := map[string]bool{}
			for _, id := range it.ops {
				ids[id] = true
			}
			used = append(used, ids)
		}
	}
	return waves
}

// PlannedBreakWaves is the number of component waves a check plans.
func PlannedBreakWaves(m appmap.Map) int {
	return len(BreakWaves(m))
}

type waveMember struct {
	c   appmap.Component
	ops []appmap.Operation
	out *breakOutcome
	// breaker overrides the check's breaker (a generic fault breaks itself).
	breaker Breaker
}

func (m waveMember) breakerOf(b *BreakConfig) Breaker {
	if m.breaker != nil {
		return m.breaker
	}
	return b.Breaker
}

// inParallel runs f for every member at once and waits for all.
func inParallel(members []waveMember, f func(waveMember)) {
	var wg sync.WaitGroup
	for _, m := range members {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f(m)
		}()
	}
	wg.Wait()
}

// breakWave breaks every member at once, waits up to the bound for each to be
// named by a raise (the component, or one of its operations or their test
// scenarios), and always restores them all. It returns the baseline for the
// next wave and whether a restore was uncertain.
func breakWave(ctx context.Context, cfg Config, members []waveMember, baseline map[string]bool) (next map[string]bool, restoreFailed bool) {
	b := cfg.Break
	for _, m := range members {
		for _, o := range members {
			if o.breaker == nil { // the components broken together, not a generic fault
				m.out.wave = append(m.out.wave, o.c.Name)
			}
		}
	}
	next = baseline
	var broken []waveMember
	var mu sync.Mutex
	inParallel(members, func(m waveMember) {
		brk := m.breakerOf(b)
		m.out.how = brk.Mode()
		err := brk.Break(ctx, m.c)
		if err == nil {
			mu.Lock()
			broken = append(broken, m)
			mu.Unlock()
			return
		}
		// A failed break may have changed the cluster; restore regardless.
		mu.Lock()
		defer mu.Unlock()
		if rerr := m.breakerOf(b).Restore(context.WithoutCancel(ctx), m.c); rerr != nil {
			m.out.err = fmt.Sprintf("break failed (%v) and restore is uncertain: %v", err, rerr)
			restoreFailed = true
			return
		}
		m.out.err = "could not break: " + err.Error()
	})
	waveStart := cfg.Now()
	var judged time.Duration
	defer func() {
		cfg.phases.add(PhaseBreakJudge, judged)
		inParallel(broken, func(m waveMember) {
			if rerr := m.breakerOf(b).Restore(context.WithoutCancel(ctx), m.c); rerr != nil {
				mu.Lock()
				m.out.err = fmt.Sprintf("restore of %s is uncertain, stopping: %v", m.c.Name, rerr)
				restoreFailed = true
				mu.Unlock()
			}
		})
		restored := cfg.Now().Sub(waveStart)
		cfg.phases.add(PhaseBreakRestore, restored-judged)
		cfg.say("fails-under-break: restored after %s", restored.Round(time.Second))
	}()
	if len(broken) == 0 {
		return next, restoreFailed
	}
	start := cfg.Now()
	seen := map[string]bool{}
	for {
		v, err := b.Eval.Evaluate(ctx)
		if err != nil {
			for _, m := range broken {
				m.out.err = "evaluation failed while broken: " + err.Error()
			}
			return next, restoreFailed
		}
		after := cfg.Now().Sub(start)
		for _, r := range v.Raised {
			if baseline[r.key()] || seen[r.key()] || r.warmup() {
				continue
			}
			seen[r.key()] = true
			f := foundRaise{raise: r, after: after}
			text := r.text()
			for _, m := range broken {
				m.out.found = append(m.out.found, f)
				mine := named(text, m.c.Name) || named(text, m.c.Workload)
				for _, op := range m.ops {
					if namesOperation(text, cfg.Map, op) {
						mine = true
						if m.out.opHit[op.ID] == nil {
							m.out.opHit[op.ID] = &f
						}
						if signalKind(r) == SignalFailure && m.out.opFailure[op.ID] == nil {
							if m.out.opFailure == nil {
								m.out.opFailure = map[string]*foundRaise{}
							}
							m.out.opFailure[op.ID] = &f
						}
					}
				}
				for _, op := range cfg.Map.Operations {
					if onWavePath(op, broken) || !namesOperation(text, cfg.Map, op) || signalKind(r) != SignalFailure || m.out.stray[op.ID] != nil {
						continue
					}
					if m.out.stray == nil {
						m.out.stray = map[string]*foundRaise{}
					}
					m.out.stray[op.ID] = &f
				}
				if mine && m.out.hit == nil {
					m.out.hit = &f
				}
			}
		}
		// A generic detector names the component within seconds; the tests of
		// its operations need their own window, so wait for those up to the bound.
		tested := true
		for _, m := range broken {
			for _, op := range m.ops {
				tested = tested && m.out.opFailure[op.ID] != nil
			}
		}
		if tested || after >= b.Bound {
			judged = after
			cfg.say("fails-under-break: wave judged after %s (bound %s, every operation failed a request or probe: %v)", after.Round(time.Second), b.Bound, tested)
			return next, restoreFailed
		}
		if err := cfg.Wait(ctx, b.Interval); err != nil {
			for _, m := range broken {
				m.out.err = "interrupted: " + err.Error()
			}
			return next, restoreFailed
		}
	}
}

// settleBaseline waits for the raises a restored wave left to clear, as the
// next break (or fault) starts from them: only a later break needs them
// cleared, so the final wave never pays for it.
func settleBaseline(ctx context.Context, cfg Config, baseline map[string]bool) map[string]bool {
	start := cfg.Now()
	next := waitClear(ctx, cfg, baseline)
	settled := cfg.Now().Sub(start)
	cfg.phases.add(PhaseBreakSettle, settled)
	cfg.say("fails-under-break: detectors settled after %s", settled.Round(time.Second))
	return next
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

// Phases of the check that the report times separately, so a trial can see
// which one dominates its wall time.
const (
	PhaseMetrics      = "metric-items"
	PhaseBreakJudge   = "fails-under-break:judge"
	PhaseBreakRestore = "fails-under-break:restore"
	PhaseBreakSettle  = "fails-under-break:settle"
)

// phaseLog sums the time spent per phase across waves.
type phaseLog struct {
	mu    sync.Mutex
	order []string
	total map[string]time.Duration
}

func (p *phaseLog) add(name string, d time.Duration) {
	if p == nil || d <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.total == nil {
		p.total = map[string]time.Duration{}
	}
	if _, ok := p.total[name]; !ok {
		p.order = append(p.order, name)
	}
	p.total[name] += d
}

func (p *phaseLog) durations() []ItemDuration {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []ItemDuration
	for _, name := range p.order {
		out = append(out, ItemDuration{Item: name, Seconds: p.total[name].Seconds()})
	}
	return out
}

// timingSummary is the one line that lists every timed phase.
func timingSummary(ds []ItemDuration) string {
	var parts []string
	for _, d := range ds {
		parts = append(parts, fmt.Sprintf("%s %s", d.Item, time.Duration(d.Seconds*float64(time.Second)).Round(time.Second)))
	}
	return strings.Join(parts, ", ")
}

// onWavePath reports whether the operation's path lists any component broken
// in the wave: a failure of it is then explained.
func onWavePath(op appmap.Operation, wave []waveMember) bool {
	for _, m := range wave {
		if onPath(op, m.c.Name) || slices.ContainsFunc(m.ops, func(o appmap.Operation) bool { return o.ID == op.ID }) {
			return true
		}
	}
	return false
}

type pendingScenario struct {
	id, why, detail, remedy string
}

const (
	underSampledMarker = "insufficient usable synthetic samples: "
	unprovenRemedy     = "fix what its latest failure names, then re-run"
	underSampleRemedy  = "make each iteration cheaper and sample often enough to qualify (enough usable samples inside its window, within about a minute of a fault); a longer interval does not fix it"
)

// pendingScenarios reads the scenario IDs out of a traffic detector's pending
// reason: "...succeeded since observation began: a (detail), b" optionally
// followed by "; insufficient usable synthetic samples: c, d". It returns nil
// when any part of the reason is not one of those two, so nothing is dropped.
func pendingScenarios(reason string) []pendingScenario {
	first, second := reason, ""
	if i := strings.Index(reason, "; "+underSampledMarker); i >= 0 {
		first, second = reason[:i], reason[i+2:]
	}
	var parts []struct{ why, remedy, list string }
	switch {
	case strings.HasPrefix(first, underSampledMarker) && second == "":
		parts = append(parts, struct{ why, remedy, list string }{"stayed under-sampled (insufficient usable synthetic samples)", underSampleRemedy, strings.TrimPrefix(first, underSampledMarker)})
	default:
		_, list, ok := strings.Cut(first, "succeeded since observation began: ")
		if !ok || !strings.Contains(first, "synthetic scenario") || strings.TrimSpace(list) == "" {
			return nil
		}
		parts = append(parts, struct{ why, remedy, list string }{"stayed pending (no successful sample since observation began)", unprovenRemedy, list})
		if second != "" {
			parts = append(parts, struct{ why, remedy, list string }{"stayed under-sampled (insufficient usable synthetic samples)", underSampleRemedy, strings.TrimPrefix(second, underSampledMarker)})
		}
	}
	var out []pendingScenario
	seen := map[string]bool{}
	for _, p := range parts {
		depth, start := 0, 0
		add := func(item string) {
			item = strings.TrimSpace(item)
			if item == "" {
				return
			}
			id, detail, _ := strings.Cut(item, " (")
			if seen[id] {
				return
			}
			seen[id] = true
			if detail != "" {
				detail = "; " + clip(strings.TrimSuffix(detail, ")"), 300)
			}
			out = append(out, pendingScenario{id: id, why: p.why, detail: detail, remedy: p.remedy})
		}
		for i, r := range p.list {
			switch r {
			case '(':
				depth++
			case ')':
				depth = max(depth-1, 0)
			case ',':
				if depth == 0 {
					add(p.list[start:i])
					start = i + 1
				}
			}
		}
		add(p.list[start:])
	}
	return out
}

// cachedOutcome is the recorded outcome of an item whose key has not changed,
// when the run may reuse verdicts.
func cachedOutcome(b *BreakConfig, name string) *breakOutcome {
	if !b.Reuse || b.Cache == nil {
		return nil
	}
	key := b.Keys[name]
	rec, ok := b.Cache.Breaks[name]
	if key == "" || !ok || rec.Key != key {
		return nil
	}
	return rec.Outcome.outcome(rec.At)
}

// recordBreaks records the green verdicts this run judged, under the keys of
// their inputs, and forgets the items it judged that are not green. A reused
// verdict keeps the time it was first recorded.
func recordBreaks(b *BreakConfig, outcomes, faultOut map[string]*breakOutcome, now time.Time) {
	if b.Cache == nil {
		return
	}
	if b.Cache.Breaks == nil {
		b.Cache.Breaks = map[string]BreakRecord{}
	}
	for name, key := range b.Keys {
		out, fault := outcomes[name], false
		if out == nil {
			out, fault = faultOut[name], true
		}
		if out == nil || !out.reusedAt.IsZero() {
			continue
		}
		if reusable(out, fault) {
			b.Cache.Breaks[name] = BreakRecord{Key: key, At: now, Outcome: storeOutcome(out)}
		} else {
			delete(b.Cache.Breaks, name)
		}
	}
}

func reusedSuffix(out *breakOutcome) string {
	if out.reusedAt.IsZero() {
		return ""
	}
	return " (" + reusedLabel(out.reusedAt) + ")"
}

// opReuseNote says an operation's verdict rests on a reused break.
func opReuseNote(op appmap.Operation, outcomes map[string]*breakOutcome) string {
	// opDetail quotes the first component whose break failed the operation; the
	// note follows that one, and else covers every component it tried.
	var oldest time.Time
	for _, name := range op.Path {
		if out := outcomes[name]; out != nil && out.opFailure[op.ID] != nil {
			if !out.reusedAt.IsZero() {
				return " (includes a break " + reusedLabel(out.reusedAt) + ")"
			}
			return ""
		}
	}
	for _, name := range op.Path {
		if out := outcomes[name]; out != nil && !out.reusedAt.IsZero() && (oldest.IsZero() || out.reusedAt.Before(oldest)) {
			oldest = out.reusedAt
		}
	}
	if oldest.IsZero() {
		return ""
	}
	return " (includes a break " + reusedLabel(oldest) + ")"
}

// reusedDetectors are the detectors whose inputs have not changed since a run
// in which they judged the healthy application quiet.
func reusedDetectors(q *QuietConfig, progress []DetectorProgress) map[string]time.Time {
	if !q.Reuse || q.Cache == nil || q.DetectorKey == nil {
		return nil
	}
	out := map[string]time.Time{}
	for _, p := range progress {
		if rec, ok := q.Cache.Detectors[p.DetectorID]; ok && rec.Key != "" && rec.Key == q.DetectorKey(p.DetectorID) {
			out[p.DetectorID] = rec.At
		}
	}
	return out
}

func oldestOf(m map[string]time.Time) time.Time {
	var oldest time.Time
	for _, t := range m {
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
		}
	}
	return oldest
}

// recordDetectors records the detectors a clean window judged quiet; one
// reused keeps the time it was first recorded.
func recordDetectors(q *QuietConfig, progress []DetectorProgress, reused map[string]time.Time, now time.Time) {
	if q.Cache == nil || q.DetectorKey == nil {
		return
	}
	if q.Cache.Detectors == nil {
		q.Cache.Detectors = map[string]DetectorRecord{}
	}
	for _, p := range progress {
		at, was := reused[p.DetectorID]
		if !was {
			at = now
		}
		if key := q.DetectorKey(p.DetectorID); key != "" {
			q.Cache.Detectors[p.DetectorID] = DetectorRecord{Key: key, At: at}
		}
	}
}
