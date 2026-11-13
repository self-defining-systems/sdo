package traffic

import (
	"cmp"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
)

// Consumer is implemented by detectors that evaluate synthetic traffic. The
// controller runtime asks the prober for exactly the workloads its detectors
// consume.
type Consumer interface {
	TrafficWorkloads() []string
}

// RuleIDPrefix prefixes the rule ID of each scenario finding ("scenario-slo.<id>").
const RuleIDPrefix = "scenario-slo."

// DefaultHealthMinDuration is how long a health (Class health) traffic
// detector's failing samples must span, from the first failing sample in the
// window to the last, before a scenario violation is reported, when its spec
// leaves Persistence.MinDuration unset. Synthetic-traffic windows are
// re-evaluated on every probe poll (sdo/operator/runtime.DefaultTrafficPollInterval,
// 500ms), so an evaluation-count-only policy reaches its firing threshold in
// about 0.5s. A kind worker's data plane can stall for about 3s after a
// pod-network change (roughly 1 in 6-10 such changes), producing real prober
// timeouts that would otherwise dispatch a responder for nothing. Real
// faults in this suite are detected in 3-5s. Nine seconds clears the
// observed stalls with headroom while adding at most one more poll beyond
// real-fault detection.
const DefaultHealthMinDuration = 9 * time.Second

type sloDetector struct {
	spec     sdk.DetectorSpec
	workload string
	// hold is the failing-sample span a violation needs before it is reported.
	hold time.Duration
}

// NewDetector returns a health detector that reports every qualified
// scenario of a health-probe workload that violates its SLO. It decides
// purely from the prober's observed samples; missing or unproven observations
// remain unconfirmed and cannot establish health. A health-class spec that
// leaves Persistence.MinDuration unset holds each violation until its failing
// samples span DefaultHealthMinDuration, so judge-authored and
// lifecycle-generated traffic-health detectors alike require a sustained
// failure, counted from the first failing sample, before they fire, not just
// an evaluation count.
func NewDetector(spec sdk.DetectorSpec, workload string) sdk.Detector {
	d := sloDetector{spec: spec, workload: workload}
	if spec.Class == sdk.DetectorClassHealth && spec.Persistence.MinDuration <= 0 {
		d.hold = DefaultHealthMinDuration
	}
	return d
}

func (d sloDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d sloDetector) TrafficWorkloads() []string { return []string{d.workload} }

// EvaluationWindow bounds how long a repaired scenario can keep violating its
// SLO: pre-repair samples age out after MaxAge, and the window needs MinSamples
// fresh samples, plus one arrival at the scenario's share of the workload rate.
func (d sloDetector) EvaluationWindow(ctx sdk.DetectionContext) time.Duration {
	window, ok := WindowFrom(ctx, d.workload)
	if !ok {
		return 0
	}
	var longest time.Duration
	for _, scenario := range window.Workload.Scenarios {
		slo := window.Workload.ScenarioSLO(scenario.ID)
		arrival := window.Workload.ScenarioArrival(scenario.ID)
		horizon := max(slo.MaxAge.Duration(), time.Duration(slo.MinSamples)*arrival) + arrival
		longest = max(longest, horizon)
	}
	return longest
}

// Confirmed implements the observation confirmation contract. Every scenario
// must have succeeded since observation began and have enough usable samples
// for its current SLO window. Unproven scenarios never manufacture findings.
// KnownBroken scenarios are not required.
func (d sloDetector) Confirmed(ctx sdk.DetectionContext) bool {
	return d.pendingReason(ctx) == ""
}

// ConfirmationReason explains why an empty steady verdict cannot establish
// health. Qualified violations remain findings even alongside unproven peers.
// On a confirmed workload it is the non-gating note about KnownBroken
// scenarios whose state changed, empty when none did; the runtime records it
// with the clear evaluation as a coverage note.
func (d sloDetector) ConfirmationReason(ctx sdk.DetectionContext) string {
	if reason := d.pendingReason(ctx); reason != "" {
		return reason
	}
	window, ok := WindowFrom(ctx, d.workload)
	if !ok {
		return ""
	}
	return KnownBrokenChanges(window)
}

func (d sloDetector) pendingReason(ctx sdk.DetectionContext) string {
	window, ok := WindowFrom(ctx, d.workload)
	if !ok {
		return "no synthetic scenario has succeeded since observation began"
	}
	if window.LoadError != "" {
		// Detect reports unavailable observations as an error, not pending.
		return ""
	}
	if len(window.Scenarios) == 0 {
		return "no synthetic scenario has succeeded since observation began"
	}
	var unqualified, incomplete []string
	qualified := 0
	seen := make(map[string]bool, len(window.Scenarios))
	for _, observed := range window.Scenarios {
		id := observed.Scenario.ID
		seen[id] = true
		if window.Workload.KnownBrokenScenario(id) != nil {
			continue
		}
		if !observed.Qualified {
			unqualified = append(unqualified, id)
			continue
		}
		qualified++
		if !scenarioHasEnoughSamples(window, observed) {
			incomplete = append(incomplete, id)
		}
	}
	for _, scenario := range window.Workload.Scenarios {
		if !seen[scenario.ID] && scenario.KnownBroken == nil {
			unqualified = append(unqualified, scenario.ID)
		}
	}
	sort.Strings(unqualified)
	sort.Strings(incomplete)
	var reasons []string
	if len(unqualified) > 0 {
		reason := "synthetic scenarios have not succeeded since observation began: "
		if qualified == 0 {
			reason = "no synthetic scenario has succeeded since observation began: "
		}
		reasons = append(reasons, reason+strings.Join(describeUnproven(window, unqualified), ", "))
	}
	if len(incomplete) > 0 {
		reasons = append(reasons, "insufficient usable synthetic samples: "+strings.Join(incomplete, ", "))
	}
	return strings.Join(reasons, "; ")
}

func (d sloDetector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	window, ok := WindowFrom(ctx, d.workload)
	if !ok {
		return nil, nil
	}
	if window.LoadError != "" {
		return nil, fmt.Errorf("traffic workload %s: %s", d.workload, window.LoadError)
	}
	descriptors := make(map[string]Descriptor, len(window.Scenarios))
	for _, observed := range window.Scenarios {
		descriptors[observed.Scenario.ID] = observed.Scenario
	}
	findings := make([]sdk.Finding, 0)
	for _, verdict := range Judge(window) {
		if verdict.Healthy || verdict.FailureSpan < d.hold {
			continue
		}
		findings = append(findings, ScenarioFinding(d.spec, ctx.Namespace(), window.Workload, descriptors[verdict.ScenarioID], verdict))
	}
	sort.Slice(findings, func(left int, right int) bool { return findings[left].RuleID < findings[right].RuleID })
	return findings, nil
}

// ScenarioFinding explains one unhealthy scenario. The primary resource is
// the Service the scenario calls; the Services it depends on are related, so
// the finding localizes the fault along the request path.
func ScenarioFinding(spec sdk.DetectorSpec, namespace string, workload Workload, scenario Descriptor, verdict SLOVerdict) sdk.Finding {
	statusKeys := make([]string, 0, len(verdict.StatusCounts))
	for key := range verdict.StatusCounts {
		statusKeys = append(statusKeys, key)
	}
	sort.Strings(statusKeys)
	statusParts := make([]string, 0, len(statusKeys))
	for _, key := range statusKeys {
		label := key
		if key != "transport" && key != "timeout" && key != "dial" && !strings.HasPrefix(key, "exit ") {
			label = "HTTP " + key
		}
		statusParts = append(statusParts, fmt.Sprintf("%s ×%d", label, verdict.StatusCounts[key]))
	}
	failures := make([]string, 0, len(verdict.RecentFailures))
	for _, sample := range verdict.RecentFailures {
		label := string(sample.Outcome)
		if sample.DialFailed {
			label = "dial " + label
		}
		if sample.Exit != nil {
			label = fmt.Sprintf("exit %d", *sample.Exit)
		} else if sample.Status > 0 {
			label = fmt.Sprintf("HTTP %d", sample.Status)
		}
		failures = append(failures, fmt.Sprintf("[%s iteration %d] step %s %s → %s: %s",
			sample.At.UTC().Format(time.RFC3339), sample.Iteration, sample.Step, sample.Request, label, sample.Error))
	}
	path := append([]string{scenario.Target.Service}, scenario.DependsOn...)
	evidence := fmt.Sprintf(
		"synthetic scenario %s (steps %s) against Service %s port %d, request path %s; last %d iterations over %s: %s; p%d latency %s. "+
			"Recent failures: %s. Replay with workload %s seed %d and the iteration number.",
		scenario.ID, strings.Join(scenario.Steps, " → "), scenario.Target.Service, scenario.Target.Port,
		strings.Join(path, " → "), verdict.Samples, verdict.Span.Round(time.Millisecond), strings.Join(statusParts, ", "),
		verdict.SLO.LatencyPercentile, verdict.Latency.Round(time.Millisecond), strings.Join(failures, "; "),
		workload.Name, workload.Seed,
	)
	metadata := map[string]any{
		"workload":           workload.Name,
		"scenario":           scenario.ID,
		"side_effect":        string(scenario.SideEffect),
		"depends_on":         append([]string(nil), scenario.DependsOn...),
		"samples":            verdict.Samples,
		"errors":             verdict.Errors,
		"timeouts":           verdict.Timeouts,
		"error_rate":         verdict.ErrorRate,
		"timeout_rate":       verdict.TimeoutRate,
		"latency_percentile": verdict.SLO.LatencyPercentile,
		"latency_ms":         verdict.Latency.Milliseconds(),
		"status_counts":      verdict.StatusCounts,
		"violations":         verdict.Violations,
	}
	if !verdict.FirstFailureAt.IsZero() {
		metadata["first_failure_at"] = verdict.FirstFailureAt.UTC().Format(time.RFC3339Nano)
	}
	if n := len(verdict.RecentFailures); n > 0 {
		last := verdict.RecentFailures[n-1]
		metadata["failure_status"] = statusKey(last)
		// What the server (or network) answered, else the probe's own error.
		metadata["failure_observed"] = cmp.Or(last.Observed, last.Error)
	}
	related := make([]sdk.ObjectRef, 0, len(scenario.DependsOn))
	for _, service := range scenario.DependsOn {
		if service != scenario.Target.Service {
			related = append(related, sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: service})
		}
	}
	return sdk.Finding{
		DetectorID: spec.ID,
		RuleID:     RuleIDPrefix + scenario.ID,
		Status:     sdk.FindingActive,
		Severity:   sdk.SeverityCritical,
		Summary: fmt.Sprintf("synthetic scenario %s violates its SLO: %s",
			scenario.ID, strings.Join(verdict.Violations, "; ")),
		Evidence:         evidence,
		PrimaryResource:  sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: scenario.Target.Service},
		RelatedResources: related,
		Playbooks:        append([]string(nil), spec.Playbooks...),
		Metadata:         metadata,
	}
}

// describeUnproven names each scenario that never succeeded with its latest
// observed failure, so an author sees what the live application answered and
// not only that the assertion never passed.
func describeUnproven(window Window, ids []string) []string {
	observed := make(map[string]ScenarioObservations, len(window.Scenarios))
	for _, o := range window.Scenarios {
		observed[o.Scenario.ID] = o
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		detail := ""
		if o, ok := observed[id]; ok {
			detail = latestFailure(o)
		}
		if detail == "" {
			out = append(out, id)
			continue
		}
		out = append(out, id+" ("+detail+")")
	}
	return out
}

func latestFailure(o ScenarioObservations) string {
	if o.IdentityWait > 0 {
		return "environment wait, not an application failure: probe user provisioning has not finished (" + bounded(o.LastInvalid) + "); re-check after it completes"
	}
	for i := len(o.Samples) - 1; i >= 0; i-- {
		sample := o.Samples[i]
		if sample.Outcome == OutcomeOK {
			continue
		}
		var parts []string
		if sample.Step != "" {
			parts = append(parts, "step "+sample.Step)
		}
		if sample.Request != "" {
			parts = append(parts, sample.Request)
		}
		if sample.Status != 0 {
			parts = append(parts, "status "+strconv.Itoa(sample.Status))
		}
		if sample.Error != "" {
			parts = append(parts, bounded(sample.Error))
		}
		return "last failure: " + strings.Join(parts, ", ")
	}
	if o.LastInvalid != "" {
		return "invalid: " + bounded(o.LastInvalid)
	}
	return ""
}
