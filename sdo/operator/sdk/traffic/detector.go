package traffic

import (
	"context"
	"fmt"
	"sort"
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

// DefaultHealthMinDuration is the sdk.PersistencePolicy.MinDuration a health
// (Class health, Owner health_judge) traffic detector gets when its spec
// leaves MinDuration unset. Synthetic-traffic windows are re-evaluated on
// every probe poll (sdo/operator/runtime.DefaultTrafficPollInterval, 500ms),
// so an evaluation-count-only policy reaches its firing threshold in about
// 0.5s. A kind worker's data plane can stall for about 3s after a
// pod-network change (roughly 1 in 6-10 such changes), producing real prober
// timeouts that would otherwise dispatch a responder for nothing. Real
// faults in this suite are detected in 3-5s. Nine seconds clears the
// observed stalls with headroom while adding at most one more poll beyond
// real-fault detection.
const DefaultHealthMinDuration = 9 * time.Second

type sloDetector struct {
	spec     sdk.DetectorSpec
	workload string
}

// NewDetector returns a health detector that reports every qualified
// scenario of a health-probe workload that violates its SLO. It decides
// purely from the prober's observed samples; missing or unproven observations
// remain unconfirmed and cannot establish health. A health-class spec that leaves Persistence.MinDuration unset
// gets DefaultHealthMinDuration, so judge-authored and lifecycle-generated
// traffic-health detectors alike require a sustained violation before they
// fire, not just an evaluation count.
func NewDetector(spec sdk.DetectorSpec, workload string) sdk.Detector {
	if spec.Class == sdk.DetectorClassHealth && spec.Persistence.MinDuration <= 0 {
		spec.Persistence.MinDuration = DefaultHealthMinDuration
	}
	return sloDetector{spec: spec, workload: workload}
}

func (d sloDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d sloDetector) TrafficWorkloads() []string { return []string{d.workload} }

// Confirmed implements the observation confirmation contract. Every scenario
// must have succeeded since observation began and have enough usable samples
// for its current SLO window. Unproven scenarios never manufacture findings.
func (d sloDetector) Confirmed(ctx sdk.DetectionContext) bool {
	return d.ConfirmationReason(ctx) == ""
}

// ConfirmationReason explains why an empty steady verdict cannot establish
// health. Qualified violations remain findings even alongside unproven peers.
func (d sloDetector) ConfirmationReason(ctx sdk.DetectionContext) string {
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
		if !observed.Qualified {
			unqualified = append(unqualified, id)
			continue
		}
		qualified++
		if !Evaluate(id, window.Workload.ScenarioSLO(id), observed.Samples, window.ObservedAt).Evaluated {
			incomplete = append(incomplete, id)
		}
	}
	for _, scenario := range window.Workload.Scenarios {
		if !seen[scenario.ID] {
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
		reasons = append(reasons, reason+strings.Join(unqualified, ", "))
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
		if verdict.Healthy {
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
		if key != "transport" && key != "timeout" && key != "dial" {
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
		if sample.Status > 0 {
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
		Metadata: map[string]any{
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
		},
	}
}
