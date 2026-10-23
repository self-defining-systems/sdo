package metrics

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"sdo.dev/operator/sdk"
)

// ThresholdRulePrefix prefixes the rule ID of each threshold finding.
const ThresholdRulePrefix = "metric-threshold."

// Comparison says when a value violates its threshold.
type Comparison string

const (
	Greater        Comparison = ">"
	GreaterOrEqual Comparison = ">="
	Less           Comparison = "<"
	LessOrEqual    Comparison = "<="
	Equal          Comparison = "=="
	NotEqual       Comparison = "!="
)

func (c Comparison) valid() bool {
	switch c {
	case Greater, GreaterOrEqual, Less, LessOrEqual, Equal, NotEqual:
		return true
	}
	return false
}

// Holds reports whether value violates threshold. NaN is no observation, so
// no comparison holds for it.
func (c Comparison) Holds(value float64, threshold float64) bool {
	if math.IsNaN(value) || math.IsNaN(threshold) {
		return false
	}
	switch c {
	case Greater:
		return value > threshold
	case GreaterOrEqual:
		return value >= threshold
	case Less:
		return value < threshold
	case LessOrEqual:
		return value <= threshold
	case Equal:
		return value == threshold
	case NotEqual:
		return value != threshold
	}
	return false
}

// Reduction collapses a series' samples to the one value a rule compares.
// Instant results have one sample, so every reduction agrees on them.
type Reduction string

const (
	ReduceLast Reduction = "last"
	ReduceMin  Reduction = "min"
	ReduceMax  Reduction = "max"
	ReduceAvg  Reduction = "avg"
)

func (r Reduction) valid() bool {
	switch r {
	case ReduceLast, ReduceMin, ReduceMax, ReduceAvg:
		return true
	}
	return false
}

// reduce skips NaN samples; with none left there is no value.
func (r Reduction) reduce(samples []Sample) (float64, bool) {
	var (
		count          int
		sum            float64
		minimum, value float64
		maximum        float64
	)
	for _, sample := range samples {
		if math.IsNaN(sample.Value) {
			continue
		}
		if count == 0 || sample.Value < minimum {
			minimum = sample.Value
		}
		if count == 0 || sample.Value > maximum {
			maximum = sample.Value
		}
		value = sample.Value
		sum += sample.Value
		count++
	}
	if count == 0 {
		return 0, false
	}
	switch r {
	case ReduceMin:
		return minimum, true
	case ReduceMax:
		return maximum, true
	case ReduceAvg:
		return sum / float64(count), true
	}
	return value, true
}

// ThresholdRule fires a finding for every series of Query whose reduced
// value violates Threshold.
type ThresholdRule struct {
	// RuleID names the rule; it defaults to the query ID, so two rules over
	// one query need explicit, distinct IDs.
	RuleID    string
	Query     Query
	Op        Comparison
	Threshold float64
	// Reduce picks the value of a range result; it defaults to the last sample.
	Reduce Reduction
	// Labels maps series labels to Kubernetes objects.
	Labels LabelMapping
	// Severity defaults to critical.
	Severity sdk.FindingSeverity
	// Description explains what the metric means in a finding's summary.
	Description string
}

type thresholdDetector struct {
	spec    sdk.DetectorSpec
	rules   []ThresholdRule
	queries []Query
	err     error
}

// NewThresholdDetector returns a detector that compares the frozen result of
// each rule's query with its threshold. Every violating series is one finding
// whose evidence is the query, value, threshold and labels. A query the
// metrics source could not answer is a detector error, never a clear: an
// unobservable metric must not close an incident or open one.
//
// An invalid configuration makes Detect fail and Validate report why.
func NewThresholdDetector(spec sdk.DetectorSpec, rules []ThresholdRule) sdk.Detector {
	d := thresholdDetector{spec: spec}
	if len(rules) == 0 {
		d.err = errors.New("threshold detector has no rules")
		return d
	}
	seenRules := make(map[string]struct{}, len(rules))
	queries := make(map[string]Query)
	for _, rule := range rules {
		if rule.RuleID == "" {
			rule.RuleID = rule.Query.ID
		}
		if rule.Reduce == "" {
			rule.Reduce = ReduceLast
		}
		if rule.Severity == "" {
			rule.Severity = sdk.SeverityCritical
		}
		rule.Labels = rule.Labels.withDefaults()
		if err := rule.validate(); err != nil {
			d.err = err
			return d
		}
		if _, ok := seenRules[rule.RuleID]; ok {
			d.err = fmt.Errorf("duplicate threshold rule id %q", rule.RuleID)
			return d
		}
		seenRules[rule.RuleID] = struct{}{}
		if existing, ok := queries[rule.Query.ID]; ok && existing != rule.Query {
			d.err = fmt.Errorf("metric query %q is defined twice with different definitions", rule.Query.ID)
			return d
		}
		queries[rule.Query.ID] = rule.Query
		d.rules = append(d.rules, rule)
	}
	for _, query := range queries {
		d.queries = append(d.queries, query)
	}
	sort.Slice(d.queries, func(left int, right int) bool { return d.queries[left].ID < d.queries[right].ID })
	return d
}

func (r ThresholdRule) validate() error {
	if err := r.Query.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(r.RuleID) == "" {
		return fmt.Errorf("threshold rule over query %q has no id", r.Query.ID)
	}
	if !r.Op.valid() {
		return fmt.Errorf("threshold rule %q: unsupported comparison %q", r.RuleID, r.Op)
	}
	if math.IsNaN(r.Threshold) {
		return fmt.Errorf("threshold rule %q: threshold must be a number", r.RuleID)
	}
	if !r.Reduce.valid() {
		return fmt.Errorf("threshold rule %q: unsupported reduction %q", r.RuleID, r.Reduce)
	}
	switch r.Severity {
	case sdk.SeverityInfo, sdk.SeverityWarning, sdk.SeverityCritical:
	default:
		return fmt.Errorf("threshold rule %q: unsupported severity %q", r.RuleID, r.Severity)
	}
	return nil
}

func (d thresholdDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d thresholdDetector) MetricQueries() []Query { return append([]Query(nil), d.queries...) }

func (d thresholdDetector) Validate() error { return d.err }

func (d thresholdDetector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.err != nil {
		return nil, d.err
	}
	// Resolve every query before judging any, so one unavailable query is
	// never masked by the findings of the others.
	results := make(map[string]Result, len(d.queries))
	for _, query := range d.queries {
		result, ok := ResultFrom(ctx, query.ID)
		if !ok {
			return nil, fmt.Errorf("metrics unavailable: detection context carries no result for query %s", query.ID)
		}
		if result.Error != "" {
			return nil, fmt.Errorf("metrics unavailable: query %s: %s", query.ID, result.Error)
		}
		results[query.ID] = result
	}
	findings := make([]sdk.Finding, 0)
	for _, rule := range d.rules {
		findings = append(findings, d.judge(ctx.Namespace(), rule, results[rule.Query.ID])...)
	}
	sort.Slice(findings, func(left int, right int) bool {
		if findings[left].RuleID != findings[right].RuleID {
			return findings[left].RuleID < findings[right].RuleID
		}
		return findings[left].Fingerprint < findings[right].Fingerprint
	})
	return findings, nil
}

func (d thresholdDetector) judge(namespace string, rule ThresholdRule, result Result) []sdk.Finding {
	foreign := 0
	findings := make([]sdk.Finding, 0)
	for _, series := range result.Series {
		if seriesNamespace, ok := rule.Labels.namespaceOf(series.Labels); ok && seriesNamespace != namespace {
			foreign++
			continue
		}
		value, ok := rule.Reduce.reduce(series.Samples)
		if !ok || !rule.Op.Holds(value, rule.Threshold) {
			continue
		}
		findings = append(findings, d.finding(namespace, rule, result, series, value))
	}
	for index := range findings {
		findings[index].Metadata["series_in_other_namespaces"] = foreign
	}
	return findings
}

func (d thresholdDetector) finding(namespace string, rule ThresholdRule, result Result, series Series, value float64) sdk.Finding {
	primary, related := rule.Labels.resources(namespace, series.Labels)
	ruleID := ThresholdRulePrefix + rule.RuleID
	observed := result.ObservedAt.UTC().Format("2006-01-02T15:04:05Z07:00")
	comparison := fmt.Sprintf("%s %s %s", FormatValue(value), rule.Op, FormatValue(rule.Threshold))
	summary := fmt.Sprintf("metric %s on %s %s is %s", rule.RuleID, primary.Kind, primary.Name, comparison)
	if rule.Description != "" {
		summary += ": " + rule.Description
	}
	reduced := ""
	if len(series.Samples) > 1 {
		reduced = fmt.Sprintf(" (%s of %d samples over %s)", rule.Reduce, len(series.Samples), rule.Query.Range)
	}
	evidence := fmt.Sprintf("query %s: %s evaluated at %s returned series %s with value %s%s, which violates the threshold (%s).",
		rule.Query.ID, rule.Query.Expr, observed, LabelString(series.Labels), FormatValue(value), reduced, comparison)
	return sdk.Finding{
		DetectorID:       d.spec.ID,
		RuleID:           ruleID,
		Status:           sdk.FindingActive,
		Severity:         rule.Severity,
		Summary:          summary,
		Evidence:         evidence,
		PrimaryResource:  primary,
		RelatedResources: related,
		Playbooks:        append([]string(nil), d.spec.Playbooks...),
		Fingerprint:      fingerprint(d.spec.ID, ruleID, series.Labels),
		Metadata: map[string]any{
			"query_id":    rule.Query.ID,
			"expr":        rule.Query.Expr,
			"observed_at": observed,
			"value":       metadataValue(value),
			"threshold":   metadataValue(rule.Threshold),
			"comparison":  string(rule.Op),
			"reduce":      string(rule.Reduce),
			"samples":     len(series.Samples),
			"labels":      copyLabels(series.Labels),
		},
	}
}

func copyLabels(labels map[string]string) map[string]string {
	copied := make(map[string]string, len(labels))
	for name, value := range labels {
		copied[name] = value
	}
	return copied
}
