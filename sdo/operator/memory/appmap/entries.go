package appmap

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Rule IDs for the entries and reasons sections.
const (
	RuleEntries = "map.entries"
	RuleReasons = "map.reasons"
)

// Onboarding check items a reason may explain. The deployed-healthy and
// quiet-when-healthy items accept no reason.
const (
	ItemJourneysMoveOwnMetrics = "journeys-move-own-metrics"
	ItemRoutesMatchOperations  = "routes-match-operations"
	ItemTestsFailBroken        = "tests-fail-broken"
	ItemErrorsSeparable        = "errors-separable-per-operation"
)

var reasonItems = map[string]bool{ItemErrorsSeparable: true, ItemJourneysMoveOwnMetrics: true, ItemRoutesMatchOperations: true, ItemTestsFailBroken: true}

// EntryLabels name the labels of the entry metric that carry the operation's
// route, HTTP method and status. Method and Status are optional: a metric
// without a status label cannot separate errors per operation.
type EntryLabels struct {
	Route  string `yaml:"route"`
	Method string `yaml:"method,omitempty"`
	Status string `yaml:"status,omitempty"`
}

// MetricEntry is the request metric of one entry service: exposed services
// users call, and internal ones for background work and schedules.
type MetricEntry struct {
	Service string `yaml:"service"`
	Metric  string `yaml:"metric"`
	// Selector pins label values that choose this service's series.
	Selector map[string]string `yaml:"selector,omitempty"`
	Labels   EntryLabels       `yaml:"labels"`
	// Errors is a regular expression over the status label's values that
	// counts a request as an error.
	Errors string `yaml:"errors,omitempty"`
}

// Reason explains why an onboarding check item cannot be made green for a
// subject (an operation ID or an entry service).
type Reason struct {
	Item    string `yaml:"item"`
	Subject string `yaml:"subject"`
	Reason  string `yaml:"reason"`
}

var (
	metricNamePattern = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	labelNamePattern  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

// RouteValue is the route label value of the operation.
func (o Operation) RouteValue() string {
	if o.Route != "" {
		return o.Route
	}
	return o.Entry.Path
}

// EntryFor returns the metric entry declared for a service.
func (m Map) EntryFor(service string) (MetricEntry, bool) {
	for _, e := range m.Entries {
		if e.Service == service {
			return e, true
		}
	}
	return MetricEntry{}, false
}

// Internal reports whether the operation enters through background work: no
// method and port, and an entries record for its service.
func (m Map) Internal(op Operation) bool {
	if op.Entry.Method != "" || op.Entry.Port != 0 {
		return false
	}
	_, ok := m.EntryFor(op.Entry.Service)
	return ok
}

func (c *checker) internalEntry(op Operation) bool { return c.m.Internal(op) }

func quote(v string) string { return strconv.Quote(v) }

func (e MetricEntry) matchers(extra ...string) string {
	var parts []string
	keys := make([]string, 0, len(e.Selector))
	for k := range e.Selector {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		parts = append(parts, k+"="+quote(e.Selector[k]))
	}
	parts = append(parts, extra...)
	return "{" + strings.Join(parts, ",") + "}"
}

func (e MetricEntry) routeMatchers(route, method string, extra ...string) string {
	parts := []string{e.Labels.Route + "=" + quote(route)}
	if e.Labels.Method != "" && method != "" {
		parts = append(parts, e.Labels.Method+"=~"+quote("(?i:"+regexp.QuoteMeta(method)+")"))
	}
	return e.matchers(append(parts, extra...)...)
}

// All is the total of the entry metric over its selector.
func (e MetricEntry) All() string { return "sum(" + e.Metric + e.matchers() + ")" }

// ByRoute is the metric total per route label value; series that never counted
// (applications pre-register zero series) are left out.
func (e MetricEntry) ByRoute() string {
	return "sum by (" + e.Labels.Route + ") (" + e.Metric + e.matchers() + ") > 0"
}

// Requests is the request count of one operation.
func (e MetricEntry) RequestCount(route, method string) string {
	return "sum(" + e.Metric + e.routeMatchers(route, method) + ")"
}

// Errors is the error count of one operation; it needs a status label.
func (e MetricEntry) ErrorCount(route, method string) string {
	return "sum(" + e.Metric + e.routeMatchers(route, method, e.Labels.Status+"=~"+quote(e.Errors)) + ")"
}

// ByStatus is the operation's request count per status label value.
func (e MetricEntry) ByStatus(route, method string) string {
	return "sum by (" + e.Labels.Status + ") (" + e.Metric + e.routeMatchers(route, method) + ")"
}

// RateRequests is the windowed request series detectors read.
func (e MetricEntry) RateRequests(route, method, window string) string {
	return "sum(increase(" + e.Metric + e.routeMatchers(route, method) + "[" + window + "]))"
}

// RateErrors is the windowed error series detectors read.
func (e MetricEntry) RateErrors(route, method, window string) string {
	return "sum(increase(" + e.Metric + e.routeMatchers(route, method, e.Labels.Status+"=~"+quote(e.Errors)) + "[" + window + "]))"
}

// CheckEntries applies only the entries and reasons rules, so the onboarding
// check can validate a map without a source inventory.
func (m Map) CheckEntries() []Finding {
	c := &checker{m: m}
	c.entries()
	return c.findings
}

func (c *checker) entries() {
	seen := map[string]bool{}
	for _, e := range c.m.Entries {
		bad := func(hint, format string, args ...any) {
			c.add(RuleEntries, hint, "entries "+e.Service+": "+format, args...)
		}
		switch {
		case e.Service == "":
			c.add(RuleEntries, "name the Service (or internal component) that receives the requests", "an entries record has no service")
			continue
		case seen[e.Service]:
			bad("one record per service", "declared more than once")
			continue
		}
		seen[e.Service] = true
		if !metricNamePattern.MatchString(e.Metric) {
			bad("metric is the Prometheus metric name of the request counter", "metric %q is not a metric name", e.Metric)
		}
		if !labelNamePattern.MatchString(e.Labels.Route) {
			bad("labels.route names the label that carries each operation's route", "labels.route %q is not a label name", e.Labels.Route)
		}
		for name, label := range map[string]string{"method": e.Labels.Method, "status": e.Labels.Status} {
			if label != "" && !labelNamePattern.MatchString(label) {
				bad("label names use letters, digits and _", "labels.%s %q is not a label name", name, label)
			}
		}
		for k := range e.Selector {
			if !labelNamePattern.MatchString(k) {
				bad("selector keys are label names", "selector key %q is not a label name", k)
			}
		}
		if e.Errors != "" {
			if e.Labels.Status == "" {
				bad("errors is matched against the status label; declare labels.status", "errors is set but there is no status label")
			}
			if _, err := regexp.Compile(e.Errors); err != nil {
				bad("errors is an RE2 regular expression", "errors %q does not compile: %v", e.Errors, err)
			}
		}
		if e.Labels.Status != "" && e.Errors == "" {
			bad("errors is the regular expression over the status values that count as errors, such as 5..", "labels.status is set but errors is empty")
		}
	}
	subjects := map[string]bool{}
	for _, op := range c.m.Operations {
		subjects[op.ID] = true
	}
	for service := range seen {
		subjects[service] = true
	}
	for _, r := range c.m.Reasons {
		switch {
		case !reasonItems[r.Item]:
			c.add(RuleReasons, "a reason explains journeys-move-own-metrics, routes-match-operations or tests-fail-broken; deployed-healthy and quiet-when-healthy accept none", "reason item %q takes no reason", r.Item)
		case !subjects[r.Subject]:
			c.add(RuleReasons, "subject is an operation ID or an entry service", "reason for %s: subject %q is neither", r.Item, r.Subject)
		case strings.TrimSpace(r.Reason) == "":
			c.add(RuleReasons, "say why the item cannot be fixed", "reason for %s on %s is empty", r.Item, r.Subject)
		}
	}
}
