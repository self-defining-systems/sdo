package appmap

import (
	"regexp"
	"strings"
)

// Label names that carry a request's route, method and status, in order of
// preference, as common Prometheus instrumentation libraries name them.
var (
	routeLabelNames  = []string{"route", "handler", "http_route", "path", "endpoint", "uri", "url"}
	methodLabelNames = []string{"method", "http_method", "request_method", "verb"}
	statusLabelNames = []string{"status", "code", "status_code", "http_status", "http_status_code", "response_code"}
	requestFamily    = regexp.MustCompile(`(?i)(request|http)`)
	sampleLine       = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+\S+`)
	labelPair        = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"`)
)

// RequestCounter finds, in a Prometheus text exposition the service's pods
// serve, the request counter the request-metrics detector reads: a counter
// (else a histogram's or summary's _count) whose name says requests or HTTP
// and whose series carry a route label, preferring one with status and method
// labels. Errors counts 5xx statuses. It reports false when no family
// qualifies, and leaves Selector empty: telling apart services that share a
// metric name is the caller's.
func RequestCounter(service string, exposition []byte) (MetricEntry, bool) {
	types := map[string]string{}
	labels := map[string]map[string]bool{}
	var order []string
	for _, line := range strings.Split(string(exposition), "\n") {
		line = strings.TrimSpace(line)
		if fields := strings.Fields(line); len(fields) == 4 && fields[0] == "#" && fields[1] == "TYPE" {
			types[fields[2]] = fields[3]
			continue
		}
		match := sampleLine.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		name := match[1]
		if labels[name] == nil {
			labels[name] = map[string]bool{}
			order = append(order, name)
		}
		for _, pair := range labelPair.FindAllStringSubmatch(match[2], -1) {
			labels[name][pair[1]] = true
		}
	}
	best, bestScore := MetricEntry{}, 0
	for _, name := range order {
		kind, ok := familyKind(name, types)
		if !ok || !requestFamily.MatchString(name) {
			continue
		}
		route := firstLabel(labels[name], routeLabelNames)
		if route == "" {
			continue
		}
		entry := MetricEntry{Service: service, Metric: name, Labels: EntryLabels{Route: route,
			Method: firstLabel(labels[name], methodLabelNames), Status: firstLabel(labels[name], statusLabelNames)}}
		score := 1
		if kind == "counter" {
			score += 4
		}
		if entry.Labels.Status != "" {
			score += 2
			entry.Errors = "5.."
		}
		if entry.Labels.Method != "" {
			score++
		}
		if score > bestScore || score == bestScore && name < best.Metric {
			best, bestScore = entry, score
		}
	}
	return best, bestScore > 0
}

// familyKind is "counter" for a counter's sample and "count" for a
// histogram's or summary's _count; other samples do not count requests.
func familyKind(name string, types map[string]string) (string, bool) {
	if types[name] == "counter" || types[strings.TrimSuffix(name, "_total")] == "counter" && strings.HasSuffix(name, "_total") {
		return "counter", true
	}
	if family, ok := strings.CutSuffix(name, "_count"); ok && (types[family] == "histogram" || types[family] == "summary") {
		return "count", true
	}
	return "", false
}

func firstLabel(present map[string]bool, names []string) string {
	for _, name := range names {
		if present[name] {
			return name
		}
	}
	return ""
}
