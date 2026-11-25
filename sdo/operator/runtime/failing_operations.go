package runtime

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/traffic"
)

// requestOperation is an operation failing for users now, read from an active
// request-metrics error finding: its own counts and failing-request samples.
// Opening is true for the operation of an opening alert; it leads, then the
// rest by the share of requests failing.
type requestOperation struct {
	ID         string // drill-down item id
	Label      string
	Failed     float64
	Total      float64
	Share      float64
	Window     string
	Class      string
	Samples    []string
	Healthy    string    // what the route did before the failure; empty when not measured
	Since      time.Time // first observed failure
	Opening    bool
	Background bool
}

func metaFloat(m map[string]any, key string) float64 {
	switch v := m[key].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	}
	return 0
}

func metaString(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// sampleLines reads the finding's failing-request samples, whether they are
// still typed or came back through JSON.
func sampleLines(m map[string]any) []string {
	var out []string
	add := func(get func(string) string) {
		line := strings.TrimSpace(get("method") + " " + get("path"))
		if line == "" {
			return
		}
		if status := get("status"); status != "" {
			line += " -> " + status
		}
		line = redactBearer.ReplaceAllString(redactSecret.ReplaceAllString(strings.Join(strings.Fields(line), " "), "${1}[redacted]"), "$1 [redacted]")
		out = append(out, string([]rune(line)[:min(len([]rune(line)), 120)]))
	}
	switch samples := m["samples"].(type) {
	case []map[string]string:
		for _, s := range samples {
			add(func(k string) string { return s[k] })
		}
	case []any:
		for _, raw := range samples {
			if s, ok := raw.(map[string]any); ok {
				add(func(k string) string { return metaString(s, k) })
			}
		}
	}
	return out
}

// keepAlertBaseline keeps what an alert's first report measured before the
// failure. The live finding cannot measure it again: once the incident holds
// it, it is judged without history, and history fills with the failure.
func keepAlertBaseline(first, live sdk.Finding) sdk.Finding {
	live.Metadata = maps.Clone(live.Metadata)
	for _, key := range []string{requestmetrics.MetaHealthyShare, requestmetrics.MetaHealthyRequests, sdk.MetaFirstFailureAt} {
		if value, ok := first.Metadata[key]; ok {
			if live.Metadata == nil {
				live.Metadata = map[string]any{}
			}
			live.Metadata[key] = value
		}
	}
	return live
}

// alertBaseline is the finding with the baseline its incident recorded.
func alertBaseline(request IncidentRequest, finding sdk.Finding) sdk.Finding {
	for _, first := range request.Findings {
		if findingKey(first) == findingKey(finding) {
			return keepAlertBaseline(first, finding)
		}
	}
	return finding
}

func firstFailure(m map[string]any) time.Time {
	at, _ := time.Parse(time.RFC3339Nano, metaString(m, sdk.MetaFirstFailureAt))
	return at
}

// addTestHistory adds to each failing test the failure time its alert recorded
// and what its samples before it did.
func addTestHistory(tests []failingTest, request IncidentRequest, history incidentSignalHistory) {
	for i := range tests {
		for _, finding := range request.Findings {
			if at := firstFailure(finding.Metadata); isProbeFinding(finding) && strings.TrimPrefix(finding.RuleID, traffic.RuleIDPrefix) == tests[i].id && !at.IsZero() {
				tests[i].onset = at
			}
		}
		if tests[i].onset.IsZero() {
			continue
		}
		for _, sample := range history.operations[tests[i].id] {
			if sample.at.Before(tests[i].onset) {
				tests[i].before++
				if sample.failed {
					tests[i].failedBefore++
				}
			}
		}
	}
}

// history is when the test began to fail and its record before, for one line.
func (test failingTest) history() string {
	out := ""
	if !test.onset.IsZero() {
		out = " since " + hms(test.onset)
	}
	if test.before > 0 {
		out += fmt.Sprintf(", against a healthy %.1f%% (%d/%d before)", 100*float64(test.failedBefore)/float64(test.before), test.failedBefore, test.before)
	}
	return out
}

// requestOperations are the active request-metrics errors, the opening alerts'
// first, then the worst share first.
func requestOperations(findings []sdk.Finding, request IncidentRequest) []requestOperation {
	opening := alertKeys(&request)
	var out []requestOperation
	for _, f := range findings {
		if f.RuleID != requestmetrics.RuleErrors || f.Status != sdk.FindingActive {
			continue
		}
		m := alertBaseline(request, f).Metadata
		label := metaString(m, "route")
		if label == "" {
			label = metaString(m, "operation")
		} else if declared := metaString(m, "operation"); declared != "" && m["undeclared_route"] != true {
			label = declared + " (" + label + ")"
		}
		if label == "" {
			label = f.Summary
		}
		healthy := ""
		if share, ok := m[requestmetrics.MetaHealthyShare].(float64); ok {
			healthy = fmt.Sprintf(" against a healthy %.1f%% (of %.0f requests)", share*100, metaFloat(m, requestmetrics.MetaHealthyRequests))
		}
		out = append(out, requestOperation{
			Healthy: healthy, Since: firstFailure(m),
			ID: shortID(FindingFingerprint(f)), Label: label, Failed: metaFloat(m, "failed"), Total: metaFloat(m, "total"),
			Share: metaFloat(m, "share"), Window: metaString(m, "window"), Class: metaString(m, "class"), Samples: sampleLines(m),
			Opening: containsKey(opening, findingKey(f)), Background: m[requestmetrics.MetaBackground] == true,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Opening != out[j].Opening {
			return out[i].Opening
		}
		return out[i].Share > out[j].Share
	})
	return out
}

// Request counters are aggregate. The prober's scenario samples cannot
// partition them into per-route probe and non-probe counts.
func requestCoverageLine(operations []requestOperation) string {
	for _, op := range operations {
		if !op.Background && op.Failed > 0 && op.Total > 0 {
			window := "window unknown"
			if op.Window != "" {
				window = "in " + middle(op.Window, 24)
			}
			return fmt.Sprintf("request failures still observed (%.0f of %.0f %s): %s; non-probe coverage unavailable",
				op.Failed, op.Total, window, middle(op.Label, 96))
		}
	}
	return "non-probe coverage unavailable (no active user-route counts)"
}

func containsKey(keys []string, key string) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

// middle keeps the head and the tail of a long route: a template's tail names
// the operation as often as its head.
func middle(text string, limit int) string {
	text = redactBearer.ReplaceAllString(redactSecret.ReplaceAllString(strings.Join(strings.Fields(text), " "), "${1}[redacted]"), "$1 [redacted]")
	r := []rune(text)
	if len(r) <= limit {
		return text
	}
	head := limit * 2 / 3
	return string(r[:head]) + "…" + string(r[len(r)-(limit-head-1):])
}

// lines renders the operation: counts first so a cut never hides how many
// users fail, then the route, its samples and the drill-down.
func (op requestOperation) lines() []string {
	counts := fmt.Sprintf("requests %.0f/%.0f (%.0f%%) failing", op.Failed, op.Total, op.Share*100) + op.Healthy
	if op.Window != "" {
		counts += " in " + op.Window
	}
	if op.Class != "" {
		counts += ", " + redactDetectText(op.Class, 24)
	}
	if !op.Since.IsZero() {
		counts += ", since " + hms(op.Since)
	}
	out := []string{counts + ": " + middle(op.Label, 110)}
	for i, sample := range op.Samples {
		if i == 2 {
			break
		}
		out = append(out, "  e.g. "+sample)
	}
	return append(out, "  more: sdo detect "+op.ID)
}

// clearedBlip is a user test with failed samples whose newest sample passes
// again and that is not judged failing: a closed episode, not a live failure.
func clearedLine(test failingTest) string {
	return fmt.Sprintf("cleared: %s (%d/%d samples failed, passes again)  more: sdo detect %s", test.id, test.failures, test.samples, test.id)
}
