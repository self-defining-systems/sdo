package runtime

import (
	"fmt"
	"sort"
	"strings"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/requestmetrics"
)

// requestOperation is an operation failing for users now, read from an active
// request-metrics error finding: its own counts and failing-request samples.
// Opening is true for the operation of an opening alert; it leads, then the
// rest by the share of requests failing.
type requestOperation struct {
	ID      string // drill-down item id
	Label   string
	Failed  float64
	Total   float64
	Share   float64
	Window  string
	Class   string
	Samples []string
	Opening bool
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

// requestOperations are the active request-metrics errors, the opening alerts'
// first, then the worst share first.
func requestOperations(findings []sdk.Finding, opening []string) []requestOperation {
	var out []requestOperation
	for _, f := range findings {
		if f.RuleID != requestmetrics.RuleErrors || f.Status != sdk.FindingActive {
			continue
		}
		m := f.Metadata
		label := metaString(m, "route")
		if label == "" {
			label = metaString(m, "operation")
		} else if declared := metaString(m, "operation"); declared != "" && m["undeclared_route"] != true {
			label = declared + " (" + label + ")"
		}
		if label == "" {
			label = f.Summary
		}
		out = append(out, requestOperation{
			ID: shortID(FindingFingerprint(f)), Label: label, Failed: metaFloat(m, "failed"), Total: metaFloat(m, "total"),
			Share: metaFloat(m, "share"), Window: metaString(m, "window"), Class: metaString(m, "class"), Samples: sampleLines(m),
			Opening: containsKey(opening, findingKey(f)),
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
	counts := fmt.Sprintf("requests %.0f/%.0f (%.0f%%) failing", op.Failed, op.Total, op.Share*100)
	if op.Window != "" {
		counts += " in " + op.Window
	}
	if op.Class != "" {
		counts += ", " + redactDetectText(op.Class, 24)
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
