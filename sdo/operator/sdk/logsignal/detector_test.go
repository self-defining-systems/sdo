package logsignal

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/sdktest"
)

const source = `
components: [api, worker]
window: 1m
signals:
  - id: denied
    re2: 'permission denied: wrong signature'
    min_count: 2
expect:
  - id: job-done
    re2: 'job done'
`

var workloads = map[string]string{"api": "Deployment/api", "worker": "Deployment/worker"}

var now = time.Unix(100_000, 0).UTC()

// window is one container of workload with one cycle per 10 s up to end,
// each matching count[pattern] lines.
func window(workload, pod string, end time.Time, span time.Duration, count map[string]int, excerpt string) logs.Window {
	w := logs.Window{Namespace: "app", Pod: pod, Container: "main", Workload: workload}
	for start := end.Add(-span); start.Before(end); start = start.Add(10 * time.Second) {
		c := logs.Cycle{Start: start, End: start.Add(10 * time.Second), Running: true, Lines: 5, Matches: map[string]logs.Match{}}
		for id, n := range count {
			m := logs.Match{Count: n}
			if n > 0 && excerpt != "" {
				m.Excerpts = []string{excerpt}
			}
			c.Matches[id] = m
		}
		w.Cycles = append(w.Cycles, c)
	}
	return w
}

func detect(t *testing.T, current []logs.Window, history []logs.Sample) []sdk.Finding {
	t.Helper()
	d := NewDetector(sdk.DetectorSpec{ID: "logs-gate"}, "gate", source, workloads)
	snapshot := sdktest.Snapshot{NamespaceName: "app", Logs: map[string]logs.Result{
		QueryID("gate"): {Query: d.(logs.Consumer).LogQueries()[0], ObservedAt: now, Windows: current, History: history},
	}}
	findings, err := d.Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

func healthy(denied, done int) []logs.Sample {
	end := now.Add(-time.Hour)
	return []logs.Sample{{End: end, Windows: []logs.Window{
		window("Deployment/api", "api-1", end, 2*time.Minute, map[string]int{"denied": denied}, ""),
		window("Deployment/worker", "worker-1", end, 2*time.Minute, map[string]int{"job-done": done}, ""),
	}}}
}

func rules(findings []sdk.Finding) []string {
	var out []string
	for _, f := range findings {
		out = append(out, f.RuleID+" "+f.Metadata["component"].(string)+" "+f.Metadata["pattern"].(string))
	}
	return out
}

func TestFailureLinesFireOnlyAboveTheHealthyHistory(t *testing.T) {
	burst := []logs.Window{window("Deployment/api", "api-1", now, 2*time.Minute, map[string]int{"denied": 3}, "permission denied: wrong signature token=synthetic-value"),
		window("Deployment/worker", "worker-1", now, 2*time.Minute, map[string]int{"job-done": 1}, "")}
	// 6 cycles in the 1m window, 3 lines each: 18 lines.
	got := detect(t, burst, healthy(0, 1))
	if len(got) != 1 || got[0].RuleID != RuleLines || got[0].PrimaryResource.Name != "api" || got[0].PrimaryResource.Kind != "Deployment" {
		t.Fatalf("findings = %v", rules(got))
	}
	if strings.Contains(got[0].Evidence, "synthetic-value") || !strings.Contains(got[0].Evidence, "sample line:") {
		t.Fatalf("evidence must carry a redacted sample: %q", got[0].Evidence)
	}
	// The same lines are normal when healthy windows logged as many.
	if got := detect(t, burst, healthy(3, 1)); len(got) != 0 {
		t.Fatalf("lines normal for the application fired: %v", rules(got))
	}
	// Without healthy history nothing is judged.
	if got := detect(t, burst, nil); len(got) != 0 {
		t.Fatalf("judged without healthy history: %v", rules(got))
	}
}

func TestExpectedActivityFiresWhenItStops(t *testing.T) {
	quiet := []logs.Window{window("Deployment/api", "api-1", now, 2*time.Minute, nil, ""),
		window("Deployment/worker", "worker-1", now, 2*time.Minute, map[string]int{"job-done": 0}, "")}
	got := detect(t, quiet, healthy(0, 1))
	if len(got) != 1 || got[0].RuleID != RuleAbsent || got[0].PrimaryResource.Name != "worker" {
		t.Fatalf("findings = %v", rules(got))
	}
	// A window the retained cycles do not fully cover (a new Pod) is not judged.
	young := []logs.Window{window("Deployment/worker", "worker-2", now, 30*time.Second, map[string]int{"job-done": 0}, "")}
	if got := detect(t, young, healthy(0, 1)); len(got) != 0 {
		t.Fatalf("partial window judged absent: %v", rules(got))
	}
	// Activity the healthy application did not always log is not expected.
	if got := detect(t, quiet, healthy(0, 0)); len(got) != 0 {
		t.Fatalf("absent activity fired without healthy activity: %v", rules(got))
	}
}

func TestFailedReadIsADetectorError(t *testing.T) {
	w := window("Deployment/worker", "worker-1", now, 2*time.Minute, nil, "")
	w.Cycles[len(w.Cycles)-1].Error = "synthetic read failure"
	d := NewDetector(sdk.DetectorSpec{ID: "logs-gate"}, "gate", source, workloads)
	snapshot := sdktest.Snapshot{NamespaceName: "app", Logs: map[string]logs.Result{QueryID("gate"): {ObservedAt: now, Windows: []logs.Window{w}, History: healthy(0, 1)}}}
	if _, err := d.Detect(context.Background(), snapshot); err == nil {
		t.Fatal("a failed read must not clear")
	}
}

func TestValidateNamesProblems(t *testing.T) {
	for name, text := range map[string]string{
		"no components": "signals: [{id: a, re2: x}]",
		"no patterns":   "components: [api]",
		"bad re2":       "components: [api]\nsignals: [{id: a, re2: '('}]",
		"matches all":   "components: [api]\nsignals: [{id: a, re2: '.*'}]",
		"window":        "components: [api]\nwindow: 1h\nsignals: [{id: a, re2: x}]",
		"duplicate id":  "components: [api]\nsignals: [{id: a, re2: x}]\nexpect: [{id: a, re2: y}]",
		"unknown field": "components: [api]\nsignals: [{id: a, re2: x, rate: 3}]",
	} {
		s, err := Parse([]byte(text))
		if err == nil {
			err = s.Validate("gate")
		}
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	s, err := Parse([]byte(source))
	if err != nil || s.Validate("gate") != nil {
		t.Fatalf("valid file rejected: %v %v", err, s.Validate("gate"))
	}
}
