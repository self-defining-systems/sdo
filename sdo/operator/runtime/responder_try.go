package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// userTest is one probe scenario with what the prober has recorded for it.
type userTest struct {
	workload string
	traffic.ScenarioObservations
}

// passesNow reports whether the latest recorded sample passed; a test with no sample passes nothing.
func (t userTest) passesNow() (bool, bool) {
	if len(t.Samples) == 0 {
		return false, false
	}
	return t.Samples[len(t.Samples)-1].Outcome == traffic.OutcomeOK, true
}

func (t userTest) statusWord() string {
	passes, sampled := t.passesNow()
	switch {
	case !sampled:
		return "NO SAMPLE"
	case passes:
		return "PASS"
	}
	return "FAIL"
}

// tryRedact masks credentials in text that came from a probe: a Bearer or Basic value first, so an
// Authorization header loses its credential and not just its scheme.
func tryRedact(text string) string {
	return redactDetectText(redactBearer.ReplaceAllString(text, "$1 [redacted]"), 600)
}

// tryUserTest lists the user tests with their current status, or shows one test's latest recorded
// samples. It reads the prober's recorded windows and sends no traffic, so it is read-only by
// construction and safe next to any other call.
func (l *ResponderLoop) tryUserTest(ctx context.Context, name string, response *ToolResponse) {
	if l.Traffic == nil {
		response.Error = "user tests are unavailable: this controller has no prober"
		return
	}
	windows, err := l.Traffic.Windows(ctx)
	if err != nil {
		response.Error = err.Error()
		return
	}
	var tests []userTest
	seen := map[string]bool{}
	workloads := make([]string, 0, len(windows))
	for workload := range windows {
		workloads = append(workloads, workload)
	}
	sort.Strings(workloads)
	for _, workload := range workloads {
		for _, observed := range windows[workload].Scenarios {
			if !seen[observed.Scenario.ID] {
				seen[observed.Scenario.ID] = true
				tests = append(tests, userTest{workload: workload, ScenarioObservations: observed})
			}
		}
	}
	sort.Slice(tests, func(i, j int) bool { return tests[i].Scenario.ID < tests[j].Scenario.ID })
	now := l.clock().UTC()
	if name == "" {
		var out strings.Builder
		failing := 0
		for _, test := range tests {
			if test.statusWord() == "FAIL" {
				failing++
			}
			line := fmt.Sprintf("%-10s %s", test.statusWord(), test.Scenario.ID)
			if len(test.Samples) > 0 {
				last := test.Samples[len(test.Samples)-1]
				line += fmt.Sprintf("  last sample %s ago", now.Sub(last.At).Round(time.Second))
			}
			if test.Scenario.Description != "" {
				line += "  " + tryRedact(test.Scenario.Description)
			}
			out.WriteString(line + "\n")
		}
		if len(tests) == 0 {
			out.WriteString("no user tests are recorded yet\n")
		}
		out.WriteString("sdo try NAME shows one test's latest recorded sample\n")
		response.Text = out.String()
		response.Summary = fmt.Sprintf("%d user tests, %d failing", len(tests), failing)
		return
	}
	for _, test := range tests {
		if test.Scenario.ID == name {
			response.Text = renderUserTest(test, now)
			passes, _ := test.passesNow()
			response.Healthy = passes
			response.Summary = fmt.Sprintf("%s: %s", name, test.statusWord())
			return
		}
	}
	ids := make([]string, len(tests))
	for i, test := range tests {
		ids[i] = test.Scenario.ID
	}
	response.Error = fmt.Sprintf("unknown user test %q; user tests: %s", name, strings.Join(ids, ", "))
}

func renderUserTest(test userTest, now time.Time) string {
	var out strings.Builder
	d := test.Scenario
	fmt.Fprintf(&out, "user test: %s (workload %s)\n", d.ID, test.workload)
	if d.Description != "" {
		fmt.Fprintf(&out, "description: %s\n", tryRedact(d.Description))
	}
	fmt.Fprintf(&out, "target: %s://%s:%d  side effect: %s\n", firstNonEmpty(d.Target.Scheme, "http"), d.Target.Service, d.Target.Port, d.SideEffect)
	fmt.Fprintf(&out, "steps: %s\n", strings.Join(d.Steps, ", "))
	passes, sampled := test.passesNow()
	if !sampled {
		out.WriteString("passes now: unknown (the prober has no sample yet)\n")
		return out.String()
	}
	ok := 0
	for _, sample := range test.Samples {
		if sample.Outcome == traffic.OutcomeOK {
			ok++
		}
	}
	fmt.Fprintf(&out, "passes now: %s (latest sample; %d of %d recorded samples ok)\n", map[bool]string{true: "yes", false: "no"}[passes], ok, len(test.Samples))
	var lastOK, lastBad *traffic.Sample
	for i := range test.Samples {
		sample := &test.Samples[i]
		if sample.Outcome == traffic.OutcomeOK {
			lastOK = sample
		} else {
			lastBad = sample
		}
	}
	writeSample(&out, "latest", test.Samples[len(test.Samples)-1], now)
	if lastBad != nil && lastBad != &test.Samples[len(test.Samples)-1] {
		writeSample(&out, "last failing", *lastBad, now)
	}
	if lastOK != nil && !passes {
		writeSample(&out, "last passing", *lastOK, now)
	}
	if test.Invalid > 0 {
		fmt.Fprintf(&out, "invalid iterations: %d (%s)\n", test.Invalid, tryRedact(test.LastInvalid))
	}
	out.WriteString("request lines are the prober's method and path; probe credentials and headers are never recorded here\n")
	return out.String()
}

func writeSample(out *strings.Builder, label string, s traffic.Sample, now time.Time) {
	fmt.Fprintf(out, "%s sample: %s (%s ago), iteration %d, outcome %s, latency %s\n", label, s.At.UTC().Format("15:04:05Z"), now.Sub(s.At).Round(time.Second), s.Iteration, s.Outcome, s.Latency.Round(time.Millisecond))
	if s.Step != "" {
		fmt.Fprintf(out, "  step: %s\n", tryRedact(s.Step))
	}
	if s.Request != "" {
		fmt.Fprintf(out, "  request: %s\n", tryRedact(s.Request))
	}
	switch {
	case s.Exit != nil:
		fmt.Fprintf(out, "  exit: %d\n", *s.Exit)
	case s.Status > 0:
		fmt.Fprintf(out, "  status: %d\n", s.Status)
	default:
		out.WriteString("  status: none received\n")
	}
	if s.DialFailed {
		out.WriteString("  dial failed: the connection was not established\n")
	}
	if s.Error != "" {
		fmt.Fprintf(out, "  error: %s\n", tryRedact(s.Error))
	}
	if s.Observed != "" {
		fmt.Fprintf(out, "  response: %s\n", tryRedact(s.Observed))
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
