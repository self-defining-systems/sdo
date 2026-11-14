package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

const (
	// detectPollInterval is how often `sdo detect --wait` reads again.
	detectPollInterval = 2 * time.Second
	// detectMaxWaitSeconds bounds one `sdo detect --wait`.
	detectMaxWaitSeconds = 600
)

// detectNamed answers `sdo detect [NAME]`: with no NAME the incident summary;
// a user test sends its scheduled request now and shows the result; a detector
// runs alone; anything else is one item of the summary.
func (l *ResponderLoop) detectNamed(ctx context.Context, request ToolRequest, response *ToolResponse) {
	name := request.Name
	if name == "" {
		l.detect(ctx, request.DetectorIDs, request.Finding, response)
		return
	}
	if workload, ok := l.userTestWorkload(ctx, name); ok {
		l.detectUserTest(ctx, workload, name, !request.noSend, response)
		return
	}
	for _, detector := range l.Detectors {
		if detector.Spec().ID == name {
			l.detect(ctx, []string{name}, "", response)
			if response.Error == "" {
				response.Text = detectorText(name, *response)
			}
			return
		}
	}
	summary := l.detect(ctx, nil, "", response)
	if response.Error != "" {
		return
	}
	if summary != nil {
		if text, ok := summary.item(name); ok {
			response.Text = text
			return
		}
	}
	response.Text = ""
	response.Error = l.unknownDetectName(ctx, name)
}

// userTestWorkload is the health-probe workload that runs the user test name.
func (l *ResponderLoop) userTestWorkload(ctx context.Context, name string) (string, bool) {
	windows := l.userTestWindows(ctx)
	workloads := make([]string, 0, len(windows))
	for workload := range windows {
		workloads = append(workloads, workload)
	}
	sort.Strings(workloads)
	for _, workload := range workloads {
		for _, observed := range windows[workload].Scenarios {
			if observed.Scenario.ID == name {
				return workload, true
			}
		}
	}
	return "", false
}

func (l *ResponderLoop) userTestWindows(ctx context.Context) map[string]traffic.Window {
	if l.Traffic == nil {
		return nil
	}
	windows, err := l.Traffic.Windows(ctx)
	if err != nil {
		return nil
	}
	return windows
}

// detectUserTest sends the test's scheduled request now (when send is set),
// runs the detectors on fresh reads and shows the new sample and the test's
// failing samples. The request is the one the prober sends on its schedule,
// so sending it again is as safe as the probe itself.
func (l *ResponderLoop) detectUserTest(ctx context.Context, workload, name string, send bool, response *ToolResponse) {
	var sent *traffic.Sample
	if send {
		result, err := l.Traffic.Burst(ctx, prober.BurstRequest{Workload: workload, Scenarios: []string{name}})
		if err != nil {
			response.Error = fmt.Sprintf("send %s now: %v", name, err)
			return
		}
		for _, observed := range result.Window.Scenarios {
			if observed.Scenario.ID == name && len(observed.Samples) > 0 {
				sample := observed.Samples[len(observed.Samples)-1]
				sent = &sample
			}
		}
	}
	summary := l.detect(ctx, nil, "", response)
	if response.Error != "" {
		return
	}
	var out strings.Builder
	if sent != nil {
		writeSentSample(&out, name, *sent)
		response.Healthy = sent.Outcome == traffic.OutcomeOK
		response.Summary = fmt.Sprintf("%s sent now: %s", name, passWord(sent.Outcome == traffic.OutcomeOK))
	}
	detail := ""
	if summary != nil {
		detail = summary.Items[name]
	}
	if detail == "" {
		detail = name + ": no failing samples in the window\n"
	}
	out.WriteString(detail)
	response.Text = out.String()
}

func passWord(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

// writeSentSample renders the sample a request sent now produced.
func writeSentSample(out *strings.Builder, name string, s traffic.Sample) {
	result := "PASS"
	if s.Outcome != traffic.OutcomeOK {
		result = "FAIL " + failureLabel(s)
	}
	fmt.Fprintf(out, "sent now: %s at %s: %s, latency %s\n", name, hms(s.At), result, s.Latency.Round(time.Millisecond))
	if s.Outcome == traffic.OutcomeOK {
		return
	}
	if s.Step != "" {
		fmt.Fprintf(out, "  step: %s\n", sentRedact(s.Step))
	}
	if s.Request != "" {
		fmt.Fprintf(out, "  request: %s\n", sentRedact(s.Request))
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
		fmt.Fprintf(out, "  error: %s\n", sentRedact(s.Error))
	}
	if s.Observed != "" {
		fmt.Fprintf(out, "  response: %s\n", sentRedact(s.Observed))
	}
}

// sentRedact masks credentials in text that came from a probe: a Bearer or
// Basic value first, so an Authorization header loses its credential and not
// just its scheme.
func sentRedact(text string) string {
	return redactDetectText(redactBearer.ReplaceAllString(text, "$1 [redacted]"), 600)
}

// detectorText is one detector's state and findings as `sdo detect DETECTOR` prints them.
func detectorText(id string, response ToolResponse) string {
	var out strings.Builder
	for _, state := range response.DetectorStates {
		if state.DetectorID != id {
			continue
		}
		status := string(state.Status)
		if n := len(state.Fingerprints); n > 0 {
			status += ", " + plural(n, "finding")
		}
		fmt.Fprintf(&out, "detector %s: %s (evaluated %s)", id, status, hms(state.EvaluatedAt))
		switch {
		case state.Error != "":
			fmt.Fprintf(&out, ": %s", redactDetectText(state.Error, 200))
		case state.Status == DetectorEvaluationPending && state.Reason != "":
			fmt.Fprintf(&out, ": %s", redactDetectText(state.Reason, 200))
		}
		out.WriteString("\n")
	}
	for _, finding := range response.Findings {
		fmt.Fprintf(&out, "  %s  %s\n", detectFindingLabel(finding), redactDetectText(finding.Summary, 160))
	}
	return out.String()
}

// detectFindingLabel names a finding the way the agent addresses it.
func detectFindingLabel(finding sdk.Finding) string {
	if id, ok := strings.CutPrefix(finding.RuleID, traffic.RuleIDPrefix); ok {
		return "user test " + id
	}
	if ref := finding.PrimaryResource; ref.Name != "" {
		return finding.DetectorID + " " + ref.Kind + "/" + ref.Name
	}
	return finding.DetectorID + " " + finding.RuleID
}

// unknownDetectName says what NAME may be.
func (l *ResponderLoop) unknownDetectName(ctx context.Context, name string) string {
	var tests []string
	for _, window := range l.userTestWindows(ctx) {
		for _, observed := range window.Scenarios {
			tests = append(tests, observed.Scenario.ID)
		}
	}
	sort.Strings(tests)
	detectors := make([]string, 0, len(l.Detectors))
	for _, detector := range l.Detectors {
		detectors = append(detectors, detector.Spec().ID)
	}
	sort.Strings(detectors)
	return fmt.Sprintf("no user test, detector or summary item named %q; user tests: %s; detectors: %s", name, orNone(strings.Join(tests, ", ")), orNone(strings.Join(detectors, ", ")))
}

// detectWatch is what `sdo detect --wait` compares: the active findings and,
// for every user test, whether its newest request passed.
type detectWatch struct {
	alerts  map[string]string // fingerprint -> label
	failing map[string]bool   // user tests with an active finding
	newest  map[string]bool   // user test -> newest request passed
}

func (l *ResponderLoop) watchState(ctx context.Context, response ToolResponse, name string) detectWatch {
	watch := detectWatch{alerts: map[string]string{}, failing: map[string]bool{}, newest: map[string]bool{}}
	for _, finding := range response.Findings {
		test, isTest := strings.CutPrefix(finding.RuleID, traffic.RuleIDPrefix)
		if name != "" && finding.DetectorID != name && test != name {
			continue
		}
		fingerprint := finding.Fingerprint
		if fingerprint == "" {
			fingerprint = FindingFingerprint(finding)
		}
		watch.alerts[fingerprint] = detectFindingLabel(finding)
		if isTest {
			watch.failing[test] = true
		}
	}
	for _, window := range l.userTestWindows(ctx) {
		for _, observed := range window.Scenarios {
			id := observed.Scenario.ID
			if name == id {
				watch.failing[id] = true
			}
			var newest *traffic.Sample
			for i := range observed.Samples {
				sample := &observed.Samples[i]
				if sample.Outcome == traffic.OutcomeInvalid {
					continue
				}
				if newest == nil || !sample.At.Before(newest.At) {
					newest = sample
				}
			}
			if newest != nil {
				watch.newest[id] = newest.Outcome == traffic.OutcomeOK
			}
		}
	}
	return watch
}

// watchChange describes how after differs from before; "" when it does not.
// A user test counts when it was failing at the start of the wait.
func watchChange(before, after detectWatch) string {
	var changes []string
	tests := make([]string, 0, len(before.failing))
	for id := range before.failing {
		tests = append(tests, id)
	}
	sort.Strings(tests)
	for _, id := range tests {
		was, known := before.newest[id]
		now, sampled := after.newest[id]
		switch {
		case !sampled || known && was == now:
		case now:
			changes = append(changes, id+" newest request passed (was failing)")
		default:
			changes = append(changes, id+" newest request failed (was passing)")
		}
	}
	var raised, cleared []string
	for fingerprint, label := range after.alerts {
		if _, ok := before.alerts[fingerprint]; !ok {
			raised = append(raised, "new alert "+label)
		}
	}
	for fingerprint, label := range before.alerts {
		if _, ok := after.alerts[fingerprint]; !ok {
			cleared = append(cleared, "cleared "+label)
		}
	}
	sort.Strings(raised)
	sort.Strings(cleared)
	changes = append(append(changes, raised...), cleared...)
	return strings.Join(changes, "; ")
}

// waitDetect answers `sdo detect [NAME] --wait SECONDS`: it reads the state
// now, reads again every detectPollInterval and returns as soon as a failing
// test or alert changes, a new alert raises, the incident recovers or the time
// passes. A user-test NAME sends its request once, at the start. The call is
// recorded once, with the last read.
func (l *ResponderLoop) waitDetect(ctx context.Context, request ToolRequest) ToolResponse {
	if request.WaitSeconds < 0 || request.WaitSeconds > detectMaxWaitSeconds {
		return ToolResponse{Command: request.Command, Error: fmt.Sprintf("--wait must be between 1 and %d seconds", detectMaxWaitSeconds)}
	}
	clock := timerClock(l.WaitClock)
	started := clock.Now()
	deadline := started.Add(time.Duration(request.WaitSeconds * float64(time.Second)))
	poll := request
	poll.WaitSeconds = 0
	l.operationMu.RLock()
	read, response, prepared := l.readOnce(ctx, poll)
	l.operationMu.RUnlock()
	change := ""
	if prepared && response.Error == "" {
		before := l.watchState(ctx, response, request.Name)
		poll.noSend = true
		for {
			if l.detectIncidentRecovered() {
				change = "recovered: SDO is ending your incident work"
				break
			}
			remaining := deadline.Sub(clock.Now())
			if remaining <= 0 || waitOnClock(ctx, clock, min(detectPollInterval, remaining)) != nil {
				break
			}
			l.operationMu.RLock()
			read, response, prepared = l.readOnce(ctx, poll)
			l.operationMu.RUnlock()
			if !prepared || response.Error != "" {
				break
			}
			if change = watchChange(before, l.watchState(ctx, response, request.Name)); change != "" {
				break
			}
		}
		elapsed := int64(clock.Now().Sub(started).Round(time.Second) / time.Second)
		line := fmt.Sprintf("wait: no change in %ds", elapsed)
		if change != "" {
			line = fmt.Sprintf("wait: after %ds, %s", elapsed, change)
		}
		response.Text = line + "\n" + response.Text
		response.Summary = line + "; " + response.Summary
	}
	l.operationMu.RLock()
	defer l.operationMu.RUnlock()
	return l.publishRead(ctx, request, read, response, prepared)
}
