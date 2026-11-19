package runtime

import (
	"context"
	"fmt"
	"slices"
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

// detectWatch is what `sdo detect --wait` compares: the incident's alert set
// and which of its members are active. Only findings in the alert set count;
// clues, other detectors' findings and a member's changing counts or text are
// not changes of the incident's state.
type detectWatch struct {
	members []string          // the alert set's keys; empty when the incident names none
	active  map[string]string // key -> label of an active finding in the alert set (any finding when the set is empty)
}

func (l *ResponderLoop) watchState(response ToolResponse, name string) detectWatch {
	request := l.liveRequest()
	keys := alertKeys(&request)
	members := slices.Clone(keys)
	sort.Strings(members)
	watch := detectWatch{members: members, active: map[string]string{}}
	for _, finding := range response.Findings {
		if finding.Status != sdk.FindingActive {
			continue
		}
		if len(keys) > 0 && !slices.Contains(keys, findingKey(finding)) {
			continue
		}
		if test, _ := strings.CutPrefix(finding.RuleID, traffic.RuleIDPrefix); name != "" && finding.DetectorID != name && test != name {
			continue
		}
		watch.active[findingKey(finding)] = detectFindingLabel(finding)
	}
	return watch
}

// watchChange describes how after differs from before; "" when it does not. A
// change is a user test or alert joining the alert set, or a member alert
// clearing or raising again. Nothing that moves with time alone is one.
func watchChange(before, after detectWatch) string {
	var changes []string
	for _, key := range after.members {
		if !slices.Contains(before.members, key) {
			label := after.active[key]
			if label == "" {
				label = strings.ReplaceAll(key, "\x00", " ")
			}
			changes = append(changes, "joined the alert set: "+label)
		}
	}
	var raised, cleared []string
	for key, label := range after.active {
		if _, was := before.active[key]; !was {
			raised = append(raised, "alert active again: "+label)
		}
	}
	for key, label := range before.active {
		if _, is := after.active[key]; !is {
			cleared = append(cleared, "cleared "+label)
		}
	}
	sort.Strings(raised)
	sort.Strings(cleared)
	return strings.Join(append(append(changes, cleared...), raised...), "; ")
}

// truncateDetectText cuts text to at most n runes for a one-line change note.
func truncateDetectText(text string, n int) string {
	text = strings.Join(strings.Fields(text), " ")
	if runes := []rune(text); len(runes) > n {
		return string(runes[:n]) + "..."
	}
	return text
}

// waitDetect answers `sdo detect [NAME] --wait SECONDS`: it reads the state
// now, reads again every detectPollInterval and returns as soon as a failing
// alert-set member clears or raises again, a user test joins the alert set, the incident recovers or the time
// passes; a failed read is skipped and an idle tick never returns early. A user-test NAME sends its request once, at the start. The call is
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
		before := l.watchState(response, request.Name)
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
			nextRead, next, nextPrepared := l.readOnce(ctx, poll)
			l.operationMu.RUnlock()
			if !nextPrepared || next.Error != "" {
				// A read that failed observed nothing: the wait goes on, and
				// shows the last good read.
				continue
			}
			read, response = nextRead, next
			if change = watchChange(before, l.watchState(response, request.Name)); change != "" {
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
