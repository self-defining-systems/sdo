package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk/traffic"
)

// SDO-started investigators. When the responder has made no write for the investigator delay, Go starts up to
// response.investigators read-only sessions, one per top candidate node with non-routine evidence, each testing
// one hypothesis. Their reports are display evidence for the responder (`sdo investigations`): they never reach
// IncidentResult, closure, escalation or learning, and the responder is never prompted about them.

const (
	// investigatorTimeLimit is the time box a brief states and the Job's deadline.
	investigatorTimeLimit = 3 * time.Minute
	// investigatorReportCap bounds one report as `sdo investigations` prints it.
	investigatorReportCap = 4000
	// investigatorEvidenceLines bounds the evidence lines of one brief.
	investigatorEvidenceLines = 6
	investigatorTestNames     = 8
)

// investigatorBrief is the Go-rendered task of one investigator.
type investigatorBrief struct {
	Node string
	Text string
}

// nonRoutineEvidence reports whether a node carries a line worth a hypothesis: not rollout churn, not a
// playbook pointer, not a bare note.
func nonRoutineEvidence(n *candidateNode) []candidateEvidence {
	var lines []candidateEvidence
	for _, e := range n.Evidence {
		if !e.Routine && e.Kind != "playbook" && e.Kind != "note" {
			lines = append(lines, e)
		}
	}
	return lines
}

func cappedList(names []string, limit int) string {
	if len(names) == 0 {
		return "none"
	}
	shown := names
	if len(shown) > limit {
		shown = shown[:limit]
	}
	text := strings.Join(shown, ", ")
	if len(names) > limit {
		text += fmt.Sprintf(" (+%d)", len(names)-limit)
	}
	return text
}

// investigatorSymptom is the brief's symptom block and how the hypothesis names the operation to reproduce.
type investigatorSymptom struct {
	Text      string
	Operation string
}

const (
	// investigatorPageCap bounds the page text of an external incident in a brief.
	investigatorPageCap = 1024
	// investigatorContextTests bounds the other failing tests a brief lists.
	investigatorContextTests = 5
)

// investigatorSymptomFor builds the symptom from what opened the incident: for an external incident the page
// message verbatim (bounded) and the failing tests it names; for an SDO-opened one the opening user tests, or
// the Kubernetes condition. Other failing tests are context, marked as possibly unrelated.
func investigatorSymptomFor(set detectSuspectSet, request IncidentRequest) investigatorSymptom {
	opening := map[string]bool{}
	for _, finding := range set.openingFindings {
		if scenario, ok := strings.CutPrefix(finding.RuleID, traffic.RuleIDPrefix); ok && isProbeFinding(finding) {
			opening[scenario] = true
		}
	}
	page := strings.ToLower(request.ExternalMessage)
	spaced := strings.NewReplacer("-", " ", "_", " ")
	var named, rest []failingTest
	for _, test := range set.failing {
		id := strings.ToLower(test.id)
		switch {
		case request.ExternalMessage != "" && (strings.Contains(page, id) || strings.Contains(page, spaced.Replace(id))):
			named = append(named, test)
		case request.ExternalMessage == "" && opening[test.id]:
			named = append(named, test)
		default:
			rest = append(rest, test)
		}
	}
	lines := func(tests []failingTest, limit int) string {
		items := make([]string, len(tests))
		for i, test := range tests {
			items[i] = fmt.Sprintf("%s: %s %q at step %s", test.id, test.label, redactDetectText(test.err, 80), orQuestion(redactDetectText(test.step, 30)))
		}
		return cappedList(items, limit)
	}
	var out strings.Builder
	var result investigatorSymptom
	switch {
	case request.ExternalMessage != "":
		fmt.Fprintf(&out, "Reported symptom (the page that opened this incident, verbatim): %s\n", redactDetectText(request.ExternalMessage, investigatorPageCap))
		if len(named) > 0 {
			fmt.Fprintf(&out, "Failing operations the page names: %s\n", lines(named, investigatorTestNames))
		}
		result.Operation = "the operation the page describes (exactly as described)"
	case len(named) > 0:
		fmt.Fprintf(&out, "Opening symptom (the user tests that opened this incident): %s\n", lines(named, investigatorTestNames))
		result.Operation = "the opening failing test " + named[0].id
	default:
		text := "Kubernetes condition"
		if len(set.openingFindings) > 0 {
			text += ": " + redactDetectText(set.openingFindings[0].Summary, 120)
		}
		fmt.Fprintf(&out, "Opening symptom: %s\n", text)
		result.Operation = "the failing condition above"
	}
	if len(rest) > 0 {
		fmt.Fprintf(&out, "Other failing tests (context only, possibly unrelated to the symptom; do not judge by them): %s\n", lines(rest, investigatorContextTests))
	}
	result.Text = strings.TrimRight(out.String(), "\n")
	return result
}

// investigatorBriefs renders one brief per top candidate node with non-routine evidence, at most max, in the
// view's ranked order. A lone candidate still gets one.
func investigatorBriefs(view candidateView, set detectSuspectSet, request IncidentRequest, max int) []investigatorBrief {
	symptom := investigatorSymptomFor(set, request)
	var briefs []investigatorBrief
	for _, node := range view.Nodes {
		evidence := nonRoutineEvidence(node)
		if len(evidence) == 0 {
			continue
		}
		if len(briefs) == max {
			break
		}
		var text strings.Builder
		fmt.Fprintf(&text, "You are an SDO investigator for incident %s in namespace %s. Test one hypothesis and report; do not fix anything. Time box: %d minutes.\n",
			request.IncidentID, request.Namespace, int(investigatorTimeLimit/time.Minute))
		text.WriteString("Read sdo-investigator/SKILL.md first (in your skills directory) and follow it, including the report format.\n\n")
		fmt.Fprintf(&text, "Node: %s\nEvidence on this node (oldest-to-newest ranking as `sdo detect` shows it):\n", node.Title())
		for i, e := range evidence {
			if i == investigatorEvidenceLines {
				fmt.Fprintf(&text, "  +%d more: sdo changes --component %s\n", len(evidence)-i, node.Name)
				break
			}
			fmt.Fprintf(&text, "  %s\n", e.Text)
		}
		fmt.Fprintf(&text, "\n%s\nPassing tests: %s\n\n", symptom.Text, cappedList(set.passing, investigatorTestNames))
		fmt.Fprintf(&text, "Hypothesis: the failure is caused by %q at node %s.\n", plainName(evidence[0].Text, 200), node.Name)
		fmt.Fprintf(&text, "If true: reproducing %s shows the effect of that evidence (in its response, data or path), and the passing tests do not depend on it.\n", symptom.Operation)
		fmt.Fprintf(&text, "If false: reproducing %s gives the same wrong result with that object in its accepted state, or the passing path depends on it equally. If you cannot reproduce it, the verdict is inconclusive.\n", symptom.Operation)
		text.WriteString("Reply with the report only; its first line must be `Verdict: supported|refuted|inconclusive - <one sentence>`.")
		briefs = append(briefs, investigatorBrief{Node: node.Name, Text: text.String()})
	}
	return briefs
}

// Title is the node's label with its tags, for a brief.
func (n *candidateNode) Title() string { return n.title() }

type investigatorState string

const (
	investigatorRunning investigatorState = "running"
	investigatorDone    investigatorState = "done"
	investigatorFailed  investigatorState = "failed"
	investigatorStopped investigatorState = "stopped"
)

type investigatorEntry struct {
	ID      string
	Node    string
	State   investigatorState
	Report  string
	Started time.Time
	// Pushed is set once the report was appended to a detect, verify or try output.
	Pushed bool
}

// verdictLine is the report's first line when it is "Verdict: ..."; Go parses nothing else.
func verdictLine(report string) string {
	first, _, _ := strings.Cut(strings.TrimSpace(report), "\n")
	if strings.HasPrefix(first, "Verdict:") {
		return plainName(first, 200)
	}
	return ""
}

// InvestigationBoard holds one incident's investigators and their reports. It lives in memory and in the
// responder's tool replies, never under `.sdo/`.
type InvestigationBoard struct {
	mu        sync.Mutex
	entries   []*investigatorEntry
	announced bool
	// Persist, when set, stores a finished report outside the board (the incident's evidence). A failure is
	// logged and never reaches the incident.
	Persist func(id, node, report string) error
}

func (b *InvestigationBoard) start(id, node string, at time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = append(b.entries, &investigatorEntry{ID: id, Node: node, State: investigatorRunning, Started: at})
}

func (b *InvestigationBoard) finish(id string, state investigatorState, report string) {
	b.mu.Lock()
	var node string
	var finished bool
	for _, entry := range b.entries {
		if entry.ID == id && entry.State == investigatorRunning {
			entry.State, entry.Report = state, report
			node, finished = entry.Node, true
		}
	}
	persist := b.Persist
	b.mu.Unlock()
	if finished && state == investigatorDone && strings.TrimSpace(report) != "" && persist != nil {
		if err := persist(id, node, report); err != nil {
			slog.Warn("investigator_report_persist", "investigator", id, "error", err)
		}
	}
}

// ReportNotes are the lines a report arrival adds to the responder's next detect, verify or try output: its
// Verdict line with the node, once per report, and for a "supported" verdict one line naming the hypothesis.
// Reports already shown are not repeated; `sdo investigations` still prints them in full.
func (b *InvestigationBoard) ReportNotes() []string {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var notes []string
	for _, entry := range b.entries {
		if entry.State != investigatorDone || entry.Pushed || strings.TrimSpace(entry.Report) == "" {
			continue
		}
		entry.Pushed = true
		verdict := verdictLine(entry.Report)
		if verdict == "" {
			notes = append(notes, fmt.Sprintf("investigator report arrived (node %s, no Verdict line); read: sdo investigations", entry.Node))
			continue
		}
		label := verdict
		if strings.HasPrefix(strings.TrimSpace(strings.TrimPrefix(verdict, "Verdict:")), "refuted") {
			label = strings.Replace(verdict, "refuted", "refuted (investigator)", 1)
		}
		notes = append(notes, fmt.Sprintf("investigator report (node %s): %s; full text: sdo investigations", entry.Node, label))
		rest := strings.TrimSpace(strings.TrimPrefix(verdict, "Verdict:"))
		if hypothesis, ok := strings.CutPrefix(rest, "supported"); ok {
			hypothesis = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(hypothesis), "-:\u2013\u2014"))
			if hypothesis == "" {
				hypothesis = "the failure is caused at node " + entry.Node
			}
			notes = append(notes, fmt.Sprintf("investigator supports: %s - read `sdo investigations` before acting", hypothesis))
		}
	}
	return notes
}

// DetectLine is the one line the first `sdo detect` after the investigators started ends with; "" before
// they start and after it was shown once.
func (b *InvestigationBoard) DetectLine() string {
	if b == nil {
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.announced || len(b.entries) == 0 {
		return ""
	}
	b.announced = true
	nodes := make([]string, len(b.entries))
	for i, entry := range b.entries {
		nodes[i] = entry.Node
	}
	return fmt.Sprintf("investigators: %d started (nodes %s); read: sdo investigations", len(b.entries), strings.Join(nodes, ", "))
}

// Text is `sdo investigations`: each report in full, running investigators named.
func (b *InvestigationBoard) Text() string {
	if b == nil {
		return "no investigators: SDO starts them only when the responder has made no write for response.investigator_delay\n"
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.entries) == 0 {
		return "no investigators started yet (SDO starts them when no write has been made for response.investigator_delay)\n"
	}
	var out strings.Builder
	fmt.Fprintf(&out, "investigators: %d started\n", len(b.entries))
	for _, entry := range b.entries {
		fmt.Fprintf(&out, "\n== node %s: %s (started %s)\n", entry.Node, entry.State, noteTime(entry.Started))
		switch {
		case entry.Report != "":
			report := strings.TrimSpace(entry.Report)
			if len(report) > investigatorReportCap {
				report = report[:investigatorReportCap] + "\n[report cut at " + fmt.Sprint(investigatorReportCap) + " characters]"
			}
			out.WriteString(report + "\n")
		case entry.State == investigatorRunning:
			out.WriteString("no report yet; run `sdo investigations` again later\n")
		default:
			out.WriteString("no report\n")
		}
	}
	return out.String()
}

// investigatorPlan is what one incident's investigator goroutine needs; the production values come from the
// dispatcher, tests inject fakes.
type investigatorPlan struct {
	IncidentID string
	Count      int
	Delay      time.Duration
	Wait       func(context.Context, time.Duration) error
	// HasWrite reports a write of the responder known to Go.
	HasWrite func() bool
	Briefs   func(ctx context.Context, max int) []investigatorBrief
	// Launch runs one investigator to its report. Its error never reaches the incident.
	Launch func(ctx context.Context, id string, brief investigatorBrief) (string, error)
	Board  *InvestigationBoard
}

// investigatorID names one investigator's session; it never equals an incident ID.
func investigatorID(incidentID string, index int) string {
	return fmt.Sprintf("%s-inv-%d", incidentID, index)
}

// run waits out the delay, starts the investigators only if no write was made, and returns when all ended.
// Cancelling ctx ends them; the caller deletes their Jobs.
func (p investigatorPlan) run(ctx context.Context) {
	wait := p.Wait
	if wait == nil {
		wait = lifecycleio.Wait
	}
	if err := wait(ctx, p.Delay); err != nil {
		return
	}
	if p.HasWrite != nil && p.HasWrite() {
		return
	}
	briefs := p.Briefs(ctx, p.Count)
	if ctx.Err() != nil || len(briefs) == 0 {
		return
	}
	started := make([]string, len(briefs))
	for i, brief := range briefs {
		started[i] = investigatorID(p.IncidentID, i+1)
		p.Board.start(started[i], brief.Node, lifecycleio.Now().UTC())
	}
	var group sync.WaitGroup
	for i, brief := range briefs {
		group.Add(1)
		go func() {
			defer group.Done()
			report, err := p.Launch(ctx, started[i], brief)
			switch {
			case ctx.Err() != nil:
				p.Board.finish(started[i], investigatorStopped, "")
			case err != nil:
				slog.Warn("investigator_failed", "incident_id", p.IncidentID, "node", brief.Node, "error", err)
				p.Board.finish(started[i], investigatorFailed, "")
			default:
				p.Board.finish(started[i], investigatorDone, report)
			}
		}()
	}
	group.Wait()
}

// investigatorRegistry is the dispatcher's investigators for the configured count; nil when none may start.
func investigatorRegistry(count int, delay time.Duration) *InvestigatorRegistry {
	if count <= 0 {
		return nil
	}
	return &InvestigatorRegistry{Count: count, Delay: delay}
}

// investigatorRun is one incident's running investigators.
type investigatorRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	ids    func() []string
}

// InvestigatorRegistry is the dispatcher's investigator configuration and the runs it owns, so a stop reaches
// them from any copy of the dispatcher.
type InvestigatorRegistry struct {
	// Count is response.investigators; Delay is response.investigator_delay.
	Count int
	Delay time.Duration
	// Wait replaces the real-time wait in tests.
	Wait func(context.Context, time.Duration) error

	mu   sync.Mutex
	runs map[string]*investigatorRun
}

// Enabled reports whether any investigator may start.
func (r *InvestigatorRegistry) Enabled() bool { return r != nil && r.Count > 0 }

func (r *InvestigatorRegistry) begin(ctx context.Context, plan investigatorPlan) {
	plan.Count, plan.Delay, plan.Wait = r.Count, r.Delay, r.Wait
	runCtx, cancel := context.WithCancel(ctx)
	run := &investigatorRun{cancel: cancel, done: make(chan struct{}), ids: func() []string {
		plan.Board.mu.Lock()
		defer plan.Board.mu.Unlock()
		ids := make([]string, len(plan.Board.entries))
		for i, entry := range plan.Board.entries {
			ids[i] = entry.ID
		}
		return ids
	}}
	r.mu.Lock()
	if r.runs == nil {
		r.runs = map[string]*investigatorRun{}
	}
	if prior := r.runs[plan.IncidentID]; prior != nil {
		prior.cancel()
	}
	r.runs[plan.IncidentID] = run
	r.mu.Unlock()
	go func() {
		defer close(run.done)
		plan.run(runCtx)
	}()
}

// stop cancels the incident's investigators, waits for their goroutines and deletes their Jobs through
// stopJob. It is safe to call with none running and more than once.
func (r *InvestigatorRegistry) stop(ctx context.Context, incidentID string, stopJob func(context.Context, string) error) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	run := r.runs[incidentID]
	delete(r.runs, incidentID)
	r.mu.Unlock()
	if run == nil {
		return nil
	}
	run.cancel()
	<-run.done
	var failure error
	for _, id := range run.ids() {
		if err := stopJob(ctx, id); err != nil && failure == nil {
			failure = fmt.Errorf("stop investigator %s: %w", id, err)
		}
	}
	return failure
}
