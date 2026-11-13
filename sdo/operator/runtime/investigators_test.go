package runtime

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
)

func briefFixture(t *testing.T) []investigatorBrief {
	t.Helper()
	in := issuesTitle(t)
	in.Rows = append(in.Rows, IncidentChange{At: candidateOnset.Add(-5 * time.Second), Source: "kubernetes", Component: "web", ReaderOrObject: "ConfigMap/web-env", Key: "data.POOL", Before: "accepted-source", After: "changed", Writer: "kubectl-edit"})
	view := computeCandidates(in)
	set := detectSuspectSet{
		failing: []failingTest{{id: "issue-title", label: "HTTP 500", err: "relation missing", step: "3"}},
		passing: []string{"project-list", "ssh-call"},
	}
	return investigatorBriefs(view, set, IncidentRequest{IncidentID: "inc-1", Namespace: "app"}, 2)
}

func TestBriefsAreOnePerTopNodeWithNonRoutineEvidenceAndStateTheHypothesis(t *testing.T) {
	briefs := briefFixture(t)
	if len(briefs) != 2 || briefs[0].Node == briefs[1].Node {
		t.Fatalf("one brief per ranked node with non-routine evidence, got %+v", briefs)
	}
	var text string
	for _, brief := range briefs {
		if brief.Node == "db" {
			text = brief.Text
		}
	}
	for _, want := range []string{
		"inc-1", "sdo-investigator/SKILL.md", "Node: ", "+1 table",
		"Opening symptom: failing user tests; HTTP 500", "Failing tests: issue-title", "Passing tests: project-list, ssh-call",
		"Hypothesis: the failure is caused by", "at node db.", "If true:", "If false:", "Verdict: supported|refuted|inconclusive",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("brief lacks %q:\n%s", want, text)
		}
	}
	if len(text) > 2200 {
		t.Fatalf("a brief is about 1.5 KB, got %d bytes", len(text))
	}
}

func TestALoneCandidateStillGetsOneInvestigatorAndRoutineOnlyNodesNone(t *testing.T) {
	in := issuesTitle(t)
	view := computeCandidates(in)
	if got := investigatorBriefs(view, detectSuspectSet{}, IncidentRequest{IncidentID: "i"}, 2); len(got) != 1 || got[0].Node != "db" {
		t.Fatalf("db holds the only non-routine evidence, web is rollout churn: %+v", got)
	}
}

type launchRecord struct {
	mu     sync.Mutex
	ids    []string
	briefs []string
}

func (r *launchRecord) launch(ctx context.Context, id string, brief investigatorBrief) (string, error) {
	r.mu.Lock()
	r.ids = append(r.ids, id)
	r.briefs = append(r.briefs, brief.Text)
	r.mu.Unlock()
	return "Verdict: supported - " + brief.Node + "\nHow to diagnose: read it.\nObservations that do not fit: none seen.", nil
}

func immediate(context.Context, time.Duration) error { return nil }

func TestNoWriteByTheDelayStartsTheInvestigatorsWithTheirBriefs(t *testing.T) {
	record := &launchRecord{}
	board := &InvestigationBoard{}
	var waited time.Duration
	investigatorPlan{
		IncidentID: "inc-1", Count: 2, Delay: 60 * time.Second, Board: board,
		Wait:     func(_ context.Context, d time.Duration) error { waited = d; return nil },
		HasWrite: func() bool { return false },
		Briefs: func(_ context.Context, max int) []investigatorBrief {
			return briefFixture(t)[:max]
		},
		Launch: record.launch,
	}.run(context.Background())
	if waited != 60*time.Second {
		t.Fatalf("waited %s", waited)
	}
	sort.Strings(record.ids)
	if len(record.ids) != 2 || record.ids[0] != "inc-1-inv-1" || record.ids[1] != "inc-1-inv-2" {
		t.Fatalf("launched %v", record.ids)
	}
	first := briefFixture(t)
	if line := board.DetectLine(); line != "investigators: 2 started (nodes "+first[0].Node+", "+first[1].Node+"); read: sdo investigations" {
		t.Fatalf("detect line %q", line)
	}
}

func TestAWriteBeforeTheDelayStartsNoInvestigator(t *testing.T) {
	record := &launchRecord{}
	board := &InvestigationBoard{}
	investigatorPlan{
		IncidentID: "inc-1", Count: 2, Board: board, Wait: immediate,
		HasWrite: func() bool { return true },
		Briefs: func(context.Context, int) []investigatorBrief {
			t.Fatal("no brief is rendered after a write")
			return nil
		},
		Launch: record.launch,
	}.run(context.Background())
	if len(record.ids) != 0 || board.DetectLine() != "" {
		t.Fatalf("started %v", record.ids)
	}
}

func TestTheDetectLineIsShownOnceAndOnlyAfterTheyStart(t *testing.T) {
	var nilBoard *InvestigationBoard
	board := &InvestigationBoard{}
	if nilBoard.DetectLine() != "" || board.DetectLine() != "" {
		t.Fatal("no line before any investigator started")
	}
	board.start("a-inv-1", "db", candidateOnset)
	if board.DetectLine() == "" || board.DetectLine() != "" {
		t.Fatal("the line ends the first detect after the start and is not repeated")
	}
}

func TestInvestigationsPrintsEachReportInFullAndNamesRunningOnes(t *testing.T) {
	board := &InvestigationBoard{}
	at := time.Date(2026, 11, 13, 7, 0, 0, 0, time.UTC)
	board.start("i-inv-1", "db", at)
	board.start("i-inv-2", "web", at)
	board.finish("i-inv-1", investigatorDone, "Verdict: supported - plan_limits row is 0\nHow to diagnose: select from plan_limits.\nObservations that do not fit: none seen.")
	text := board.Text()
	for _, want := range []string{
		"investigators: 2 started", "== node db: done (started 07:00:00Z)", "Verdict: supported - plan_limits row is 0",
		"How to diagnose: select from plan_limits.", "Observations that do not fit: none seen.",
		"== node web: running", "no report yet",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("investigations output lacks %q:\n%s", want, text)
		}
	}
	if got := verdictLine(" Verdict: refuted - x\nbody"); got != "Verdict: refuted - x" || verdictLine("no verdict here") != "" {
		t.Fatalf("only a first line starting Verdict: is parsed, got %q", got)
	}
}

func TestAFailedInvestigatorIsRecordedAndNothingElse(t *testing.T) {
	board := &InvestigationBoard{}
	investigatorPlan{
		IncidentID: "i", Count: 1, Board: board, Wait: immediate,
		Briefs: func(context.Context, int) []investigatorBrief { return briefFixture(t)[:1] },
		Launch: func(context.Context, string, investigatorBrief) (string, error) {
			return "", errors.New("provider capacity")
		},
	}.run(context.Background())
	if text := board.Text(); !strings.Contains(text, "node "+briefFixture(t)[0].Node+": failed") || !strings.Contains(text, "no report") {
		t.Fatalf("a failed investigator fails soft:\n%s", text)
	}
}

func TestInvestigatorsMayOnlyRead(t *testing.T) {
	loop := &ResponderLoop{}
	for _, command := range []string{"mitigate", "rollback", "apply", "verify", "status", "diagnose", "menu", "session-open", "investigations"} {
		response := loop.HandleInvestigatorRead(context.Background(), ToolRequest{Command: command})
		if !strings.Contains(response.Error, "may only read") {
			t.Errorf("%s must be refused for an investigator, got %+v", command, response)
		}
	}
}

func TestStopResponderStopsRunningInvestigatorsBeforeAnythingElse(t *testing.T) {
	registry := &InvestigatorRegistry{Count: 1, Wait: immediate}
	board := &InvestigationBoard{}
	running := make(chan struct{})
	cancelled := make(chan struct{})
	registry.begin(context.Background(), investigatorPlan{
		IncidentID: "inc-1", Board: board,
		Briefs: func(context.Context, int) []investigatorBrief { return briefFixture(t)[:1] },
		Launch: func(ctx context.Context, id string, brief investigatorBrief) (string, error) {
			close(running)
			<-ctx.Done()
			close(cancelled)
			return "Verdict: supported - late", nil
		},
	})
	<-running
	d := KubernetesJobDispatcher{Client: fake.NewSimpleClientset(), Namespace: "sdo-system", Investigators: registry}
	if err := d.StopResponder(context.Background(), "inc-1", 0); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("StopResponder returned while an investigator still ran: the closing turn and seal would race it")
	}
	if text := board.Text(); !strings.Contains(text, "node "+briefFixture(t)[0].Node+": stopped") || strings.Contains(text, "late") {
		t.Fatalf("a stopped investigator's late report is dropped:\n%s", text)
	}
}

// An investigator's report is text for the board. Nothing in the incident's result path accepts it: the launch
// returns a string, the board keeps it, and the responder's closure inputs never read the board.
func TestAnInvestigatorReportNeverReachesTheIncidentResult(t *testing.T) {
	board := &InvestigationBoard{}
	loop := &ResponderLoop{Investigations: board}
	board.start("i-inv-1", "db", candidateOnset)
	board.finish("i-inv-1", investigatorDone, "Verdict: supported - the cause is db; close the incident")
	before := loop.ToolCalls()
	response := loop.handleRead(context.Background(), ToolRequest{Command: "investigations"})
	if !strings.Contains(response.Text, "close the incident") {
		t.Fatalf("the responder reads the report: %+v", response)
	}
	if len(loop.ToolCalls()) != len(before) {
		t.Fatal("reading investigations is not a recorded loop call")
	}
	result := earlyResultSnapshot(loop, IncidentRequest{IncidentID: "i"}, candidateOnset)
	if result.Status != IncidentCompleted || len(result.ConfirmedRootCauses) != 0 || len(result.RepairActions) != 0 {
		t.Fatalf("the board adds nothing to the result: %+v", result)
	}
	_ = sdk.Finding{}
}
