package envcheck_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/envcheck"
	"sdo.dev/operator/operability"
)

type script struct {
	verdicts []envcheck.Verdict
	calls    int
}

func (s *script) Evaluate(context.Context) (envcheck.Verdict, error) {
	v := s.verdicts[min(s.calls, len(s.verdicts)-1)]
	s.calls++
	return v, nil
}

// clock is virtual time: Sleep advances it.
type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }
func (c *clock) Sleep(_ context.Context, d time.Duration) error {
	c.now = c.now.Add(d)
	return nil
}

func input(e envcheck.Evaluator) envcheck.Input {
	c := &clock{now: time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)}
	return envcheck.Input{
		Commit: "e4f1a9c0deadbeef", Evaluator: e, QuietWindow: 10 * time.Minute, QuietInterval: time.Minute,
		Now: c.Now, Sleep: c.Sleep,
	}
}

func run(t *testing.T, in envcheck.Input) operability.Report {
	t.Helper()
	r, err := envcheck.Run(context.Background(), operability.Subject{AcceptedCommit: in.Commit}, in, envcheck.Items())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Validate(); err != nil {
		t.Fatalf("the report must stay a valid operability report: %v", err)
	}
	return r
}

func find(r operability.Report, id string) operability.Check {
	for _, it := range r.Items {
		for _, c := range it.Checks {
			if c.ID == id {
				return c
			}
		}
	}
	return operability.Check{}
}

func TestHealthyDeployIsGreenAndTaggedWithTheCommit(t *testing.T) {
	scripted := &script{verdicts: []envcheck.Verdict{{Status: "clear"}}}
	r := run(t, input(scripted))
	green, red := envcheck.Tally(r)
	if green != 2 || red != 0 {
		t.Fatalf("green %d red %d\n%s", green, red, envcheck.Render(r))
	}
	if scripted.calls < 10 {
		t.Fatalf("the quiet window must sample throughout, got %d evaluations", scripted.calls)
	}
	if got := envcheck.Render(r); !strings.Contains(got, "commit e4f1a9c0") || !strings.Contains(got, "check: green 2 red 0") {
		t.Fatalf("render = %q", got)
	}
}

func TestADetectorRaisingAtRolloutIsRedOnDeployedHealthyAndSkipsTheQuietWindow(t *testing.T) {
	r := run(t, input(&script{verdicts: []envcheck.Verdict{{Status: "firing", Raised: []envcheck.Raise{{DetectorID: "orders-probe", Reason: "500"}}}}}))
	c := find(r, "O1.deployed-healthy")
	if c.Status != operability.StatusFail || !strings.Contains(c.Detail, "orders-probe") {
		t.Fatalf("%+v", c)
	}
}

func TestARaiseDuringTheQuietWindowIsRedAndNamesTheDetector(t *testing.T) {
	r := run(t, input(&script{verdicts: []envcheck.Verdict{
		{Status: "clear"}, {Status: "clear"}, {Status: "firing", Raised: []envcheck.Raise{{DetectorID: "noisy", Reason: "flapped"}}},
	}}))
	if c := find(r, "O1.deployed-healthy"); c.Status != operability.StatusPass {
		t.Fatalf("%+v", c)
	}
	c := find(r, "O4.quiet-when-healthy")
	if c.Status != operability.StatusFail || c.Cause != operability.CauseDetectorFault || !strings.Contains(c.Detail, "noisy") {
		t.Fatalf("%+v", c)
	}
	if !strings.Contains(envcheck.Render(r), "red O4.quiet-when-healthy") {
		t.Fatal(envcheck.Render(r))
	}
}

func TestAFailedDeployIsRedWithARolloutCauseAndNothingElseRuns(t *testing.T) {
	in := input(&script{verdicts: []envcheck.Verdict{{Status: "clear"}}})
	in.DeployErr = errors.New("deployment/api not ready")
	r := run(t, in)
	c := find(r, "O1.deployed-healthy")
	if c.Status != operability.StatusFail || c.Cause != operability.CauseRollout {
		t.Fatalf("%+v", c)
	}
	if q := find(r, "O4.quiet-when-healthy"); q.Status != operability.StatusSkipped {
		t.Fatalf("%+v", q)
	}
}

func TestQuickSkipsTheQuietWindow(t *testing.T) {
	scripted := &script{verdicts: []envcheck.Verdict{{Status: "clear"}}}
	in := input(scripted)
	in.Quick = true
	r := run(t, in)
	if c := find(r, "O4.quiet-when-healthy"); c.Status != operability.StatusSkipped || scripted.calls != 1 {
		t.Fatalf("%+v calls=%d", c, scripted.calls)
	}
}

type extra struct{}

func (extra) Name() string { return "extra" }
func (extra) Run(context.Context, envcheck.Input) []operability.Check {
	return []operability.Check{{ID: "O3.journeys-move-own-metrics", Phase: operability.PhaseLive, Status: operability.StatusFail, Cause: operability.CauseCoverage, Detail: "9 operations share route label value unknown"}}
}

func TestAnotherPackagesItemJoinsTheSameReport(t *testing.T) {
	in := input(&script{verdicts: []envcheck.Verdict{{Status: "clear"}}})
	r, err := envcheck.Run(context.Background(), operability.Subject{AcceptedCommit: in.Commit}, in, append(envcheck.Items(), extra{}))
	if err != nil {
		t.Fatal(err)
	}
	if got := envcheck.Render(r); !strings.Contains(got, "red O3.journeys-move-own-metrics: 9 operations") {
		t.Fatal(got)
	}
}
