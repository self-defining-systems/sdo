package program_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

var tracePod = sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "web-1"}

// traceView answers the reads a gated Diagnose makes with fixed results.
type traceView struct {
	program.ReadView
	logs     string
	matches  program.LogMatchResult
	exit     int32
	observed error
}

func (v traceView) Logs(sdk.ObjectRef, string, time.Duration) (string, error) { return v.logs, nil }
func (v traceView) LogMatches(sdk.ObjectRef, string, time.Duration, string, int) (program.LogMatchResult, error) {
	return v.matches, nil
}
func (v traceView) Observe(program.ExternalOperation) (int32, error) { return v.exit, v.observed }

// gated reads a log first and a command second, returning NoMatch at whichever
// condition fails: the shape of a learned diagnosis.
type gated struct{}

func (gated) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "gated"} }
func (gated) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	m, err := v.LogMatches(tracePod, "web", 10*time.Minute, "rejected", 1)
	if err != nil || len(m.Lines) == 0 {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	code, err := v.Observe(program.ExternalOperation{Exec: &program.Command{Pod: tracePod, Container: "web", Args: []string{"check"}}})
	if err != nil {
		return program.Verdict{Result: program.Unknown}, nil
	}
	if code == 0 {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match}, nil
}

func run(v traceView) program.Verdict {
	v.ReadView = mapSnapshot().RuntimeReadView()
	return program.RunDiagnose(context.Background(), gated{}, v, program.Bindings{}, time.Second)
}

func steps(v program.Verdict) []string {
	var out []string
	for _, s := range v.Reads {
		out = append(out, fmt.Sprintf("%s %s: %s", s.Op, s.Target, s.Outcome))
	}
	return out
}

func TestADiagnoseVerdictNamesTheReadThatDecidedNoMatch(t *testing.T) {
	// The log condition fails: the command is never run, and the trace says so.
	got := run(traceView{matches: program.LogMatchResult{Scanned: 812}})
	if got.Result != program.NoMatch {
		t.Fatalf("result %v", got.Result)
	}
	want := []string{"log_matches app/web-1/web: 0 lines matched of 812 scanned"}
	if fmt.Sprint(steps(got)) != fmt.Sprint(want) {
		t.Fatalf("trace %q, want %q", steps(got), want)
	}

	// The log condition holds and the command check fails.
	got = run(traceView{matches: program.LogMatchResult{Lines: []string{"x"}, Scanned: 5}, exit: 0})
	want = []string{"log_matches app/web-1/web: 1 lines matched of 5 scanned", "observe app/web-1/web: exit 0"}
	if got.Result != program.NoMatch || fmt.Sprint(steps(got)) != fmt.Sprint(want) {
		t.Fatalf("%v trace %q, want %q", got.Result, steps(got), want)
	}
}

func TestADiagnoseTraceKeepsAReadErrorOnAnUnknownVerdict(t *testing.T) {
	got := run(traceView{matches: program.LogMatchResult{Lines: []string{"x"}}, observed: fmt.Errorf("external observation unavailable")})
	if got.Result != program.Unknown || len(got.Reads) != 2 || got.Reads[1].Outcome != "error: external observation unavailable" {
		t.Fatalf("%v %q", got.Result, steps(got))
	}
}
