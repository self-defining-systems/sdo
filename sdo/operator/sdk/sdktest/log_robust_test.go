package sdktest

import (
	"encoding/json"
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type logDiagnoser struct{ stateOnly bool }

func (logDiagnoser) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "fix"} }

func (d logDiagnoser) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	if d.stateOnly {
		return program.Verdict{Result: program.Match}, nil
	}
	logs, err := v.Logs(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "web-0"}, "web", time.Minute)
	if err != nil {
		return program.Verdict{}, err
	}
	if strings.Contains(logs, "violates check constraint") {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func logCase(t *testing.T) CorpusCase {
	lines := []string{"violates check constraint"}
	for i := 0; i < 40; i++ {
		lines = append(lines, "request ok")
	}
	return CorpusCase{Open: Snapshot{NamespaceName: "app"}, Recordings: []PrimitiveCall{{
		Method:    "Logs",
		Arguments: mustJSON(t, []any{sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "web-0"}, "web", time.Minute}),
		Result:    mustJSON(t, strings.Join(lines, "\n")),
	}}}
}

// A Diagnose that needs an old log line passes on a quiet capture and misses on a busy pod's bounded read.
func TestCheckDiagnoseLogRobustRefusesALogGatedMatch(t *testing.T) {
	err := CheckDiagnoseLogRobust(logDiagnoser{}, logCase(t))
	if err == nil || !strings.Contains(err.Error(), "program.log-gated-diagnose") {
		t.Fatalf("log-gated: %v", err)
	}
	if err := CheckDiagnoseLogRobust(logDiagnoser{stateOnly: true}, logCase(t)); err != nil {
		t.Fatalf("state-based: %v", err)
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
