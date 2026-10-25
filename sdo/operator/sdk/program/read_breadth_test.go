package program_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestDiagnoseReadsRecordedPrimitives(t *testing.T) {
	args, _ := json.Marshal([]any{serviceRef, "web", time.Minute})
	snapshot := sdktest.Snapshot{NamespaceName: "demo", Readings: []sdktest.PrimitiveCall{{Method: "Logs", Arguments: args, Result: json.RawMessage(`"broken"`)}}}
	p := diagnostic{fn: func(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
		logs, err := v.Logs(serviceRef, "web", time.Minute)
		if err != nil {
			return program.Verdict{}, err
		}
		if logs == "broken" {
			return program.Verdict{Result: program.Match}, nil
		}
		return program.Verdict{Result: program.NoMatch}, nil
	}}
	b := program.Bindings{Objects: program.ObjectBindings{"service": serviceRef}}
	got := program.RunDiagnose(context.Background(), p, program.ReadFrom(snapshot), b, time.Second)
	if got.Result != program.Match {
		t.Fatalf("recorded primitive lost: %+v", got)
	}
	snapshot.Readings = nil
	if got = program.RunDiagnose(context.Background(), p, program.ReadFrom(snapshot), b, time.Second); got.Result != program.Unknown {
		t.Fatalf("missing read cleared diagnosis: %+v", got)
	}

}

func TestDiagnoseReadsMetricsAndDoesNotIgnoreUnavailableMetrics(t *testing.T) {
	q := metrics.Query{ID: "load", Expr: "up"}
	p := diagnostic{fn: func(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
		_, _ = metrics.ResultFrom(v, "load")
		_, _ = metrics.AlertsFrom(v)
		_ = v.QueryMetrics(q)
		return program.Verdict{Result: program.NoMatch}, nil
	}}
	b := program.Bindings{Objects: program.ObjectBindings{"service": serviceRef}}
	for _, unavailable := range []bool{false, true} {
		result := metrics.Result{Query: q}
		alerts := metrics.AlertsResult{}
		if unavailable {
			result.Error = "offline"
			alerts.Error = "offline"
		}
		view := program.ReadFrom(sdktest.Snapshot{Metrics: map[string]metrics.Result{"load": result}, Alerts: &alerts})
		got := program.RunDiagnose(context.Background(), p, view, b, time.Second)
		want := program.NoMatch
		if unavailable {
			want = program.Unknown
		}
		if got.Result != want {
			t.Fatalf("unavailable=%v got=%+v want=%s", unavailable, got, want)
		}
	}
}
