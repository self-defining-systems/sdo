package replay

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type recordedDetector struct{}

func (recordedDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "recorded", Class: sdk.DetectorClassIncident, Playbooks: []string{"recorded"}}
}
func (recordedDetector) Detect(_ context.Context, s sdk.DetectionContext) ([]sdk.Finding, error) {
	v := program.ReadFrom(s)
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "web"}
	logs, _ := v.Logs(ref, "web", time.Minute)
	_, _ = v.ProbeTCP(ref, 80)
	_, _ = v.ProbeHTTP(ref, program.HTTPProbe{Method: "GET", Path: "/"})
	_, _ = v.DiffAgainstSource(ref)
	_, _ = v.DiffAgainstBaseline(ref)
	_ = v.QueryMetrics(metrics.Query{ID: "up", Expr: "up"})
	_, _ = v.AlertsResult()
	if logs == "broken" {
		return []sdk.Finding{{Status: sdk.FindingActive}}, nil
	}
	return nil, nil
}
func TestMatrixDetectorReplaysAllReadsByPhase(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "web"}
	q := metrics.Query{ID: "up", Expr: "up"}
	c := Case{ID: "read-breadth", Corpus: sdktest.CorpusCase{Open: sdktest.Snapshot{NamespaceName: "app"}, Healthy: sdktest.Snapshot{NamespaceName: "app"}, RootCauseProgramIDs: []string{"recorded"}}}
	add := func(phase, method string, args, result any) {
		a, _ := json.Marshal(args)
		r, _ := json.Marshal(result)
		c.Corpus.Recordings = append(c.Corpus.Recordings, sdktest.PrimitiveCall{Phase: phase, Method: method, Arguments: a, Result: r})
	}
	for _, phase := range []string{"open", "healthy"} {
		logs := "ready"
		if phase == "open" {
			logs = "broken"
		}
		add(phase, "Logs", []any{ref, "web", time.Minute}, logs)
		add(phase, "ProbeTCP", []any{ref, 80}, program.ProbeResult{})
		add(phase, "ProbeHTTP", []any{ref, program.HTTPProbe{Method: "GET", Path: "/"}}, program.ProbeResult{Success: true})
		add(phase, "DiffAgainstSource", []any{ref}, program.Diff{Changed: true})
		add(phase, "DiffAgainstBaseline", []any{ref}, program.Diff{})
		add(phase, "QueryMetrics", []any{q}, metrics.Result{Query: q})
		add(phase, "AlertsResult", []any{}, map[string]any{"result": metrics.AlertsResult{}, "available": true})
	}
	registry, _ := program.NewRegistry(nil, nil)
	rows := Matrix(context.Background(), registry, []sdk.Detector{recordedDetector{}}, []Case{c})
	if len(rows) != 1 || rows[0].Status != "detected" {
		t.Fatalf("rows=%+v", rows)
	}
	// No healthy reads may be borrowed from Open, even when errors are ignored.
	c.Corpus.Recordings = c.Corpus.Recordings[:7]
	rows = Matrix(context.Background(), registry, []sdk.Detector{recordedDetector{}}, []Case{c})
	if rows[0].Status != "failed" || rows[0].Detectors["healthy"][0].Error == "" {
		t.Fatalf("missing healthy observations cleared detector: %+v", rows)
	}
}
