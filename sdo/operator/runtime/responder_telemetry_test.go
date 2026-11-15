package runtime

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/telemetry"
)

type fakeMetrics struct {
	queries []metrics.Query
	answer  func(metrics.Query) []metrics.Series
}

func (f *fakeMetrics) Query(_ context.Context, q metrics.Query, at time.Time) ([]metrics.Series, error) {
	f.queries = append(f.queries, q)
	return f.answer(q), nil
}
func (f *fakeMetrics) Alerts(context.Context) ([]metrics.Alert, error) { return nil, nil }

func series(value float64, labels map[string]string) metrics.Series {
	return metrics.Series{Labels: labels, Samples: []metrics.Sample{{At: detectNow(), Value: value}}}
}

func telemetryLoop(t *testing.T, source ApplicationTelemetry) *ResponderLoop {
	t.Helper()
	view := detectApp()
	var extra corev1.Pod
	for _, pod := range view.PodList {
		if pod.Name == "api-0" {
			extra = *pod.DeepCopy()
		}
	}
	extra.Name = "api-1"
	view.PodList = append(view.PodList, extra)
	loop, _ := onDemandLoop(t, view, &staticAppState{})
	loop.Telemetry = func() ApplicationTelemetry { return source }
	return loop
}

func TestMetricsPrintsLargestSeriesFirstAndRangeUsesSinceAndStep(t *testing.T) {
	api := &fakeMetrics{answer: func(q metrics.Query) []metrics.Series {
		return []metrics.Series{series(2, map[string]string{"route": "/a"}), series(40, map[string]string{"route": "/b"})}
	}}
	loop := telemetryLoop(t, ApplicationTelemetry{Metrics: api})
	response := loop.Handle(context.Background(), ToolRequest{Command: "metrics", Query: `sum by (route) (rate(errors[1m]))`})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	rows := parseCSV(t, []byte(response.Text))
	if len(rows) != 3 || !strings.Contains(rows[1][0], "/b") || rows[1][1] != "40" {
		t.Fatalf("rows = %v", rows)
	}
	response = loop.Handle(context.Background(), ToolRequest{Command: "metrics", Query: "up", Since: "15m", Step: "15s"})
	last := api.queries[len(api.queries)-1]
	if response.Error != "" || last.Range != 15*time.Minute || last.Step != 15*time.Second {
		t.Fatalf("range query = %+v, error %q", last, response.Error)
	}
	if response = loop.Handle(context.Background(), ToolRequest{Command: "metrics", Query: "up", Step: "15s"}); response.Error == "" {
		t.Fatal("--step without --since is refused")
	}
}

func TestMetricsListNamesMetricsAndLabelsOfOne(t *testing.T) {
	api := &fakeMetrics{answer: func(q metrics.Query) []metrics.Series {
		if strings.HasPrefix(q.Expr, "count by") {
			if strings.Contains(q.Expr, "http_errors") {
				return []metrics.Series{series(3, map[string]string{"__name__": "http_errors_total"})}
			}
			return []metrics.Series{series(3, map[string]string{"__name__": "http_errors_total"}), series(1, map[string]string{"__name__": "up"})}
		}
		return []metrics.Series{series(1, map[string]string{"__name__": "http_errors_total", "route": "/a", "code": "500"}), series(1, map[string]string{"__name__": "http_errors_total", "route": "/b", "code": "500"})}
	}}
	loop := telemetryLoop(t, ApplicationTelemetry{Metrics: api})
	response := loop.Handle(context.Background(), ToolRequest{Command: "metrics", List: true})
	if rows := parseCSV(t, []byte(response.Text)); len(rows) != 3 || rows[1][0] != "http_errors_total" {
		t.Fatalf("names:\n%s", response.Text)
	}
	response = loop.Handle(context.Background(), ToolRequest{Command: "metrics", List: true, Name: "http_errors"})
	if !strings.Contains(response.Text, "label route: /a, /b") || !strings.Contains(response.Text, "label code: 500") {
		t.Fatalf("labels:\n%s", response.Text)
	}
}

func TestMetricsWithoutASourceSaysWhatIsMissing(t *testing.T) {
	loop := telemetryLoop(t, ApplicationTelemetry{})
	response := loop.Handle(context.Background(), ToolRequest{Command: "metrics", Query: "up"})
	if !strings.Contains(response.Error, "no metrics source is bound") {
		t.Fatalf("error = %q", response.Error)
	}
}

func fakeLogs(byPod map[string]string) PodLogReader {
	return func(_ context.Context, _, pod string, _ *corev1.PodLogOptions) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(byPod[pod])), nil
	}
}

func TestLogsFailingKeepsErrorAndFailedRequestLinesAcrossPodsAndRollsUpRoutes(t *testing.T) {
	stamp := func(s int) string { return detectNow().Add(time.Duration(s) * time.Second).Format(time.RFC3339Nano) }
	loop := telemetryLoop(t, ApplicationTelemetry{PodLogs: fakeLogs(map[string]string{
		"api-0": stamp(-5) + ` {"method":"GET","path":"/items","status":200}` + "\n" +
			stamp(-4) + ` {"method":"POST","path":"/items?token=abc","status":500}` + "\n",
		"api-1": stamp(-3) + ` 10.0.0.1 - - [x] "POST /items HTTP/1.1" 502 12` + "\n" +
			stamp(-2) + " level=info started\n" + stamp(-1) + " ERROR db unreachable password=hunter2\n",
	})})
	response := loop.Handle(context.Background(), ToolRequest{Command: "logs", Name: "api", Failing: true})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	text := response.Text
	for _, want := range []string{"api-1", "api-0 POST /items -> 500", "502", "ERROR db unreachable", "# failing requests: 1 x POST /items -> 500", "# failing requests: 1 x POST /items -> 502"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q:\n%s", want, text)
		}
	}
	for _, unwanted := range []string{`status":200`, "started", "hunter2"} {
		if strings.Contains(text, unwanted) {
			t.Fatalf("unexpected %q:\n%s", unwanted, text)
		}
	}
	response = loop.Handle(context.Background(), ToolRequest{Command: "logs", Name: "api", Grep: "started"})
	if !strings.Contains(response.Text, "started") || strings.Contains(response.Text, "ERROR") {
		t.Fatalf("--grep:\n%s", response.Text)
	}
	if response = loop.Handle(context.Background(), ToolRequest{Command: "logs", Name: "nothing"}); !strings.Contains(response.Error, "no pods found") {
		t.Fatalf("unknown component: %q", response.Error)
	}
	if response = loop.Handle(context.Background(), ToolRequest{Command: "logs", Name: "api", Grep: "("}); response.Error == "" {
		t.Fatal("a bad --grep is refused")
	}
}

func TestLogsResolvesAMapComponentToItsWorkloadPods(t *testing.T) {
	loop := telemetryLoop(t, ApplicationTelemetry{PodLogs: fakeLogs(map[string]string{"db-0": detectNow().Format(time.RFC3339Nano) + " ERROR disk full\n"})})
	response := loop.Handle(context.Background(), ToolRequest{Command: "logs", Name: "db", Failing: true})
	if response.Error != "" || !strings.Contains(response.Text, "db-0") {
		t.Fatalf("%q\n%s", response.Error, response.Text)
	}
}

func TestTracesWithoutAStoreSaysSoAndHowToEmit(t *testing.T) {
	loop := telemetryLoop(t, ApplicationTelemetry{})
	response := loop.Handle(context.Background(), ToolRequest{Command: "traces", Failing: true})
	if !strings.Contains(response.Error, "no trace store is bound") || !strings.Contains(response.Error, "OpenTelemetry") {
		t.Fatalf("error = %q", response.Error)
	}
	loop = telemetryLoop(t, ApplicationTelemetry{Sources: []telemetry.Source{{ID: "tempo", Kinds: []telemetry.Kind{telemetry.KindTraces}}}})
	response = loop.Handle(context.Background(), ToolRequest{Command: "traces"})
	if !strings.Contains(response.Text+response.Error, "tempo") {
		t.Fatalf("a bound but unreadable store is named: %q", response.Error)
	}
}

func TestMapShowsWhereToObserveEachComponent(t *testing.T) {
	doc := strings.Replace(mapTestDoc, "probes:", "entries:\n  - {service: front, metric: http_requests_total, selector: {job: front}, labels: {route: route, status: code}}\nprobes:", 1)
	loop := mapLoop(doc, nil, "create")
	// No trace store: no trace pointer.
	near := mapCall(t, loop, "near", "front")
	wantAll(t, near.Text, `observe: metrics http_requests_total{job="front"} (labels route=route, status=code); logs: sdo logs front (pods front-0)`)
	if strings.Contains(near.Text, "traces:") {
		t.Fatalf("no trace store, no trace pointer:\n%s", near.Text)
	}
	path := mapCall(t, loop, "path", "create")
	wantAll(t, path.Text, "observe front: metrics http_requests_total", "observe api: logs: sdo logs api (pods api-0)", "observe db: logs: sdo logs db (pods db-0)")
	loop.Telemetry = func() ApplicationTelemetry {
		return ApplicationTelemetry{Sources: []telemetry.Source{{ID: "tempo", Kinds: []telemetry.Kind{telemetry.KindTraces}}}}
	}
	wantAll(t, mapCall(t, loop, "near", "api").Text, "traces: service api")
}
