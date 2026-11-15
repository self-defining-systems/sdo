package jaeger

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/telemetrytest"
)

var epoch = time.Unix(1_700_000_000, 0).UTC()

// doer answers Jaeger's query API from canned bodies, in process.
type doer struct {
	behavior telemetrytest.Behavior
	bodies   map[string]string // by URL path
}

func (d *doer) Do(r *http.Request) (*http.Response, error) {
	reply := func(code int, body string) (*http.Response, error) {
		return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
	}
	switch d.behavior {
	case telemetrytest.BehaviorHang:
		<-r.Context().Done()
		return nil, r.Context().Err()
	case telemetrytest.BehaviorServerError:
		return reply(500, "boom")
	case telemetrytest.BehaviorMalformed:
		return reply(200, "{not json")
	}
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	if r.URL.Path == "/api/traces" && (r.URL.Query().Get("service") == "" || r.URL.Query().Get("start") == "" || r.URL.Query().Get("end") == "") {
		return reply(400, "service, start and end are required")
	}
	body, ok := d.bodies[r.URL.Path]
	if !ok {
		return reply(404, "trace not found")
	}
	return reply(200, body)
}

func micros(t time.Time) string { return strconv.FormatInt(t.UnixMicro(), 10) }

func failingTrace() string {
	return `{"traceID":"aaaa","processes":{"p1":{"serviceName":"front"},"p2":{"serviceName":"db"}},"spans":[
	 {"traceID":"aaaa","spanID":"s1","operationName":"GET /items","startTime":` + micros(epoch) + `,"duration":9000,"processID":"p1","references":[],
	  "tags":[{"key":"http.route","value":"/items"}]},
	 {"traceID":"aaaa","spanID":"s2","operationName":"query","startTime":` + micros(epoch.Add(time.Millisecond)) + `,"duration":5000,"processID":"p2",
	  "references":[{"refType":"CHILD_OF","spanID":"s1"}],
	  "tags":[{"key":"db.statement","value":"SELECT  *\n FROM items"},{"key":"error","value":true},{"key":"otel.status_description","value":"deadlock"}]}]}`
}

func okTrace() string {
	return `{"traceID":"bbbb","processes":{"p1":{"serviceName":"front"}},"spans":[{"traceID":"bbbb","spanID":"t1","operationName":"GET /health","startTime":` +
		micros(epoch.Add(-5*time.Second)) + `,"duration":100,"processID":"p1","tags":[]}]}`
}

func fakeJaeger() *doer {
	return &doer{bodies: map[string]string{
		"/api/services":    `{"data":["front","db","jaeger-all-in-one"]}`,
		"/api/traces":      `{"data":[` + failingTrace() + `,` + okTrace() + `]}`,
		"/api/traces/aaaa": `{"data":[` + failingTrace() + `]}`,
	}}
}

func TestTracesListSummarizesFailureAndSkipsJaegerOwnService(t *testing.T) {
	client := Client{BaseURL: "http://jaeger", HTTPClient: fakeJaeger()}
	services, err := client.Services(context.Background())
	if err != nil || len(services) != 2 {
		t.Fatalf("services = %v, %v", services, err)
	}
	query := telemetry.TraceQuery{Lookback: time.Hour, End: epoch.Add(time.Minute)}
	all, err := client.Traces(context.Background(), query)
	if err != nil || len(all) != 2 {
		t.Fatalf("traces = %+v, %v", all, err)
	}
	query.Failing = true
	failing, err := client.Traces(context.Background(), query)
	if err != nil || len(failing) != 1 {
		t.Fatalf("failing = %+v, %v", failing, err)
	}
	got := failing[0]
	if got.ID != "aaaa" || got.RootService != "front" || got.RootOperation != "GET /items" || got.Route != "/items" ||
		strings.Join(got.Services, ",") != "db,front" || got.FirstFailure != "db query: deadlock" || got.Duration != 9*time.Millisecond {
		t.Fatalf("summary = %+v", got)
	}
	if _, err := client.Traces(context.Background(), telemetry.TraceQuery{Lookback: time.Hour}); err == nil {
		t.Fatal("a listing without an end time is refused")
	}
}

func TestTraceKeepsParentsAndCollapsesStatements(t *testing.T) {
	client := Client{BaseURL: "http://jaeger", HTTPClient: fakeJaeger()}
	trace, err := client.Trace(context.Background(), "aaaa")
	if err != nil || len(trace.Spans) != 2 {
		t.Fatalf("trace = %+v, %v", trace, err)
	}
	child := trace.Spans[1]
	if child.ParentID != "s1" || child.Statement != "SELECT * FROM items" || !child.Failed {
		t.Fatalf("child = %+v", child)
	}
	if _, err := client.Trace(context.Background(), "../x"); err == nil {
		t.Fatal("a path in place of an ID is refused")
	}
	if _, err := client.Trace(context.Background(), "zzzz"); err == nil {
		t.Fatal("an unknown trace is an error")
	}
}

func TestServerErrorStatusFailsASpan(t *testing.T) {
	body := `{"traceID":"cccc","processes":{"p":{"serviceName":"api"}},"spans":[{"traceID":"cccc","spanID":"x","operationName":"GET /a","startTime":` +
		micros(epoch) + `,"duration":10,"processID":"p","tags":[{"key":"http.status_code","value":503}]}]}`
	d := &doer{bodies: map[string]string{"/api/traces/cccc": `{"data":[` + body + `]}`}}
	trace, err := Client{BaseURL: "http://jaeger", HTTPClient: d}.Trace(context.Background(), "cccc")
	if err != nil || !trace.Spans[0].Failed || trace.Spans[0].Error != "HTTP 503" {
		t.Fatalf("span = %+v, %v", trace.Spans, err)
	}
}

func TestAdapterIsRegisteredForTracesAndRefusesDetectorQueries(t *testing.T) {
	registry := telemetry.NewRegistry()
	if err := Register(registry); err != nil {
		t.Fatal(err)
	}
	binding := telemetry.Binding{ID: "traces", Adapter: ID, Kinds: []telemetry.Kind{telemetry.KindTraces}, Endpoint: telemetry.Endpoint{URL: "http://jaeger:16686"}}
	client, err := registry.Open(binding, telemetry.Env{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := client.(telemetry.TraceReader); !ok {
		t.Fatal("the client reads traces")
	}
	if _, err := client.Query(context.Background(), telemetry.Query{Kind: telemetry.KindTraces, Text: "x"}, epoch); err == nil {
		t.Fatal("detector queries are refused")
	}
	binding.Endpoint.URL = "ftp://x"
	if _, err := registry.Open(binding, telemetry.Env{}); err == nil {
		t.Fatal("bad endpoint is refused")
	}
}

func TestConformance(t *testing.T) {
	telemetrytest.TestClient(t, telemetrytest.Harness{
		Kind: telemetry.KindTraces,
		WantTraces: []telemetry.TraceSummary{{
			ID: "cc", Start: epoch, Duration: 2 * time.Millisecond,
			RootService: "front", RootOperation: "GET /", Services: []string{"front"}, Spans: 1,
		}},
		New: func(t *testing.T, b telemetrytest.Behavior) telemetry.Client {
			t.Helper()
			d := &doer{behavior: b, bodies: map[string]string{
				"/api/services": `{"data":["front"]}`,
				"/api/traces": `{"data":[{"traceID":"cc","processes":{"p":{"serviceName":"front"}},"spans":[{"traceID":"cc","spanID":"r","operationName":"GET /","startTime":` +
					micros(epoch) + `,"duration":2000,"processID":"p"}]}]}`,
			}}
			if b == telemetrytest.BehaviorEmpty {
				d.bodies["/api/traces"] = `{"data":[]}`
			}
			return Client{BaseURL: "http://jaeger", HTTPClient: d}
		},
		HangGrace: 500 * time.Millisecond,
	}, ID)
}
