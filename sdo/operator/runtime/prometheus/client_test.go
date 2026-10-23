package prometheus

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/metrics"
)

var at = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func serve(t *testing.T, handler http.HandlerFunc) Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return Client{BaseURL: server.URL}
}

func respond(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

func TestInstantQueryParsesVectorAtTheRequestedTime(t *testing.T) {
	var got url.Values
	var path string
	client := serve(t, func(w http.ResponseWriter, r *http.Request) {
		got, path = r.URL.Query(), r.URL.Path
		respond(`{"status":"success","data":{"resultType":"vector","result":[
			{"metric":{"deployment":"web","namespace":"shop"},"value":[1790856000.5,"0.25"]},
			{"metric":{"deployment":"api"},"value":[1790856000.5,"NaN"]},
			{"metric":{"deployment":"db"},"value":[1790856000.5,"+Inf"]}]}}`)(w, r)
	})
	series, err := client.Query(context.Background(), metrics.Query{ID: "q", Expr: `sum(rate(x[1m]))`}, at)
	if err != nil {
		t.Fatal(err)
	}
	if path != "/api/v1/query" || got.Get("query") != `sum(rate(x[1m]))` || got.Get("time") != "1790856000" {
		t.Fatalf("request %s %v", path, got)
	}
	if len(series) != 3 || series[0].Labels["deployment"] != "web" || len(series[0].Samples) != 1 || series[0].Samples[0].Value != 0.25 {
		t.Fatalf("series: %+v", series)
	}
	if !math.IsNaN(series[1].Samples[0].Value) || !math.IsInf(series[2].Samples[0].Value, 1) {
		t.Fatalf("non-finite values: %+v", series)
	}
	if want := time.Unix(1790856000, 500_000_000).UTC(); !series[0].Samples[0].At.Equal(want) {
		t.Fatalf("sample time %s want %s", series[0].Samples[0].At, want)
	}
}

func TestScalarResultIsOneUnlabelledSeries(t *testing.T) {
	client := serve(t, respond(`{"status":"success","data":{"resultType":"scalar","result":[1790856000,"7"]}}`))
	series, err := client.Query(context.Background(), metrics.Query{ID: "q", Expr: "scalar(up)"}, at)
	if err != nil || len(series) != 1 || series[0].Samples[0].Value != 7 || len(series[0].Labels) != 0 {
		t.Fatalf("series %+v err %v", series, err)
	}
}

func TestRangeQueryUsesQueryRangeEndingAtTheRequestedTime(t *testing.T) {
	var got url.Values
	var path string
	client := serve(t, func(w http.ResponseWriter, r *http.Request) {
		got, path = r.URL.Query(), r.URL.Path
		respond(`{"status":"success","data":{"resultType":"matrix","result":[
			{"metric":{"service":"web"},"values":[[1790855700,"1"],[1790855760,"2"],[1790855820,"3"]]}]}}`)(w, r)
	})
	query := metrics.Query{ID: "q", Expr: "up", Range: 5 * time.Minute, Step: time.Minute}
	series, err := client.Query(context.Background(), query, at)
	if err != nil {
		t.Fatal(err)
	}
	if path != "/api/v1/query_range" || got.Get("start") != "1790855700" || got.Get("end") != "1790856000" || got.Get("step") != "60" {
		t.Fatalf("request %s %v", path, got)
	}
	if len(series) != 1 || len(series[0].Samples) != 3 || series[0].Samples[2].Value != 3 {
		t.Fatalf("series: %+v", series)
	}
}

func TestAlertsParsesStatesAndAnnotations(t *testing.T) {
	client := serve(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/alerts" {
			http.NotFound(w, r)
			return
		}
		respond(`{"status":"success","data":{"alerts":[
			{"labels":{"alertname":"A","namespace":"shop","severity":"critical"},"annotations":{"summary":"s"},"state":"firing","activeAt":"2026-10-01T11:55:00.5Z","value":"1e+00"},
			{"labels":{"alertname":"B"},"annotations":{},"state":"pending","activeAt":"2026-10-01T11:59:00Z","value":"2"}]}}`)(w, r)
	})
	alerts, err := client.Alerts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 2 || alerts[0].Name() != "A" || alerts[0].State != "firing" || alerts[0].Annotations["summary"] != "s" ||
		alerts[0].Value != "1e+00" || !alerts[0].ActiveAt.Equal(time.Date(2026, 10, 1, 11, 55, 0, 500_000_000, time.UTC)) || alerts[1].State != "pending" {
		t.Fatalf("alerts: %+v", alerts)
	}
}

func TestFailuresAreErrors(t *testing.T) {
	query := metrics.Query{ID: "q", Expr: "up"}
	cases := map[string]struct {
		handler http.HandlerFunc
		want    string
	}{
		"server error": {func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "overloaded", http.StatusServiceUnavailable)
		}, "503"},
		"bad query": {func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"parse error: unexpected end of input"}`))
		}, "parse error"},
		"error status in 200": {respond(`{"status":"error","errorType":"timeout","error":"query timed out"}`), "query timed out"},
		"malformed json":      {respond(`{"status":"succ`), "decode"},
		"not json":            {respond(`<html>login</html>`), "decode"},
		"string result":       {respond(`{"status":"success","data":{"resultType":"string","result":[1,"x"]}}`), "string"},
		"bad sample value":    {respond(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"abc"]}]}}`), "abc"},
		"missing data":        {respond(`{"status":"success"}`), "result"},
		"huge response": {func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(strings.Repeat(" ", maxResponseBytes+10)))
		}, "exceeds"},
		"redirect to nowhere": {func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/login", http.StatusFound) }, ""},
	}
	for name, tc := range cases {
		client := serve(t, tc.handler)
		if _, err := client.Query(context.Background(), query, at); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: want error containing %q, got %v", name, tc.want, err)
		}
	}
	if _, err := (Client{BaseURL: "http://127.0.0.1:1"}).Query(context.Background(), query, at); err == nil {
		t.Error("unreachable source must be an error")
	}
	if _, err := (Client{}).Alerts(context.Background()); err == nil {
		t.Error("a client without an address must fail")
	}
}

func TestRequestHonoursContextDeadline(t *testing.T) {
	release := make(chan struct{})
	client := serve(t, func(http.ResponseWriter, *http.Request) { <-release })
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := client.Alerts(ctx); err == nil {
		t.Fatal("expected a deadline error")
	}
	if time.Since(started) > 2*time.Second {
		t.Fatalf("deadline ignored: %s", time.Since(started))
	}
}

func TestBearerTokenIsReadFromFileOnEveryRequest(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	var seen []string
	client := serve(t, func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		respond(`{"status":"success","data":{"alerts":[]}}`)(w, r)
	})
	client.TokenFile = tokenFile
	if err := os.WriteFile(tokenFile, []byte("first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Alerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenFile, []byte("second\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Alerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(seen, "|") != "Bearer first|Bearer second" {
		t.Fatalf("authorization headers: %v", seen)
	}
	if err := os.Remove(tokenFile); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Alerts(context.Background()); err == nil {
		t.Fatal("an unreadable token file must be an error, not an unauthenticated request")
	}
}

func TestBaseURLPathPrefixIsKept(t *testing.T) {
	var path string
	client := serve(t, func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		respond(`{"status":"success","data":{"alerts":[]}}`)(w, r)
	})
	client.BaseURL += "/prometheus/"
	if _, err := client.Alerts(context.Background()); err != nil || path != "/prometheus/api/v1/alerts" {
		t.Fatalf("path %q err %v", path, err)
	}
}
