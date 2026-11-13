package traffic

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type sizedAPI struct{ size int }

func (a sizedAPI) Do(request *http.Request) (*http.Response, error) {
	body := `[` + strings.Repeat(`{"name":"repo"},`, a.size/16) + `{"name":"repo"}]`
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func listScenario() Scenario {
	return Scenario{
		ID: "list-repos", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectRead,
		Steps: []Step{{Name: "list", Endpoint: GET("/api/repos", Params{"per_page": Const("100")}).Each("", "name", "^repo$")}},
	}
}

// A response past the engine's cap is a probe-authoring defect, never an
// application failure: the sample is invalid, names the step and the cap, and
// carries the machine-readable defect.
func TestOversizedResponseIsAProbeDefectNotAFailure(t *testing.T) {
	engine := setupEngine(t, listScenario(), sizedAPI{size: 2 * MaxResponseBody})
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeInvalid || sample.Defect != DefectResponseTruncated || sample.ByteCap != MaxResponseBody || sample.Step != "list" {
		t.Fatalf("sample = %+v", sample)
	}
	if !strings.Contains(sample.Error, "response_truncated") || !strings.Contains(sample.Error, "page size") {
		t.Fatalf("message is not actionable: %q", sample.Error)
	}
}

// An in-cap response is judged normally, and an oversized one that the check
// still accepts is not a defect.
func TestInCapResponseIsJudgedNormally(t *testing.T) {
	engine := setupEngine(t, listScenario(), sizedAPI{size: 1024})
	if _, sample := engine.Iterate(context.Background(), 0); sample.Outcome != OutcomeOK || sample.Defect != "" {
		t.Fatalf("sample = %+v", sample)
	}
	status := listScenario()
	status.Steps = []Step{{Name: "list", Endpoint: GET("/api/repos", nil)}}
	big := setupEngine(t, status, sizedAPI{size: 2 * MaxResponseBody})
	if _, sample := big.Iterate(context.Background(), 0); sample.Outcome != OutcomeOK {
		t.Fatalf("sample = %+v", sample)
	}
}

// The scenario's window says why it is unproven, naming the defect and that
// it is not an application failure, so the designer and judge can act.
func TestWindowReasonNamesTheProbeDefect(t *testing.T) {
	engine := setupEngine(t, listScenario(), sizedAPI{size: 2 * MaxResponseBody})
	_, sample := engine.Iterate(context.Background(), 0)
	reason := latestFailure(ScenarioObservations{Samples: []Sample{sample}, Invalid: 1, LastInvalid: sample.Error})
	for _, want := range []string{"probe defect response_truncated", "not an application failure", "list", "reduce page size"} {
		if !strings.Contains(reason, want) {
			t.Fatalf("reason %q lacks %q", reason, want)
		}
	}
}

// A window of nothing but truncation defects is never a failing user test:
// Evaluate ignores invalid samples, so no incident can open or be held.
func TestTruncationDefectsNeverFailAScenario(t *testing.T) {
	engine := setupEngine(t, listScenario(), sizedAPI{size: 2 * MaxResponseBody})
	var samples []Sample
	for i := uint64(0); i < 20; i++ {
		_, sample := engine.Iterate(context.Background(), i)
		samples = append(samples, sample)
	}
	last := samples[len(samples)-1]
	verdict := Evaluate("list-repos", SLO{Window: 20, MinSamples: 1, MaxAge: Duration(time.Hour)}, samples, last.At)
	if !verdict.Healthy || verdict.Errors != 0 || verdict.Timeouts != 0 || verdict.Samples != 0 {
		t.Fatalf("verdict = %+v", verdict)
	}
}
