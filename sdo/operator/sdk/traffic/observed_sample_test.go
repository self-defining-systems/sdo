package traffic

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// reasonEndpoint judges with its own reason, as a scenario with a custom check does.
type reasonEndpoint struct{}

func (reasonEndpoint) Build(context.Context, *rand.Rand, State) (Request, error) {
	return Request{Method: http.MethodGet, Path: "/branches"}, nil
}

func (reasonEndpoint) Check(response Response, _ State) Verdict {
	if response.Status != http.StatusOK {
		return Fail("HTTP %d deleting a leftover branch, want 200", response.Status)
	}
	return Pass()
}

type answerDoer struct {
	status int
	body   string
	err    error
}

func (d answerDoer) Do(request *http.Request) (*http.Response, error) {
	if d.err != nil {
		return nil, &url.Error{Op: request.Method, URL: request.URL.String(), Err: d.err}
	}
	return &http.Response{StatusCode: d.status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(d.body)), Request: request}, nil
}

func iterateOnce(t *testing.T, client Doer) Sample {
	t.Helper()
	scenario := Scenario{ID: "branches", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectRead,
		Steps: []Step{{Name: "list", Endpoint: reasonEndpoint{}}}}
	_, sample := setupEngine(t, scenario, client).Iterate(t.Context(), 0)
	return sample
}

// A custom check's own reason says what the probe expected; the sample still
// keeps what the server said.
func TestFailedSampleKeepsTheServersBodyBesideTheChecksReason(t *testing.T) {
	sample := iterateOnce(t, answerDoer{status: http.StatusTooManyRequests, body: "Retry later"})
	if sample.Outcome != OutcomeError || sample.Status != http.StatusTooManyRequests {
		t.Fatalf("sample = %+v", sample)
	}
	if !strings.Contains(sample.Error, "leftover branch") {
		t.Errorf("the check's reason was lost: %q", sample.Error)
	}
	if !strings.Contains(sample.Observed, "Retry later") {
		t.Errorf("the server's body was lost: observed %q", sample.Observed)
	}
}

// A transport failure keeps its cause without the client's method and URL
// prefix, which the sample's request already carries.
func TestFailedSampleKeepsTheTransportCause(t *testing.T) {
	sample := iterateOnce(t, answerDoer{err: errors.New("dial tcp 10.0.0.1:80: connect: connection refused")})
	if sample.Observed != "dial tcp 10.0.0.1:80: connect: connection refused" {
		t.Fatalf("observed = %q, want the bare transport cause", sample.Observed)
	}
}

// The server's body is redacted and bounded like every other sample text.
func TestObservedBodyIsRedactedAndBounded(t *testing.T) {
	sample := iterateOnce(t, answerDoer{status: http.StatusBadGateway, body: "token: hunter2hunter2 " + strings.Repeat("x ", 400)})
	if strings.Contains(sample.Observed, "hunter2") || len(sample.Observed) > MaxErrorExcerpt+len("…") {
		t.Fatalf("observed = %q", sample.Observed)
	}
}
