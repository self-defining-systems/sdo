package traffic

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// loginPage is a synthetic page whose head is long enough that a plain prefix
// of the body never reaches the title.
func loginPage(extra string) []byte {
	head := "<!DOCTYPE html><html><head><meta charset=\"utf-8\"><style>" + strings.Repeat("a{color:red}", 60) + "</style>" +
		"<title>Sign in · Example</title><script>var token = 'x';</script></head>"
	return []byte(head + "<body><h1>Welcome back</h1>" + extra + "</body></html>")
}

// A mismatched body assertion reports what the page actually said: the status,
// the page title and the text near the expected phrase, not the page's head.
func TestBodyMismatchReportsTheObservedTitleAndStatus(t *testing.T) {
	endpoint := GET("/users/sign_in", nil).Contains("Sign in to Example")
	got := endpoint.Check(Response{Status: http.StatusOK, Body: loginPage("")}, State{})
	if got.OK {
		t.Fatal("a body without the expected text passed")
	}
	for _, want := range []string{"HTTP 200", `"Sign in to Example"`, "Sign in · Example"} {
		if !strings.Contains(got.Reason, want) {
			t.Errorf("reason %q lacks %q", got.Reason, want)
		}
	}
	if len(got.Reason) > 2*MaxErrorExcerpt+len(`HTTP 200 body lacks "Sign in to Example": `) {
		t.Errorf("reason is not bounded: %d bytes", len(got.Reason))
	}
}

// Anything secret-looking in the observed response is redacted before it can
// reach an agent.
func TestObservedExcerptRedactsSecrets(t *testing.T) {
	page := []byte(`<html><title>Sign in · Example</title><body>`+`<input name="authenticity_token" value="AbCdEf0123456789AbCdEf0123456789AbCdEf==">` +
		`<p>password: hunter2hunter2 Bearer abcdefghijklmnop0123456789</p></body></html>`)
	got := GET("/", nil).Contains("Missing phrase").Check(Response{Status: http.StatusOK, Body: page}, State{})
	if !strings.Contains(got.Reason, "[redacted]") {
		t.Fatalf("nothing was redacted: %s", got.Reason)
	}
	for _, secret := range []string{"AbCdEf0123456789", "hunter2", "abcdefghijklmnop0123456789"} {
		if strings.Contains(got.Reason, secret) {
			t.Errorf("reason leaks %q: %s", secret, got.Reason)
		}
	}
}

type windowContext struct {
	sdk.DetectionContext
	window Window
}

func (c windowContext) TrafficWindow(string) (Window, bool) { return c.window, true }

// The pending reason of a scenario that never succeeded carries its latest
// observed failure (step, request, status, response excerpt), so the author
// can see why instead of only that it never passed.
func TestConfirmationReasonCarriesTheLatestObservedFailure(t *testing.T) {
	at := time.Unix(1000, 0)
	window := Window{
		Workload: Workload{Name: "probe-call", Scenarios: []WorkloadScenario{{ID: "sign-in"}}},
		Scenarios: []ScenarioObservations{{
			Scenario: Descriptor{ID: "sign-in"},
			Samples: []Sample{
				{At: at, Outcome: OutcomeError, Status: 500, Step: "old", Request: "GET /old", Error: "HTTP 500: old failure"},
				{At: at.Add(time.Second), Outcome: OutcomeError, Status: 200, Step: "open", Request: "GET /users/sign_in", Error: `HTTP 200 body lacks "Sign in to Example": title "Sign in · Example"`},
			},
		}},
	}
	reason := NewDetector(sdk.DetectorSpec{ID: "traffic-probe-call"}, "probe-call").(interface {
		ConfirmationReason(sdk.DetectionContext) string
	}).ConfirmationReason(windowContext{window: window})
	for _, want := range []string{"sign-in", "open", "GET /users/sign_in", "HTTP 200", "Sign in · Example"} {
		if !strings.Contains(reason, want) {
			t.Errorf("reason %q lacks %q", reason, want)
		}
	}
	if strings.Contains(reason, "old failure") {
		t.Errorf("reason reports a stale failure: %s", reason)
	}
}
