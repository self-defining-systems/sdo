package traffic

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall"
	"time"
)

// faultDoer simulates one fault class for every request.
type faultDoer struct{ class FaultClass }

func (d faultDoer) Do(request *http.Request) (*http.Response, error) {
	switch d.class {
	case FaultUnreachable:
		return nil, fmt.Errorf("dial tcp: connect: %w", syscall.ECONNREFUSED)
	case FaultErrorStatus:
		return stubResponse(request, http.StatusServiceUnavailable, "upstream connect error"), nil
	case FaultWrongBody:
		return stubResponse(request, http.StatusOK, ""), nil
	case FaultSlow:
		<-request.Context().Done()
		return nil, request.Context().Err()
	default:
		return nil, fmt.Errorf("unknown fault class %q", d.class)
	}
}

func stubResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status, Status: http.StatusText(status), Header: http.Header{},
		Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}
}

// CheckDetects runs scenario once against a simulated fault of class and
// returns an error unless the scenario fails the way a probe must: with an
// error or timeout, not by being unable to build its requests.
func CheckDetects(ctx context.Context, scenario Scenario, class FaultClass) error {
	if !knownFaultClasses[class] {
		return fmt.Errorf("unknown fault class %q", class)
	}
	workload := Workload{
		APIVersion: APIVersion, Kind: WorkloadKind, Name: "conformance", Purpose: PurposeHealthProbe,
		Timeout: Duration(50 * time.Millisecond), IterationTimeout: Duration(time.Second),
		Scenarios: []WorkloadScenario{{ID: scenario.ID}},
	}
	catalog := Catalog{scenario}
	if scenario.SideEffect != SideEffectRead {
		// A health probe needs a read scenario; conformance checks one
		// scenario at a time, so run writes under a journey profile.
		workload.Purpose, workload.Duration = PurposeJourney, Duration(time.Second)
	}
	engine, err := NewEngine(workload, catalog, faultDoer{class: class}, func(Target) string {
		return "http://conformance.invalid:80"
	}, nil)
	if err != nil {
		return err
	}
	_, sample := engine.Iterate(ctx, 0)
	switch sample.Outcome {
	case OutcomeError, OutcomeTimeout:
		return nil
	case OutcomeInvalid:
		return fmt.Errorf("scenario %s cannot build its first request: %s", scenario.ID, sample.Error)
	default:
		return fmt.Errorf("scenario %s does not detect fault class %s: it accepted the faulty response", scenario.ID, class)
	}
}

// CheckCatalog validates the catalog and checks every scenario detects its
// required and declared fault classes. Generated tests call it so a
// generator that cannot tell a broken application from a healthy one never
// reaches the controller.
func CheckCatalog(ctx context.Context, catalog Catalog) []error {
	if err := catalog.Validate(); err != nil {
		return []error{err}
	}
	var failures []error
	for _, scenario := range catalog {
		for _, class := range scenario.FaultClasses() {
			if err := CheckDetects(ctx, scenario, class); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return failures
}
