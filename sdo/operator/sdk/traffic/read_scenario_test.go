package traffic

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

// A read scenario whose steps write fails every live iteration as invalid, so
// the static check rejects it before it reaches the controller, even when a
// setup step would fail first under the conformance check's simulated fault.
func TestReadScenarioRejectsWriteStepsStatically(t *testing.T) {
	for _, method := range []func(string, Params) *HTTPEndpoint{POST, PUT, DELETE} {
		scenario := laterScenario()
		scenario.Steps = append(scenario.Steps, Step{Name: "login", Endpoint: method("/login", Params{"user": Const("sdo-later-user")})})
		err := scenario.Validate()
		if err == nil || !strings.Contains(err.Error(), "login") {
			t.Fatalf("%s step in a read scenario: Validate() = %v, want an error naming the step", scenario.Steps[1].Endpoint.(*HTTPEndpoint).Method, err)
		}
		if failures := CheckCatalog(context.Background(), Catalog{scenario}); len(failures) == 0 {
			t.Fatalf("CheckCatalog accepted a read scenario with a %s step", scenario.Steps[1].Endpoint.(*HTTPEndpoint).Method)
		}
	}
	head := laterScenario()
	head.Steps = append(head.Steps, Step{Name: "head", Endpoint: &HTTPEndpoint{Method: http.MethodHead, Path: "/items"}})
	if err := head.Validate(); err != nil {
		t.Fatalf("a HEAD step in a read scenario was rejected: %v", err)
	}
	if err := laterScenario().Validate(); err != nil {
		t.Fatalf("a POST in a read scenario's setup was rejected: %v", err)
	}
}
