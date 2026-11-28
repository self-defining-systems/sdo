package traffic

import (
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// listDoer answers every request with a JSON list of the given names.
type listDoer struct{ names []string }

func (d listDoer) Do(request *http.Request) (*http.Response, error) {
	body := `{"items":[`
	for i, name := range d.names {
		if i > 0 {
			body += ","
		}
		body += `{"name":"` + name + `"}`
	}
	body += "]}"
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func recordsScenario() Scenario {
	return Scenario{
		ID: "list-existing", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectRead, Marker: "sdo-probe",
		Setup: []Step{{Name: "fixture", Endpoint: POST("/items", Params{"name": Const("sdo-probe-fixture")})}},
		Steps: []Step{{Name: "list", Endpoint: GET("/items", nil).ReadsExistingRecords("items", "name", `^.+$`)}},
	}
}

func iterateRecords(t *testing.T, names []string) Sample {
	t.Helper()
	engine := setupEngine(t, recordsScenario(), listDoer{names})
	_, sample := engine.Iterate(context.Background(), 0)
	return sample
}

// A fresh install has no user data: an empty marked read passes and is noted.
func TestEmptyExistingRecordsReadPasses(t *testing.T) {
	sample := iterateRecords(t, nil)
	if sample.Outcome != OutcomeOK || sample.ExistingRecords != ExistingRecordsEmpty {
		t.Fatalf("outcome=%s error=%q existing=%q", sample.Outcome, sample.Error, sample.ExistingRecords)
	}
}

func TestExistingRecordsExercisedOnlyByRecordsTheProbeDidNotCreate(t *testing.T) {
	if s := iterateRecords(t, []string{"sdo-probe-a", "alice"}); s.Outcome != OutcomeOK || s.ExistingRecords != ExistingRecordsExercised {
		t.Fatalf("%+v", s)
	}
	if s := iterateRecords(t, []string{"sdo-probe-a"}); s.Outcome != OutcomeOK || s.ExistingRecords != ExistingRecordsOwnOnly {
		t.Fatalf("%+v", s)
	}
}

func TestExistingRecordsShapeFailureOnANonEmptyListFails(t *testing.T) {
	scenario := recordsScenario()
	scenario.Steps[0].Endpoint = GET("/items", nil).ReadsExistingRecords("items", "name", `^[0-9]+$`)
	engine := setupEngine(t, scenario, listDoer{[]string{"alice"}})
	if _, sample := engine.Iterate(context.Background(), 0); sample.Outcome == OutcomeOK {
		t.Fatal("a malformed record passed")
	}
}

func TestExistingRecordsStepsFactsFindKeysTheScenarioWrote(t *testing.T) {
	own := Scenario{
		ID: "own", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectWriteWithCleanup, Marker: "sdo-probe",
		Setup: []Step{{Name: "create", Endpoint: POST("/items", Params{"name": Const("sdo-probe")}).SaveJSON("id", "own_id")}},
		Steps: []Step{{Name: "open", Endpoint: GET("/items/{own_id}", nil).ReadsExistingRecords("items", "name", `^.+$`)}},
	}
	facts := own.ExistingRecordsSteps()
	if len(facts) != 1 || !slices.Equal(facts[0].OwnKeys, []string{"own_id"}) || !facts[0].ChecksContent {
		t.Fatalf("%+v", facts)
	}
	// A key saved by a read step of existing data is not own.
	live := own
	live.Setup = nil
	live.Steps = []Step{
		{Name: "list", Endpoint: GET("/items", nil).SaveJSON("items.0.id", "first_id")},
		{Name: "open", Endpoint: GET("/items/{first_id}", nil).ReadsExistingRecords("items", "name", `^.+$`)},
	}
	if facts := live.ExistingRecordsSteps(); len(facts) != 1 || len(facts[0].OwnKeys) != 0 {
		t.Fatalf("%+v", facts)
	}
	// A sign-in token the read sends in a header is a credential, not a
	// record the probe wrote: the read still lists users' records.
	signedIn := own
	signedIn.Setup = []Step{{Name: "sign-in", Endpoint: POST("/login", nil).WithJSON(`{"user":"sdo-probe"}`, nil).SaveJSON("token", "token")}}
	signedIn.Steps = []Step{{Name: "list", Endpoint: GET("/items", nil).WithHeader("Authorization", "Bearer {token}").ReadsExistingRecords("items", "name", `^.+$`)}}
	if facts := signedIn.ExistingRecordsSteps(); len(facts) != 1 || len(facts[0].OwnKeys) != 0 {
		t.Fatalf("a header credential counted as own data: %+v", facts)
	}
	// A wrapper that embeds the marked endpoint to refine Check is still
	// the marked step.
	wrapped := signedIn
	wrapped.Steps = []Step{{Name: "list", Endpoint: wrappedRecords{GET("/items", nil).ReadsExistingRecords("items", "name", `^.+$`)}}}
	if facts := wrapped.ExistingRecordsSteps(); len(facts) != 1 {
		t.Fatalf("a wrapped marked step was not found: %+v", facts)
	}
	// An empty pattern checks nothing.
	empty := recordsScenario()
	empty.Steps[0].Endpoint = GET("/items", nil).ReadsExistingRecords("items", "name", "")
	if facts := empty.ExistingRecordsSteps(); len(facts) != 1 || facts[0].ChecksContent {
		t.Fatalf("%+v", facts)
	}
}

// The sample counts the probe-marked records a users' list showed, so a run
// can tell probe data that piles up in what users see from a steady fixture.
func TestExistingRecordsCountsTheProbeMarkedRecordsUsersSee(t *testing.T) {
	s := iterateRecords(t, []string{"sdo-probe-a-deletion_scheduled", "sdo-probe-b", "alice"})
	if s.Outcome != OutcomeOK || s.ExistingMarked != 2 {
		t.Fatalf("existingMarked=%d, want 2: %+v", s.ExistingMarked, s)
	}
}

type wrappedRecords struct{ *HTTPEndpoint }
