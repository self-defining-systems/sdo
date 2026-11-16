package traffic

import (
	"context"
	"math/rand/v2"
	"net/http"
	"reflect"
	"testing"
	"time"
)

// timedEndpoint is agent code that formats a date from the iteration's time.
type timedEndpoint struct{}

func (timedEndpoint) Build(_ context.Context, _ *rand.Rand, state State) (Request, error) {
	now, err := time.Parse(time.RFC3339, state[StateNow])
	if err != nil {
		return Request{}, err
	}
	return Request{Method: "POST", Path: "/pay", Query: map[string]string{
		"ref": "sdo-card-1", "expires": now.AddDate(1, 0, 0).Format("01/2006"), "day": now.Format(DateLayout),
	}}, nil
}
func (timedEndpoint) Check(Response, State) Verdict { return Pass() }

func cardScenario() Scenario {
	return Scenario{
		ID: "checkout", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectIdempotentWrite, Marker: "sdo-card",
		Steps: []Step{{Name: "pay", Endpoint: timedEndpoint{}}},
	}
}

func TestEngineTimeIsStateAndTheSampleRecordsIt(t *testing.T) {
	clock := newSimClock()
	var got map[string]string
	server := &simServer{clock: clock, handle: func(_ time.Time, r *http.Request) (int, string) {
		got = map[string]string{"expires": r.URL.Query().Get("expires"), "day": r.URL.Query().Get("day")}
		return 200, "ok"
	}}
	engine := simEngine(t, probeWorkload("checkout"), Catalog{cardScenario()}, server)
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeOK {
		t.Fatalf("sample: %+v", sample)
	}
	if want := map[string]string{"expires": "11/2027", "day": "2026-11-16"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("query = %v, want %v", got, want)
	}
	if sample.At != time.Date(2026, 11, 16, 9, 30, 0, 0, time.UTC) {
		t.Fatalf("sample.At = %s is not the iteration start", sample.At)
	}
}

// Replay with the failing sample's At builds the requests the iteration sent,
// whatever the clock says later.
func TestPlanAtReplaysTheRecordedTime(t *testing.T) {
	clock := newSimClock()
	server := &simServer{clock: clock, handle: func(time.Time, *http.Request) (int, string) { return 200, "ok" }}
	engine := simEngine(t, probeWorkload("checkout"), Catalog{cardScenario()}, server)
	_, sample := engine.Iterate(context.Background(), 0)
	clock.advance(400 * 24 * time.Hour)
	replayed, err := engine.PlanAt(context.Background(), 0, sample.At)
	if err != nil || replayed[0].Query["expires"] != "11/2027" {
		t.Fatalf("replay built %v (%v)", replayed, err)
	}
	if later, err := engine.Plan(context.Background(), 0); err != nil || later[0].Query["expires"] == "11/2027" {
		t.Fatalf("Plan must start now: %v %v", later, err)
	}
}

// A setup step's recorded State never freezes the time of a later iteration.
func TestSetupDoesNotFreezeEngineTime(t *testing.T) {
	scenario := cardScenario()
	scenario.Setup = []Step{{Name: "ensure", Endpoint: POST("/fixture", Params{"ref": Const("sdo-card-fixture")})}}
	clock := newSimClock()
	var days []string
	server := &simServer{clock: clock, handle: func(_ time.Time, r *http.Request) (int, string) {
		if r.URL.Path == "/pay" {
			days = append(days, r.URL.Query().Get("day"))
		}
		return 200, "ok"
	}}
	engine := simEngine(t, probeWorkload("checkout"), Catalog{scenario}, server)
	engine.Iterate(context.Background(), 0)
	clock.advance(48 * time.Hour)
	engine.Iterate(context.Background(), 1)
	if !reflect.DeepEqual(days, []string{"2026-11-16", "2026-11-18"}) {
		t.Fatalf("days %v: the second iteration reused the first one's time", days)
	}
}
