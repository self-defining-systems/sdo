package journeycheck

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// store is a small item service: POST creates, GET reads, DELETE removes.
type store struct {
	mu    sync.Mutex
	items map[string]string
	next  int
	// failDelete refuses deletes; constant answers a duplicate name with 409.
	failDelete, unique bool
	// deferDelete accepts a delete but only schedules it, the way an
	// application with delayed deletion does: the item stays, renamed.
	deferDelete bool
}

func answer(r *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}
}

func (s *store) Do(r *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := strings.TrimPrefix(r.URL.Path, "/items/")
	switch r.Method {
	case http.MethodPost:
		raw, _ := io.ReadAll(r.Body)
		var in struct{ Name string }
		_ = json.Unmarshal(raw, &in)
		s.next++
		key := fmt.Sprint(s.next)
		s.items[key] = in.Name
		return answer(r, 201, fmt.Sprintf(`{"id":%q,"name":%q}`, key, in.Name)), nil
	case http.MethodDelete:
		if s.failDelete {
			return answer(r, 500, "no"), nil
		}
		if s.deferDelete {
			if name, ok := s.items[id]; ok {
				s.items[id] = name + "-deletion_scheduled"
			}
			return answer(r, 202, ""), nil
		}
		delete(s.items, id)
		return answer(r, 204, ""), nil
	}
	name, ok := s.items[id]
	if !ok {
		return answer(r, 404, "missing"), nil
	}
	return answer(r, 200, fmt.Sprintf(`{"id":%q,"name":%q}`, id, name)), nil
}

func scenario(readsBack bool) traffic.Scenario {
	read := traffic.GET("/items/{id}", nil).Status(200)
	if readsBack {
		read = traffic.GET("/items/{id}", nil).JSONEquals("name", "{name}")
	}
	return traffic.Scenario{
		ID: "write-item", Description: "create an item and read it back", Target: traffic.Target{Service: "web", Port: 80},
		Marker: "sdoprobe", SideEffect: traffic.SideEffectWriteWithCleanup, DependsOn: []string{"web"},
		Steps: []traffic.Step{
			{Name: "create", Endpoint: traffic.POST("/items", nil).WithJSON(`{"name":"{name}"}`, traffic.Params{"name": traffic.Unique("sdoprobe-")}).Status(201).SaveJSON("id", "id").SaveJSON("name", "name")},
			{Name: "read", Endpoint: read},
		},
		Cleanup: []traffic.Step{
			{Name: "delete", Endpoint: traffic.DELETE("/items/{id}", traffic.Params{"probe": traffic.Const("sdoprobe")}).Status(202, 204)},
			{Name: "gone", Endpoint: traffic.GET("/items/{id}", traffic.Params{"probe": traffic.Const("sdoprobe")}).Status(404)},
		},
	}
}

func config(s *store, sc traffic.Scenario) Config {
	w := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "probe-write", Purpose: traffic.PurposeHealthProbe,
		RatePerSecond: 0.1, Scenarios: []traffic.WorkloadScenario{{ID: sc.ID}}}.WithDefaults()
	return Config{Catalog: traffic.Catalog{sc}, Workloads: []traffic.Workload{w}, Alter: true, Client: s,
		BaseURL: func(traffic.Target) string { return "http://web:80" },
		Wait:    func(context.Context, time.Duration) error { return nil }}
}

func find(r Report, item string) Result {
	for _, x := range r.Results {
		if x.Item == item {
			return x
		}
	}
	return Result{}
}

func TestAProbeThatReadsBackWhatItWroteIsGreen(t *testing.T) {
	r := Run(context.Background(), config(&store{items: map[string]string{}}, scenario(true)))
	if r.Reds() != 0 || find(r, ItemAltered).Status != Green {
		t.Fatalf("%+v", r.Results)
	}
	if r.Results[0].Item != ItemBudget || r.PlannedRate <= 0 || !strings.Contains(r.Results[0].Detail, "left") {
		t.Fatalf("the budget comes first: %+v", r.Results[0])
	}
}

func TestAReadThatIgnoresItsContentIsReported(t *testing.T) {
	r := Run(context.Background(), config(&store{items: map[string]string{}}, scenario(false)))
	if got := find(r, ItemAltered); !strings.Contains(got.Detail, "GET /items/") {
		t.Fatalf("want the read named: %+v", got)
	}
}

func TestAFailedCleanupIsRed(t *testing.T) {
	r := Run(context.Background(), config(&store{items: map[string]string{}, failDelete: true}, scenario(true)))
	if find(r, ItemCleanup).Status != Red {
		t.Fatalf("%+v", r.Results)
	}
}

func TestAnUnscheduledScenarioAndAnOverBudgetPlanAreRed(t *testing.T) {
	c := config(&store{items: map[string]string{}}, scenario(true))
	c.Workloads = nil
	if r := Run(context.Background(), c); find(r, ItemScheduled).Status != Red {
		t.Fatalf("%+v", r.Results)
	}
	w := traffic.Workload{Name: "w", Purpose: traffic.PurposeHealthProbe}
	for i := range 100 {
		w.Scenarios = append(w.Scenarios, traffic.WorkloadScenario{ID: fmt.Sprint(i)})
	}
	c.Workloads = []traffic.Workload{w}
	if r := Run(context.Background(), c); r.Results[0].Status != Red {
		t.Fatalf("%+v", r.Results[0])
	}
}

func TestUnspentBudgetWithASlowWorkloadIsRed(t *testing.T) {
	c := config(&store{items: map[string]string{}}, scenario(true))
	c.Workloads[0].SampleInterval, c.Workloads[0].RatePerSecond = traffic.Duration(2*time.Minute), 1.0/120
	if r := Run(context.Background(), c); r.Results[0].Status != Red || !strings.Contains(r.Results[0].Detail, "unspent") {
		t.Fatalf("%+v", r.Results[0])
	}
}

// Every scheduled scenario gets its schedule, selected or not: what it costs
// and how soon a full failure of it is noticed, from the workload that
// notices it soonest. A plan run sends nothing and judges nothing.
func TestEveryScheduledScenarioHasItsScheduleAndAPlanSendsNothing(t *testing.T) {
	s := &store{items: map[string]string{}}
	c := config(s, scenario(true))
	c.Workloads[0].SampleInterval, c.Workloads[0].RatePerSecond = traffic.Duration(time.Minute), 1.0/60
	fast := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "probe-fast-write", Purpose: traffic.PurposeHealthProbe,
		SampleInterval: traffic.Duration(10 * time.Second), Scenarios: []traffic.WorkloadScenario{{ID: "write-item"}}}.WithDefaults()
	c.Workloads = append(c.Workloads, fast)
	c.Only = []string{"nothing-matches"}
	r := Run(context.Background(), c)
	if len(r.Schedules) != 1 {
		t.Fatalf("schedules = %+v", r.Schedules)
	}
	got := r.Schedules[0]
	want := fast.ExpectedDetection("write-item", 0)
	if got.Scenario != "write-item" || got.Requests != 2 || len(got.Workloads) != 2 || got.Typical != want.Typical.Seconds() || got.Every != 10 {
		t.Fatalf("schedule = %+v, want the fast workload's %+v", got, want)
	}
	if got.Cost < 0.23 || got.Cost > 0.24 {
		t.Fatalf("cost = %v, want both workloads' requests per second (2/10 + 2/60)", got.Cost)
	}
	c.Only, c.Plan = nil, true
	if r := Run(context.Background(), c); len(s.items) != 0 || s.next != 0 || len(r.Schedules) != 1 || find(r, ItemPasses).Item != "" {
		t.Fatalf("a plan run sent requests or judged: %d writes, %+v", s.next, r.Results)
	}
}

// A cleanup that never reads its data back gone cannot tell a delete that
// only scheduled the removal from one that removed it; one that accepts a
// refusal (400) counts a delete the application refused as done. Both are red
// before anything is sent, so the author fixes them in a plan run.
func TestACleanupThatCannotSeeItsDataStayIsRedInAPlan(t *testing.T) {
	cases := map[string]func(*traffic.Scenario){
		"no read-back": func(sc *traffic.Scenario) { sc.Cleanup = sc.Cleanup[:1] },
		"accepts a refusal": func(sc *traffic.Scenario) {
			sc.Cleanup = []traffic.Step{{Name: "delete", Endpoint: traffic.DELETE("/items/{id}", traffic.Params{"probe": traffic.Const("sdoprobe")}).Status(202, 400, 404)}}
		},
	}
	wants := map[string]string{"no read-back": "reads the data back gone", "accepts a refusal": "accepts 400"}
	for name, edit := range cases {
		sc := scenario(true)
		edit(&sc)
		c := config(&store{items: map[string]string{}}, sc)
		c.Plan = true
		r := Run(context.Background(), c)
		got := find(r, ItemCleanup)
		if got.Status != Red || got.Subject != "write-item" || !r.Red() || !strings.Contains(got.Detail, wants[name]) {
			t.Fatalf("%s: want a red cleanup item in a plan run, got %+v", name, r.Results)
		}
	}
	// A refusal the read-back then checks is fine: "already gone" may be a 400.
	verified := scenario(true)
	verified.Cleanup[0].Endpoint = traffic.DELETE("/items/{id}", traffic.Params{"probe": traffic.Const("sdoprobe")}).Status(202, 204, 400)
	for _, sc := range []traffic.Scenario{scenario(true), verified} {
		c := config(&store{items: map[string]string{}}, sc)
		c.Plan = true
		if r := Run(context.Background(), c); find(r, ItemCleanup).Status == Red {
			t.Fatalf("a cleanup that reads the item gone after its last write is fine: %+v", r.Results)
		}
	}
}

// An application that defers deletion answers the delete with success and
// keeps the object; the cleanup's read-back sees it, so every run reports the
// data it leaves behind instead of piling it up unseen.
func TestDeferredDeletionIsSeenByTheCleanupReadBack(t *testing.T) {
	s := &store{items: map[string]string{}, deferDelete: true}
	r := Run(context.Background(), config(s, scenario(true)))
	if got := find(r, ItemCleanup); got.Status != Red || !strings.Contains(got.Detail, "gone") {
		t.Fatalf("want the read-back step named in a red cleanup, got %+v", r.Results)
	}
}
