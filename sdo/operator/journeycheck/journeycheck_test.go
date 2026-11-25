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
		Cleanup: []traffic.Step{{Name: "delete", Endpoint: traffic.DELETE("/items/{id}", traffic.Params{"probe": traffic.Const("sdoprobe")}).Status(204)}},
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
