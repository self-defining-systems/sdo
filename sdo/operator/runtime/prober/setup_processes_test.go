package prober

import (
	"context"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"

	"sdo.dev/operator/sdk/traffic"
)

// racyStore creates a fixture by check, pause, create, like an application
// without a unique constraint: two concurrent creates both succeed.
type racyStore struct {
	mu      sync.Mutex
	created int
	names   map[string]bool
}

func (s *racyStore) Do(request *http.Request) (*http.Response, error) {
	status, body := http.StatusOK, ""
	if request.Method == http.MethodPost {
		name := request.URL.Query().Get("name")
		s.mu.Lock()
		exists := s.names[name]
		s.mu.Unlock()
		for yield := 0; yield < 2000; yield++ {
			runtime.Gosched()
		}
		if exists {
			status, body = http.StatusConflict, "already exists"
		} else {
			s.mu.Lock()
			s.names[name] = true
			s.created++
			s.mu.Unlock()
			status, body = http.StatusCreated, "created"
		}
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func fixtureScenario(id string) traffic.Scenario {
	return traffic.Scenario{
		ID: id, Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead, Marker: "sdo-fixture",
		Setup: []traffic.Step{{Name: "ensure-fixture", Endpoint: traffic.POST("/items", traffic.Params{"name": traffic.Const("sdo-fixture")}).
			OrExisting(http.StatusConflict, "already exists")}},
		Steps: []traffic.Step{{Name: "read", Endpoint: traffic.GET("/items", nil)}},
	}
}

// Several probe processes (separate probers, as a hot-swapped memory host and
// a restart make) each run the setup of scenarios that share one fixture: the
// fixture is created once.
func TestConcurrentProbeProcessesCreateOneFixture(t *testing.T) {
	store := &racyStore{names: map[string]bool{}}
	locks := &FileSetupLock{Dir: t.TempDir()}
	catalog := traffic.Catalog{fixtureScenario("a"), fixtureScenario("b")}
	var workloads []traffic.Workload
	for _, id := range []string{"a", "b"} {
		workloads = append(workloads, traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "probe-" + id,
			Purpose: traffic.PurposeHealthProbe, Scenarios: []traffic.WorkloadScenario{{ID: id}}})
	}
	var wg sync.WaitGroup
	for process := 0; process < 6; process++ {
		p, err := New(Config{Namespace: "app", Catalog: catalog, Workloads: workloads, Client: store, SetupLock: locks,
			BaseURL: func(traffic.Target) string { return "http://web:80" }})
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"probe-a", "probe-b"} {
			wg.Add(1)
			go func(engine *traffic.Engine) {
				defer wg.Done()
				if _, sample := engine.Iterate(context.Background(), 0); sample.Outcome != traffic.OutcomeOK {
					t.Errorf("iteration: %+v", sample)
				}
			}(p.probes[name].engine)
		}
	}
	wg.Wait()
	if store.created != 1 {
		t.Fatalf("fixture created %d times, want once", store.created)
	}
}
