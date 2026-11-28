package traffic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// recordStore is a synthetic application: POST /records creates a record and
// answers its server-chosen id; GET /records/{id} reads it (404 once gone);
// DELETE /records/{id} removes it unless keep is set (a delete it only
// pretends to do). POST /search and POST /session answer without writing.
type recordStore struct {
	mu      sync.Mutex
	records map[string]string
	next    int
	keep    bool
	sent    []string
}

func (s *recordStore) Do(r *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, r.Method+" "+r.URL.Path)
	reply := func(status int, body string) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	}
	id := strings.TrimPrefix(r.URL.Path, "/records/")
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/records":
		raw, _ := io.ReadAll(r.Body)
		s.next++
		key := fmt.Sprint(100 + s.next)
		s.records[key] = string(raw)
		body, _ := json.Marshal(map[string]string{"id": key})
		return reply(201, string(body))
	case r.Method == http.MethodPost:
		return reply(200, `{"results":[]}`)
	case r.Method == http.MethodDelete:
		if !s.keep {
			delete(s.records, id)
		}
		return reply(204, "")
	}
	if body, ok := s.records[id]; ok {
		return reply(200, body)
	}
	return reply(404, "missing")
}

func inferenceEngine(t *testing.T, s Scenario, client Doer) *Engine {
	t.Helper()
	completed := CompleteCatalog(Catalog{s}, nil)
	workload := Workload{APIVersion: APIVersion, Kind: WorkloadKind, Name: "probe-write", Purpose: PurposeHealthProbe,
		Scenarios: []WorkloadScenario{{ID: s.ID}}}
	engine, err := NewEngine(workload, completed, client, func(Target) string { return "http://web:80" }, nil)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func createReadDelete() Scenario {
	return Scenario{
		ID: "records", Target: Target{Service: "web", Port: 80}, Marker: "sdoprobe",
		Steps: []Step{
			{Name: "create", Endpoint: POST("/records", nil).WithJSON(`{"name":"{name}"}`, Params{"name": Unique("sdoprobe-")}).Status(201).SaveJSON("id", "id")},
			{Name: "read", Endpoint: GET("/records/{id}", nil).Contains("sdoprobe-")},
		},
		Cleanup: []Step{{Name: "delete", Endpoint: DELETE("/records/{id}", nil).Status(204)}},
	}
}

// The side effect follows from the steps' methods: GET/HEAD only is a read,
// a write with Cleanup is write-with-cleanup. A write without Cleanup and a
// custom endpoint still need a declaration, and the error says which.
func TestSideEffectIsInferredFromTheStepsMethods(t *testing.T) {
	read := Scenario{ID: "list", Target: Target{Service: "web", Port: 80},
		Steps: []Step{{Name: "list", Endpoint: Eventually(GET("/records", nil).Contains("x"))}, {Name: "search", Endpoint: POST("/search", nil).Reads().Contains("results")}}}
	write := createReadDelete()
	noCleanup := createReadDelete()
	noCleanup.Cleanup = nil
	custom := Scenario{ID: "custom", Target: Target{Service: "web", Port: 80}, Steps: []Step{{Name: "x", Endpoint: customEndpoint{}}}}
	// A wrapper that embeds the declarative endpoint to refine Check keeps its method visible.
	write.Steps[1].Endpoint = wrapped{GET("/records/{id}", nil)}
	got := CompleteCatalog(Catalog{read, write, noCleanup, custom}, nil)
	if got[0].SideEffect != SideEffectRead || got[1].SideEffect != SideEffectWriteWithCleanup {
		t.Fatalf("inferred %q and %q", got[0].SideEffect, got[1].SideEffect)
	}
	if got[0].Validate() != nil || got[1].Validate() != nil {
		t.Fatalf("inferred scenarios validate: %v, %v", got[0].Validate(), got[1].Validate())
	}
	if err := got[2].Validate(); err == nil || !strings.Contains(err.Error(), "SideEffectIdempotentWrite") || !strings.Contains(err.Error(), "Cleanup") {
		t.Fatalf("a write without cleanup must name both fixes: %v", err)
	}
	if err := got[3].Validate(); err == nil || !strings.Contains(err.Error(), "step x") || !strings.Contains(err.Error(), "SideEffect") {
		t.Fatalf("a custom endpoint must name the step and the field: %v", err)
	}
	declared := createReadDelete()
	declared.SideEffect = SideEffectIdempotentWrite
	declared.Cleanup = nil
	if CompleteCatalog(Catalog{declared}, nil)[0].SideEffect != SideEffectIdempotentWrite {
		t.Fatal("a declared side effect is kept")
	}
}

// A sign-in or search POST marked Reads runs in a read scenario's steps.
func TestAReadsPostRunsInAReadScenario(t *testing.T) {
	s := Scenario{ID: "search", Target: Target{Service: "web", Port: 80},
		Steps: []Step{{Name: "search", Endpoint: POST("/search", nil).Reads().Contains("results")}}}
	store := &recordStore{records: map[string]string{}}
	_, sample := inferenceEngine(t, s, store).Iterate(context.Background(), 1)
	if sample.Outcome != OutcomeOK {
		t.Fatalf("%+v", sample)
	}
}

// A marker on a scenario that writes nothing is unused, not an error.
func TestAnUnusedMarkerIsAllowed(t *testing.T) {
	s := Scenario{ID: "list", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectRead, Marker: "sdoprobe",
		Steps: []Step{{Name: "list", Endpoint: GET("/records", nil).Contains("x")}}}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
}

// A request that addresses a record by the id a marked create saved is the
// probe's own data: it needs no extra marker, in steps or cleanup. An id the
// probe did not create never marks a request.
func TestARequestNamingAnIDItsOwnMarkedWriteSavedIsMarked(t *testing.T) {
	store := &recordStore{records: map[string]string{}}
	_, sample := inferenceEngine(t, createReadDelete(), store).Iterate(context.Background(), 1)
	if sample.Outcome != OutcomeOK || sample.CleanupError != "" {
		t.Fatalf("%+v", sample)
	}
	if len(store.records) != 0 {
		t.Fatalf("records left: %v", store.records)
	}

	foreign := Scenario{ID: "foreign", Target: Target{Service: "web", Port: 80}, Marker: "sdoprobe", SideEffect: SideEffectWriteWithCleanup,
		Steps: []Step{
			{Name: "find", Endpoint: GET("/records/101", nil).SaveJSON("name", "name").Status(200)},
			{Name: "edit", Endpoint: PUT("/records/{name}", nil).Status(200)},
		},
		Cleanup: []Step{{Name: "gone", Endpoint: GET("/records/{name}", nil).Status(404)}}}
	store = &recordStore{records: map[string]string{"101": `{"name":"103"}`}}
	_, sample = inferenceEngine(t, foreign, store).Iterate(context.Background(), 1)
	if sample.Outcome != OutcomeInvalid || !strings.Contains(sample.Error, "marker") {
		t.Fatalf("a value read from users' data must not mark a write: %+v", sample)
	}
}

// After a cleanup DELETE of a path the scenario read, SDO reads it back and
// expects it gone: a delete the application only pretends to do fails
// cleanup, and the scenario needs no read-back step of its own.
func TestSDOReadsADeletedRecordBack(t *testing.T) {
	store := &recordStore{records: map[string]string{}}
	_, sample := inferenceEngine(t, createReadDelete(), store).Iterate(context.Background(), 1)
	if sample.CleanupError != "" || store.sent[len(store.sent)-1] != "GET /records/101" {
		t.Fatalf("want the read-back last: %v %+v", store.sent, sample)
	}
	store = &recordStore{records: map[string]string{}, keep: true}
	_, sample = inferenceEngine(t, createReadDelete(), store).Iterate(context.Background(), 1)
	if !strings.Contains(sample.CleanupError, "still") {
		t.Fatalf("a kept record fails cleanup: %+v", sample)
	}
	if _, ok := createReadDelete().DeleteReadBack(); !ok {
		t.Fatal("the static check sees the read-back")
	}
	unread := createReadDelete()
	unread.Steps = unread.Steps[:1]
	if _, ok := unread.DeleteReadBack(); ok {
		t.Fatal("a path the scenario never read may have no GET route: no automatic read-back")
	}
}

type wrapped struct{ *HTTPEndpoint }

type customEndpoint struct{}

func (customEndpoint) Build(context.Context, *rand.Rand, State) (Request, error) {
	return Request{Method: http.MethodGet, Path: "/x"}, nil
}
func (customEndpoint) Check(Response, State) Verdict { return Pass() }

// DependsOn follows from the map's probe paths: one Service per component, the
// one a step calls when the component has several; a declared one is kept.
func TestDependsOnIsDerivedFromTheMapPaths(t *testing.T) {
	s := createReadDelete()
	s.Steps = append(s.Steps, Step{Name: "admin", Endpoint: GET("/records", nil).Contains("x"), Target: &Target{Service: "web-admin", Port: 81}})
	paths := map[string][][]string{"records": {{"web", "web-admin"}, {"db", "db-headless"}}}
	if got := CompleteCatalog(Catalog{s}, paths)[0].DependsOn; strings.Join(got, ",") != "web,db" {
		t.Fatalf("derived %v", got)
	}
	s.Target.Service = "web-admin"
	if got := CompleteCatalog(Catalog{s}, paths)[0].DependsOn; strings.Join(got, ",") != "web-admin,db" {
		t.Fatalf("the called Service of a component wins: %v", got)
	}
	s.DependsOn = []string{"web"}
	if got := CompleteCatalog(Catalog{s}, paths)[0].DependsOn; strings.Join(got, ",") != "web" {
		t.Fatalf("a declared DependsOn is kept: %v", got)
	}
}

// A read in Cleanup (here a search that shows a restore took) writes nothing,
// so it needs no marker.
func TestACleanupReadNeedsNoMarker(t *testing.T) {
	s := createReadDelete()
	s.Cleanup = []Step{
		{Name: "delete", Endpoint: DELETE("/records/{id}", nil).Status(204)},
		{Name: "search", Endpoint: GET("/records", nil).Status(404)},
	}
	store := &recordStore{records: map[string]string{}}
	if _, sample := inferenceEngine(t, s, store).Iterate(context.Background(), 1); sample.CleanupError != "" {
		t.Fatalf("%+v", sample)
	}
}
