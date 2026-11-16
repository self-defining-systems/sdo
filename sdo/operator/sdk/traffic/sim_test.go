package traffic

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// simClock is simulated time: After advances it by the wait and fires at
// once, so a test runs a minute of polling without sleeping.
type simClock struct {
	mu  sync.Mutex
	now time.Time
}

func newSimClock() *simClock { return &simClock{now: time.Date(2026, 11, 16, 9, 30, 0, 0, time.UTC)} }

func (c *simClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *simClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	c.mu.Unlock()
	fired := make(chan time.Time, 1)
	fired <- now
	return fired
}

func (c *simClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// simServer is a fake application on the simulated clock: each request takes
// 50 ms, and handle answers it given the current time.
type simServer struct {
	clock  *simClock
	mu     sync.Mutex
	handle func(now time.Time, request *http.Request) (int, string)
	log    []string
}

func (s *simServer) Do(request *http.Request) (*http.Response, error) {
	s.clock.advance(50 * time.Millisecond)
	status, body := s.handle(s.clock.Now(), request)
	s.mu.Lock()
	s.log = append(s.log, request.Method+" "+request.URL.RequestURI())
	s.mu.Unlock()
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func (s *simServer) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.log...)
}

func simEngine(t *testing.T, workload Workload, catalog Catalog, server *simServer) *Engine {
	t.Helper()
	engine, err := NewEngine(workload, catalog, server, func(Target) string { return "http://web:80" }, server.clock)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func probeWorkload(ids ...string) Workload {
	workload := Workload{APIVersion: APIVersion, Kind: WorkloadKind, Name: "probe-sim", Purpose: PurposeHealthProbe}
	for _, id := range ids {
		workload.Scenarios = append(workload.Scenarios, WorkloadScenario{ID: id})
	}
	return workload
}
