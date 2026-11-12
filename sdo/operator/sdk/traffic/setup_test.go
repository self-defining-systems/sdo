package traffic

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// itemStore is a synthetic application: POST /items creates the marked item
// (409 "already exists" when present), GET /items lists the stored items.
type itemStore struct {
	mu        sync.Mutex
	items     map[string]bool
	posts     int
	failPosts int
}

func (s *itemStore) Do(request *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status, body := http.StatusOK, ""
	switch request.Method {
	case http.MethodPost:
		s.posts++
		name := request.URL.Query().Get("name")
		switch {
		case s.failPosts > 0:
			s.failPosts--
			status, body = http.StatusServiceUnavailable, "store unavailable"
		case s.items[name]:
			status, body = http.StatusConflict, "item already exists"
		default:
			s.items[name] = true
			status, body = http.StatusCreated, "created"
		}
	default:
		for name := range s.items {
			body += name + "\n"
		}
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func (s *itemStore) wipe() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = map[string]bool{}
}

// laterScenario reads a fixture its own setup step writes once.
func laterScenario() Scenario {
	return Scenario{
		ID: "items-later", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectRead, Marker: "sdo-later",
		Setup: []Step{{Name: "ensure-fixture", Endpoint: POST("/items", Params{"name": Const("sdo-later-fixture")}).
			OrExisting(http.StatusConflict, "already exists")}},
		Steps: []Step{{Name: "read-fixture", Endpoint: GET("/items", nil).Contains("sdo-later-fixture")}},
	}
}

func setupEngine(t *testing.T, scenario Scenario, client Doer) *Engine {
	t.Helper()
	workload := Workload{
		APIVersion: APIVersion, Kind: WorkloadKind, Name: "probe-read-later", Purpose: PurposeHealthProbe,
		Scenarios: []WorkloadScenario{{ID: scenario.ID}},
	}
	engine, err := NewEngine(workload, Catalog{scenario}, client, func(Target) string { return "http://web:80" }, nil)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

// A read-later probe on a freshly deployed application creates its own
// fixture once, then only reads: data lost afterwards fails every iteration
// instead of being written again.
func TestSetupRunsOncePerEngineUntilItSucceeds(t *testing.T) {
	store := &itemStore{items: map[string]bool{}, failPosts: 1}
	engine := setupEngine(t, laterScenario(), store)
	ctx := context.Background()

	_, sample := engine.Iterate(ctx, 0)
	if sample.Outcome != OutcomeError || sample.Step != "ensure-fixture" {
		t.Fatalf("a failed setup must fail the iteration at the setup step: %+v", sample)
	}
	_, sample = engine.Iterate(ctx, 1)
	if sample.Outcome != OutcomeOK || sample.Requests != 2 {
		t.Fatalf("setup then read on a fresh application: %+v", sample)
	}
	_, sample = engine.Iterate(ctx, 2)
	if sample.Outcome != OutcomeOK || sample.Requests != 1 || store.posts != 2 {
		t.Fatalf("setup ran again after it succeeded: %+v, posts %d", sample, store.posts)
	}

	store.wipe()
	for iteration := uint64(3); iteration < 6; iteration++ {
		if _, sample = engine.Iterate(ctx, iteration); sample.Outcome != OutcomeError || sample.Step != "read-fixture" {
			t.Fatalf("iteration %d after the fixture was lost: %+v", iteration, sample)
		}
	}
	if store.posts != 2 {
		t.Fatalf("setup re-created lost data and hid the loss: %d posts", store.posts)
	}

	// A new engine (a restarted prober) ensures the fixture again; it already exists.
	store.items["sdo-later-fixture"] = true
	if _, sample = setupEngine(t, laterScenario(), store).Iterate(ctx, 0); sample.Outcome != OutcomeOK {
		t.Fatalf("an existing fixture must satisfy the ensure step: %+v", sample)
	}
}

func TestSetupTimeIsNotScenarioLatency(t *testing.T) {
	clock := &steppingClock{now: time.Unix(0, 0), step: 100 * time.Millisecond}
	store := &itemStore{items: map[string]bool{}}
	workload := Workload{
		APIVersion: APIVersion, Kind: WorkloadKind, Name: "probe-read-later", Purpose: PurposeHealthProbe,
		Scenarios: []WorkloadScenario{{ID: "items-later"}},
	}
	engine, err := NewEngine(workload, Catalog{laterScenario()}, clockedDoer{store, clock}, func(Target) string { return "http://web:80" }, clock)
	if err != nil {
		t.Fatal(err)
	}
	_, first := engine.Iterate(context.Background(), 0)
	_, second := engine.Iterate(context.Background(), 1)
	if first.Outcome != OutcomeOK || first.Latency != second.Latency {
		t.Fatalf("latency with setup %v, without %v: setup must not count", first.Latency, second.Latency)
	}
}

type steppingClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

func (c *steppingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After never fires: Iterate measures time but never waits on the clock.
func (c *steppingClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

func (c *steppingClock) advance() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(c.step)
}

// clockedDoer advances the clock by one step per request.
type clockedDoer struct {
	doer  Doer
	clock *steppingClock
}

func (d clockedDoer) Do(request *http.Request) (*http.Response, error) {
	d.clock.advance()
	return d.doer.Do(request)
}

func TestOrExistingAcceptsOnlyTheAlreadyExistsAnswer(t *testing.T) {
	endpoint := POST("/users", nil).Contains("created").OrExisting(http.StatusConflict, "already exists")
	for _, tc := range []struct {
		status int
		body   string
		ok     bool
	}{
		{http.StatusCreated, "created", true},
		{http.StatusConflict, "user already exists", true},
		{http.StatusConflict, "conflict", false},
		{http.StatusInternalServerError, "already exists", false},
		{http.StatusCreated, "nothing", false},
	} {
		if got := endpoint.Check(Response{Status: tc.status, Body: []byte(tc.body)}, State{}); got.OK != tc.ok {
			t.Errorf("HTTP %d %q: ok=%v, want %v (%s)", tc.status, tc.body, got.OK, tc.ok, got.Reason)
		}
	}
}

func TestSetupSafetyRules(t *testing.T) {
	ensure := Step{Name: "ensure", Endpoint: POST("/items", Params{"name": Const("sdo-later-x")})}
	valid := laterScenario()
	if err := valid.Validate(); err != nil {
		t.Fatalf("a read scenario with marked setup was rejected: %v", err)
	}
	for name, edit := range map[string]func(*Scenario){
		"setup without a marker":         func(s *Scenario) { s.Marker = "" },
		"marker on a read without setup": func(s *Scenario) { s.Setup = nil },
		"too many setup steps":           func(s *Scenario) { s.Setup = make([]Step, MaxSteps+1) },
		"setup step without an endpoint": func(s *Scenario) { s.Setup = []Step{{Name: "ensure"}} },
	} {
		scenario := laterScenario()
		edit(&scenario)
		if err := scenario.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	unmarked := laterScenario()
	unmarked.Setup = []Step{{Name: "ensure", Endpoint: POST("/items", Params{"name": Const("other")})}}
	if _, sample := setupEngine(t, unmarked, &itemStore{items: map[string]bool{}}).Iterate(context.Background(), 0); sample.Outcome != OutcomeInvalid {
		t.Fatalf("a setup write without the marker was sent: %+v", sample)
	}
	write := Scenario{ID: "write-item", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectIdempotentWrite,
		Marker: "sdo-later", Setup: []Step{ensure}, Steps: []Step{ensure}}
	if err := write.Validate(); err != nil {
		t.Fatalf("a write scenario with setup was rejected: %v", err)
	}
	if got := valid.Describe().Setup; len(got) != 1 || got[0] != "ensure-fixture" {
		t.Fatalf("descriptor setup steps = %v", got)
	}
}

// tokenStore is a synthetic application whose POST /tokens creates a new
// token on every call (no duplicate answer an ensure step could accept), and
// whose POST /items fails while failPosts is positive.
type tokenStore struct {
	itemStore
	tokens    int
	passwords []string
}

func (s *tokenStore) Do(request *http.Request) (*http.Response, error) {
	if request.Method != http.MethodPost || request.URL.Path != "/tokens" {
		return s.itemStore.Do(request)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens++
	s.passwords = append(s.passwords, request.URL.Query().Get("password"))
	body := `{"token":"tok-` + strconv.Itoa(s.tokens) + `"}`
	return &http.Response{StatusCode: http.StatusCreated, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

// tokenScenario creates a credential, then ensures a fixture with it.
func tokenScenario() Scenario {
	return Scenario{
		ID: "items-token", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectRead, Marker: "sdo-later",
		Users: []ProbeUser{{Name: "sdo-later-user", As: "user_password"}},
		Setup: []Step{
			{Name: "create-token", Endpoint: POST("/tokens", Params{"name": Const("sdo-later-token"), "password": FromPrevious("user_password")}).
				Status(http.StatusCreated).SaveJSON("token", "token")},
			{Name: "ensure-fixture", Endpoint: POST("/items", Params{"name": Const("sdo-later-fixture")}).
				WithHeader("Authorization", "Bearer {token}").OrExisting(http.StatusConflict, "already exists")},
		},
		Steps: []Step{{Name: "read-fixture", Endpoint: GET("/items", nil).WithHeader("Authorization", "Bearer {token}").Contains("sdo-later-fixture")}},
	}
}

// A setup step that succeeded is not repeated when a later setup step fails:
// a retry resumes at the failed step with the State saved so far, so a
// credential setup creates does not pile up while the application is unstable.
func TestSetupRetryResumesAfterTheLastStepThatSucceeded(t *testing.T) {
	store := &tokenStore{itemStore: itemStore{items: map[string]bool{}, failPosts: 3}}
	engine := setupEngine(t, tokenScenario(), store)
	engine.UsePasswords(passwords{"sdo-later-user": "pw-1"})
	ctx := context.Background()

	for iteration := uint64(0); iteration < 3; iteration++ {
		if _, sample := engine.Iterate(ctx, iteration); sample.Outcome != OutcomeError || sample.Step != "ensure-fixture" {
			t.Fatalf("iteration %d must fail at the failing setup step: %+v", iteration, sample)
		}
	}
	_, sample := engine.Iterate(ctx, 3)
	if sample.Outcome != OutcomeOK || sample.Requests != 2 {
		t.Fatalf("resumed setup (one request) then the read: %+v", sample)
	}
	if store.tokens != 1 {
		t.Fatalf("setup created %d tokens across retries of a later step, want 1", store.tokens)
	}
	if _, sample = engine.Iterate(ctx, 4); sample.Outcome != OutcomeOK || sample.Requests != 1 || store.tokens != 1 {
		t.Fatalf("setup ran again after it completed: %+v, tokens %d", sample, store.tokens)
	}
	if len(store.passwords) != 1 || store.passwords[0] != "pw-1" {
		t.Fatalf("the token step did not see the probe user's password: %q", store.passwords)
	}
}

// A resumed setup step sees the current probe-user passwords, as the first
// attempt did.
func TestResumedSetupStepSeesProbeUserPasswords(t *testing.T) {
	scenario := tokenScenario()
	scenario.Setup[1].Endpoint = POST("/items", Params{"name": Const("sdo-later-fixture"), "password": FromPrevious("user_password")}).
		WithHeader("Authorization", "Bearer {token}").OrExisting(http.StatusConflict, "already exists")
	store := &tokenStore{itemStore: itemStore{items: map[string]bool{}, failPosts: 1}}
	engine := setupEngine(t, scenario, store)
	engine.UsePasswords(passwords{"sdo-later-user": "pw-1"})
	ctx := context.Background()
	if _, sample := engine.Iterate(ctx, 0); sample.Step != "ensure-fixture" {
		t.Fatalf("first attempt must fail at ensure-fixture: %+v", sample)
	}
	if _, kept := engine.setups.progress[scenario.ID].saved["user_password"]; kept {
		t.Fatal("the record of a partial setup kept the password")
	}
	if _, sample := engine.Iterate(ctx, 1); sample.Outcome != OutcomeOK {
		t.Fatalf("a resumed step lost the probe user's password: %+v", sample)
	}
}
