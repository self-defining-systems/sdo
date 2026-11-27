package traffic

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// loginAPI is a synthetic application: POST /session signs a user in with
// its password (and echoes a rejected password, as some applications do);
// GET /api/repos answers only a request with the session token it issued.
type loginAPI struct {
	mu        sync.Mutex
	passwords map[string]string
	logins    int
	sent      int
}

func (a *loginAPI) Do(request *http.Request) (*http.Response, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sent++
	status, body := http.StatusOK, `[{"name":"repo"}]`
	switch request.URL.Path {
	case "/session":
		a.logins++
		user, password, _ := request.BasicAuth()
		if want, ok := a.passwords[user]; !ok || want != password {
			status, body = http.StatusUnauthorized, "bad password "+password+" for "+user
		} else {
			body = `{"token":"session-` + user + `"}`
		}
	default:
		if request.Header.Get("Private-Token") != "session-sdoprobe-writer" {
			status, body = http.StatusUnauthorized, "no session"
		}
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

type passwords map[string]string

func (p passwords) Password(_ context.Context, user string) (string, error) {
	value, ok := p[user]
	if !ok {
		return "", errors.New("identity pending for " + user)
	}
	return value, nil
}

func userScenario() Scenario {
	return Scenario{
		ID: "call-repos", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectRead, Marker: "sdoprobe",
		Users: []ProbeUser{{Name: "sdoprobe-writer", As: "writer_password"}},
		Setup: []Step{{Name: "sign-in", Endpoint: POST("/session", Params{"login": Const("sdoprobe-writer")}).WithBasicAuth("sdoprobe-writer", "{writer_password}").SaveJSON("token", "token")}},
		Steps: []Step{{Name: "list", Endpoint: GET("/api/repos", nil).WithHeader("PRIVATE-TOKEN", "{token}").Each("", "name", "^repo$")}},
	}
}

const writerPassword = "Synthetic0passwordForTheWriterUser0000aa"

// A scenario that logs in as a probe user gets the user's password in State
// for its setup steps, which create the session the steps use.
func TestProbeUserPasswordSignsSetupIn(t *testing.T) {
	api := &loginAPI{passwords: map[string]string{"sdoprobe-writer": writerPassword}}
	engine := setupEngine(t, userScenario(), api)
	engine.UsePasswords(passwords{"sdoprobe-writer": writerPassword})
	for iteration := uint64(0); iteration < 2; iteration++ {
		if _, sample := engine.Iterate(context.Background(), iteration); sample.Outcome != OutcomeOK {
			t.Fatalf("iteration %d: %+v", iteration, sample)
		}
	}
	if api.logins != 1 {
		t.Fatalf("setup signed in %d times, want once per prober", api.logins)
	}
}

// The password never leaves the request: a failure that echoes it is
// redacted from the sample, and the setup record does not keep it.
func TestProbeUserPasswordNeverReachesASample(t *testing.T) {
	api := &loginAPI{passwords: map[string]string{"sdoprobe-writer": "a-different-password"}}
	engine := setupEngine(t, userScenario(), api)
	engine.UsePasswords(passwords{"sdoprobe-writer": writerPassword})
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeError || sample.Status != http.StatusUnauthorized {
		t.Fatalf("a rejected sign-in is a failed request: %+v", sample)
	}
	text := sample.Error + sample.Request + sample.Step + sample.CleanupError
	for cut := 8; cut <= len(writerPassword); cut++ {
		if strings.Contains(text, writerPassword[:cut]) {
			t.Fatalf("password prefix of %d characters leaked into the sample: %+v", cut, sample)
		}
	}
	direct := userScenario()
	direct.Setup = nil
	direct.SideEffect = SideEffectIdempotentWrite
	direct.Steps = []Step{{Name: "list", Endpoint: GET("/api/repos", Params{"login": Const("sdoprobe-writer")}).WithBasicAuth("sdoprobe-writer", "{writer_password}").Contains("repo")}}
	planned := setupEngine(t, direct, api)
	planned.UsePasswords(passwords{"sdoprobe-writer": writerPassword})
	if requests, err := planned.Plan(context.Background(), 0); err != nil || len(requests) != 1 || requests[0].Headers["Authorization"] != "Basic "+base64.StdEncoding.EncodeToString([]byte("sdoprobe-writer:[password]")) {
		t.Fatalf("plan must use a placeholder, got %+v (%v)", requests, err)
	}
	engine.UsePasswords(passwords{"sdoprobe-writer": writerPassword})
	api.passwords["sdoprobe-writer"] = writerPassword
	if _, sample := engine.Iterate(context.Background(), 1); sample.Outcome != OutcomeOK {
		t.Fatalf("sample = %+v", sample)
	}
	for _, progress := range engine.setups.progress {
		if _, kept := progress.saved["writer_password"]; kept {
			t.Fatal("the setup record kept the password")
		}
	}
}

// A user whose identity is not provisioned yet says nothing about the
// application: nothing is sent and the sample is invalid, naming the user.
func TestPendingProbeUserSendsNothing(t *testing.T) {
	api := &loginAPI{}
	engine := setupEngine(t, userScenario(), api)
	engine.UsePasswords(passwords{})
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeInvalid || !strings.Contains(sample.Error, "sdoprobe-writer") || api.sent != 0 {
		t.Fatalf("sample = %+v, sent %d", sample, api.sent)
	}
	plain := setupEngine(t, userScenario(), api)
	if _, sample := plain.Iterate(context.Background(), 0); sample.Outcome != OutcomeInvalid || api.sent != 0 {
		t.Fatalf("an engine without a password source sent the request: %+v", sample)
	}
}

func TestProbeUserDeclarationsAreValidated(t *testing.T) {
	for name, users := range map[string][]ProbeUser{
		"no marker":       {{Name: "writer", As: "password"}},
		"space":           {{Name: "sdoprobe writer", As: "password"}},
		"state key":       {{Name: "sdoprobe-writer", As: "{password}"}},
		"duplicate state": {{Name: "sdoprobe-a", As: "password"}, {Name: "sdoprobe-b", As: "password"}},
		"duplicate user":  {{Name: "sdoprobe-a", As: "one"}, {Name: "sdoprobe-a", As: "two"}},
	} {
		scenario := userScenario()
		scenario.Users = users
		if err := scenario.Validate(); err == nil {
			t.Errorf("%s: invalid probe user accepted", name)
		}
	}
	unmarked := userScenario()
	unmarked.Marker, unmarked.Setup = "", nil
	unmarked.Steps = []Step{{Name: "list", Endpoint: GET("/api/repos", nil).WithBasicAuth("sdoprobe-writer", "{writer_password}").Contains("repo")}}
	if err := unmarked.Validate(); err == nil {
		t.Error("a scenario with probe users and no marker was accepted")
	}
	if err := userScenario().Validate(); err != nil {
		t.Fatal(err)
	}
}

// The generated conformance test runs every scenario against faulty
// responses; a scenario that logs in is judged by its checks there too.
func TestConformanceChecksAProbeUserScenario(t *testing.T) {
	if failures := CheckCatalog(context.Background(), Catalog{userScenario()}); len(failures) != 0 {
		t.Fatalf("failures: %v", failures)
	}
}

type tokenPasswords struct{ passwords }

func (p tokenPasswords) Token(_ context.Context, _ string) (string, error) {
	return "session-sdoprobe-writer", nil
}

func TestProbeUsesProvisionedTokenWithoutSigningIn(t *testing.T) {
	scenario := userScenario()
	scenario.Setup = nil
	scenario.Users = []ProbeUser{{Name: "sdoprobe-writer", Token: "token"}}
	api := &loginAPI{}
	engine := setupEngine(t, scenario, api)
	engine.UsePasswords(tokenPasswords{})
	plans, err := engine.Plan(context.Background(), 0)
	if err != nil || plans[0].Headers["PRIVATE-TOKEN"] != "[token]" {
		t.Fatalf("plan should use a placeholder: %v (%v)", plans, err)
	}
	for i := range uint64(2) {
		if _, sample := engine.Iterate(context.Background(), i); sample.Outcome != OutcomeOK {
			t.Fatalf("token did not authenticate: %+v", sample)
		}
	}
	if api.logins != 0 || api.sent != 2 {
		t.Fatalf("sent %d requests including %d logins", api.sent, api.logins)
	}

	scenario.Steps = []Step{{Name: "bad", Endpoint: GET("/api/repos/{token}", nil).Contains("repo")}}
	engine = setupEngine(t, scenario, api)
	engine.UsePasswords(tokenPasswords{})
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeError || strings.Contains(sample.Request+sample.Error+sample.Observed, "session-sdoprobe-writer") {
		t.Fatalf("failed sample must redact the token: %+v", sample)
	}
}
