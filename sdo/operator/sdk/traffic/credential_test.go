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

// tokenAPI is a synthetic application whose API answers only a request that
// carries the current token, and echoes a rejected token in its error body
// (as many applications do).
type tokenAPI struct {
	mu    sync.Mutex
	token string
	seen  []string
}

func (a *tokenAPI) Do(request *http.Request) (*http.Response, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	got := request.Header.Get("Private-Token")
	if user, password, ok := request.BasicAuth(); ok {
		got = user + ":" + password
	}
	a.seen = append(a.seen, got)
	status, body := http.StatusOK, `[{"name":"repo"}]`
	if got != a.token {
		status, body = http.StatusUnauthorized, "invalid token "+got
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

type secretValues map[string]string

func (s secretValues) Value(_ context.Context, secret, key string) (string, error) {
	value, ok := s[secret+"/"+key]
	if !ok {
		return "", errors.New("secret key not found")
	}
	return value, nil
}

func credentialScenario() Scenario {
	return Scenario{
		ID: "call-repos", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectRead,
		Credentials: []Credential{{Secret: "app-probe-token", Key: "token", As: "token"}},
		Steps:       []Step{{Name: "list", Endpoint: GET("/api/repos", nil).WithHeader("PRIVATE-TOKEN", "{token}").Each("", "name", "^repo$")}},
	}
}

// A scenario that must authenticate names a Secret key; the engine resolves
// it at run time and the step sends it. Rotation is seen on the next
// iteration.
func TestCredentialFromASecretAuthenticatesTheStep(t *testing.T) {
	api := &tokenAPI{token: "glpat-synthetic-one"}
	values := secretValues{"app-probe-token/token": "glpat-synthetic-one"}
	engine := setupEngine(t, credentialScenario(), api)
	engine.UseCredentials(values)
	if _, sample := engine.Iterate(context.Background(), 0); sample.Outcome != OutcomeOK {
		t.Fatalf("sample = %+v", sample)
	}
	api.token, values["app-probe-token/token"] = "glpat-synthetic-two", "glpat-synthetic-two"
	if _, sample := engine.Iterate(context.Background(), 1); sample.Outcome != OutcomeOK {
		t.Fatalf("rotated credential not used: %+v", sample)
	}
}

// The value never leaves the request: a failure that echoes it is redacted
// from the sample, which travels to detectors, evidence and agents.
func TestCredentialValueNeverReachesASample(t *testing.T) {
	api := &tokenAPI{token: "glpat-expected"}
	engine := setupEngine(t, credentialScenario(), api)
	engine.UseCredentials(secretValues{"app-probe-token/token": "stale-value-123"})
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeError || sample.Status != http.StatusUnauthorized {
		t.Fatalf("a rejected credential is a failed request: %+v", sample)
	}
	if strings.Contains(sample.Error+sample.Request+sample.Step, "stale-value-123") {
		t.Fatalf("credential value leaked into the sample: %+v", sample)
	}
}

// A credential the prober cannot read is not a health signal about the
// application: nothing is sent and the sample is invalid, naming the key.
func TestUnreadableCredentialSendsNothing(t *testing.T) {
	api := &tokenAPI{token: "x"}
	engine := setupEngine(t, credentialScenario(), api)
	engine.UseCredentials(secretValues{})
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeInvalid || !strings.Contains(sample.Error, "app-probe-token") || len(api.seen) != 0 {
		t.Fatalf("sample = %+v, sent %d", sample, len(api.seen))
	}
	plain := setupEngine(t, credentialScenario(), api)
	if _, sample := plain.Iterate(context.Background(), 0); sample.Outcome != OutcomeInvalid || len(api.seen) != 0 {
		t.Fatalf("an engine without a credential source sent the request: %+v", sample)
	}
}

func TestBasicAuthFillsUserAndPasswordFromState(t *testing.T) {
	api := &tokenAPI{token: "root:pw-synthetic"}
	scenario := credentialScenario()
	scenario.Credentials = []Credential{{Secret: "app-root", Key: "password", As: "password"}}
	scenario.Steps = []Step{{Name: "refs", Endpoint: GET("/group/repo.git/info/refs", Params{"service": Const("git-upload-pack")}).WithBasicAuth("root", "{password}").Contains("repo")}}
	engine := setupEngine(t, scenario, api)
	engine.UseCredentials(secretValues{"app-root/password": "pw-synthetic"})
	if _, sample := engine.Iterate(context.Background(), 0); sample.Outcome != OutcomeOK {
		t.Fatalf("sample = %+v", sample)
	}
	request, err := scenario.Steps[0].Endpoint.Build(context.Background(), nil, State{"password": "pw"})
	if err != nil || request.Headers["Authorization"] != "Basic "+base64.StdEncoding.EncodeToString([]byte("root:pw")) {
		t.Fatalf("request = %+v (%v)", request, err)
	}
}

func TestCredentialDeclarationsAreValidated(t *testing.T) {
	for name, credentials := range map[string][]Credential{
		"secret name":     {{Secret: "Bad_Name", Key: "token", As: "token"}},
		"key":             {{Secret: "app", Key: "a/b", As: "token"}},
		"state key":       {{Secret: "app", Key: "token", As: "{token}"}},
		"duplicate state": {{Secret: "app", Key: "a", As: "token"}, {Secret: "app", Key: "b", As: "token"}},
	} {
		scenario := credentialScenario()
		scenario.Credentials = credentials
		if err := scenario.Validate(); err == nil {
			t.Errorf("%s: invalid credential accepted", name)
		}
	}
	if err := credentialScenario().Validate(); err != nil {
		t.Fatal(err)
	}
}

// The generated conformance test runs every scenario against faulty
// responses; a scenario with credentials is judged by its checks there too.
func TestConformanceChecksACredentialScenario(t *testing.T) {
	if failures := CheckCatalog(context.Background(), Catalog{credentialScenario()}); len(failures) != 0 {
		t.Fatalf("failures: %v", failures)
	}
}
