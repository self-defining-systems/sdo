package traffic

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// versionedFiles is a synthetic application whose read of a record needs a
// version query (GET /files/{id}?ref=…, 400 without it) and a token, and
// whose DELETE takes write parameters (branch, message): the shape of many
// file and document APIs.
type versionedFiles struct {
	mu   sync.Mutex
	gone bool
	sent []*http.Request
}

func (s *versionedFiles) Do(r *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, r)
	reply := func(status int) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("{}")), Request: r}, nil
	}
	if r.Header.Get("Token") != "secret" {
		return reply(401)
	}
	switch r.Method {
	case http.MethodPost:
		s.gone = false
		return reply(201)
	case http.MethodDelete:
		s.gone = true
		return reply(204)
	}
	if r.URL.Query().Get("ref") == "" {
		return reply(400)
	}
	if s.gone {
		return reply(404)
	}
	return reply(200)
}

func versionedFileScenario() Scenario {
	token := func(e *HTTPEndpoint) *HTTPEndpoint { return e.WithHeader("Token", "secret") }
	return Scenario{
		ID: "files", Target: Target{Service: "web", Port: 80}, Marker: "sdoprobe",
		Steps: []Step{
			{Name: "create", Endpoint: token(POST("/files/sdoprobe-1", Params{"branch": Const("work")})).Status(201)},
			{Name: "read", Endpoint: token(GET("/files/sdoprobe-1", Params{"ref": Const("work")})).Status(200)},
		},
		Cleanup: []Step{{Name: "delete", Endpoint: token(DELETE("/files/sdoprobe-1", Params{"branch": Const("work"), "commit_message": Const("sdoprobe")}))}},
	}
}

// SDO reads a deleted path back the way the scenario reads it: with the read
// step's query and headers, never the DELETE's write parameters. A read-back
// built from the DELETE's query asks a question the application cannot answer
// (400 here), so the probe itself sends failing requests users never send.
func TestTheDeleteReadBackSendsTheScenariosOwnRead(t *testing.T) {
	app := &versionedFiles{}
	_, sample := inferenceEngine(t, versionedFileScenario(), app).Iterate(context.Background(), 1)
	if sample.Outcome != OutcomeOK || sample.CleanupError != "" {
		t.Fatalf("want a clean iteration and read-back: %+v", sample)
	}
	last := app.sent[len(app.sent)-1]
	if last.Method != http.MethodGet || last.URL.Query().Get("ref") != "work" || last.URL.Query().Has("commit_message") || last.Header.Get("Token") != "secret" {
		t.Fatalf("read-back %s %s (token %q): want the read step's GET with its own query and headers", last.Method, last.URL, last.Header.Get("Token"))
	}
	for _, request := range app.sent {
		if request.Method == http.MethodGet && request.URL.Query().Has("branch") {
			t.Fatalf("a GET carried the DELETE's write parameters: %s", request.URL)
		}
	}
}
