package traffic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"sdo.dev/operator/sdk"
)

// execEndpoint is a generator-written EXEC step: a command with stdin, and a
// check that requires stdout to contain want and the exit status to be 0.
type execEndpoint struct {
	command, stdin, want string
}

func (e execEndpoint) Build(_ context.Context, _ *rand.Rand, state State) (Request, error) {
	command, err := fillState(e.command, state)
	if err != nil {
		return Request{}, err
	}
	stdin, err := fillState(e.stdin, state)
	if err != nil {
		return Request{}, err
	}
	return Request{Method: MethodExec, Path: command, Body: stdin}, nil
}

func (e execEndpoint) Check(response Response, _ State) Verdict {
	if response.Status != 0 {
		return Fail("exit %d", response.Status)
	}
	if !strings.Contains(string(response.Body), e.want) {
		return Fail("stdout lacks %q", e.want)
	}
	return Pass()
}

// keyAPI is a synthetic application whose HTTP API registers a probe user's
// public key for its SSH service.
type keyAPI struct {
	mu   sync.Mutex
	keys []string
}

func (a *keyAPI) Do(request *http.Request) (*http.Response, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	body, _ := io.ReadAll(request.Body)
	a.keys = append(a.keys, string(body))
	return &http.Response{StatusCode: http.StatusCreated, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("{}")), Request: request}, nil
}

// fakeExecer records each exec and answers with result or err.
type fakeExecer struct {
	mu     sync.Mutex
	calls  []Exec
	result ExecResult
	err    error
}

func (f *fakeExecer) Exec(_ context.Context, exec Exec) (ExecResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, exec)
	return f.result, f.err
}

// probeKeys is a synthetic probe-user source with passwords and public keys.
type probeKeys map[string]string

func (k probeKeys) Password(_ context.Context, user string) (string, error) {
	if _, ok := k[user]; !ok {
		return "", errors.New("identity pending for " + user)
	}
	return writerPassword, nil
}

func (k probeKeys) SSHPublicKey(_ context.Context, user string) (string, error) {
	line, ok := k[user]
	if !ok {
		return "", errors.New("identity pending for " + user)
	}
	return line, nil
}

const syntheticPublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAISyntheticPublicKeyForTests"

func sshTarget() *Target { return &Target{Service: "shell", Port: 22, Scheme: "ssh", User: "svc"} }

func sshScenario() Scenario {
	return Scenario{
		ID: "ssh-journey", Target: Target{Service: "web", Port: 80}, DependsOn: []string{"shell"},
		SideEffect: SideEffectIdempotentWrite, Marker: "sdoprobe",
		Users: []ProbeUser{{Name: "sdoprobe-writer", As: "writer_password", SSHKey: "writer_key"}},
		Setup: []Step{{Name: "register-key", Endpoint: POST("/keys", nil).
			WithJSON(`{"title":"sdoprobe","key":"{writer_key}"}`, nil)}},
		Steps: []Step{{Name: "list-refs", Target: sshTarget(), Endpoint: execEndpoint{
			command: "list-refs 'sdoprobe/project'", stdin: "0000", want: "refs/heads/main"}}},
	}
}

func sshEngine(t *testing.T, scenario Scenario, client Doer, execer Execer) *Engine {
	t.Helper()
	engine := setupEngine(t, scenario, client)
	engine.baseURL = func(target Target) string { return target.Scheme + "://" + target.Service + ".app:" + fmt.Sprint(target.Port) }
	engine.UsePasswords(probeKeys{"sdoprobe-writer": syntheticPublicKey})
	if execer != nil {
		engine.UseExec(execer)
	}
	return engine
}

// A journey registers the probe user's public key over HTTP, then runs an
// EXEC step against its own SSH target as that user: the transport gets the
// address, login, probe user, command and stdin; the check sees the exit
// status and stdout.
func TestSSHStepRunsAsTheProbeUserAfterHTTPSetup(t *testing.T) {
	api := &keyAPI{}
	execer := &fakeExecer{result: ExecResult{Exit: 0, Stdout: []byte("0032abc refs/heads/main\n")}}
	engine := sshEngine(t, sshScenario(), api, execer)
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeOK {
		t.Fatalf("sample = %+v", sample)
	}
	if len(api.keys) != 1 || !strings.Contains(api.keys[0], syntheticPublicKey) {
		t.Fatalf("setup registered %q, want the public key line", api.keys)
	}
	want := Exec{Address: "shell.app:22", Login: "svc", ProbeUser: "sdoprobe-writer", Command: "list-refs 'sdoprobe/project'", Stdin: "0000"}
	if len(execer.calls) != 1 || execer.calls[0] != want {
		t.Fatalf("exec = %+v, want %+v", execer.calls, want)
	}
	if sample.Exit == nil || *sample.Exit != 0 || sample.Requests != 2 {
		t.Fatalf("sample exit/requests = %v/%d", sample.Exit, sample.Requests)
	}
}

// A non-zero exit fails the step: the sample names the exit status, the SSH
// target and the remote's stderr, and the window counts it as "exit <n>",
// never as an HTTP status.
func TestSSHExitStatusIsReportedAsExitNotHTTP(t *testing.T) {
	execer := &fakeExecer{result: ExecResult{Exit: 128, Stderr: "fatal: access denied"}}
	engine := sshEngine(t, sshScenario(), &keyAPI{}, execer)
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeError || sample.Step != "list-refs" || sample.Exit == nil || *sample.Exit != 128 {
		t.Fatalf("sample = %+v", sample)
	}
	if !strings.Contains(sample.Error, "exit 128") || !strings.Contains(sample.Error, "stderr: fatal: access denied") {
		t.Fatalf("error = %q", sample.Error)
	}
	if !strings.HasPrefix(sample.Request, "EXEC ssh://svc@shell:22 list-refs") {
		t.Fatalf("request = %q", sample.Request)
	}
	if key := statusKey(sample); key != "exit 128" {
		t.Fatalf("status key = %q", key)
	}
	// Exit 0 with a failed check is not a transport failure either.
	execer.result = ExecResult{Exit: 0, Stdout: []byte("nothing")}
	_, sample = engine.Iterate(context.Background(), 1)
	if sample.Outcome != OutcomeError || statusKey(sample) != "exit 0" || strings.Contains(sample.Error, "stderr") {
		t.Fatalf("exit 0 sample = %+v key %q", sample, statusKey(sample))
	}
	verdict := SLOVerdict{StatusCounts: map[string]int{"exit 128": 2, "500": 1}, RecentFailures: []Sample{sample}}
	finding := ScenarioFinding(sdk.DetectorSpec{ID: "traffic-probe"}, "app", Workload{Name: "probe"}, sshScenario().Describe(), verdict)
	if !strings.Contains(finding.Evidence, "exit 128 ×2") || !strings.Contains(finding.Evidence, "HTTP 500 ×1") ||
		strings.Contains(finding.Evidence, "HTTP exit") || !strings.Contains(finding.Evidence, "→ exit 0:") {
		t.Fatalf("evidence = %s", finding.Evidence)
	}
}

// Transport failures keep today's classes: a refused dial is DialFailed, a
// deadline is a timeout, a session that ends without an exit status is an
// error with no exit.
func TestSSHTransportFailuresAreClassifiedLikeHTTP(t *testing.T) {
	cases := map[string]struct {
		err     error
		outcome Outcome
		dial    bool
	}{
		"refused":   {&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, OutcomeError, true},
		"deadline":  {fmt.Errorf("ssh session: %w", context.DeadlineExceeded), OutcomeTimeout, false},
		"no status": {errors.New("ssh session ended without an exit status"), OutcomeError, false},
	}
	for name, c := range cases {
		engine := sshEngine(t, sshScenario(), &keyAPI{}, &fakeExecer{err: c.err})
		_, sample := engine.Iterate(context.Background(), 0)
		if sample.Outcome != c.outcome || sample.DialFailed != c.dial || sample.Exit != nil || sample.Step != "list-refs" {
			t.Errorf("%s: sample = %+v", name, sample)
		}
	}
}

// A prober without an SSH transport cannot measure an SSH step: the sample
// is invalid, which says nothing about the application.
func TestSSHStepWithoutTransportIsInvalid(t *testing.T) {
	engine := sshEngine(t, sshScenario(), &keyAPI{}, nil)
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeInvalid || sample.Step != "list-refs" {
		t.Fatalf("sample = %+v", sample)
	}
}

// Per-request rules: EXEC only on SSH targets, HTTP methods only on HTTP
// ones, a single-line command, no query or headers, the Marker like any
// write, and never a probe-user password.
func TestSSHRequestSafetyRules(t *testing.T) {
	cases := map[string]Request{
		"http method on ssh":  {Method: http.MethodGet, Path: "/sdoprobe"},
		"empty command":       {Method: MethodExec, Path: ""},
		"multi-line command":  {Method: MethodExec, Path: "list-refs sdoprobe\nrm -rf /"},
		"query":               {Method: MethodExec, Path: "list-refs sdoprobe", Query: map[string]string{"a": "b"}},
		"headers":             {Method: MethodExec, Path: "list-refs sdoprobe", Headers: map[string]string{"a": "b"}},
		"no marker":           {Method: MethodExec, Path: "list-refs project"},
		"password in command": {Method: MethodExec, Path: "list-refs sdoprobe " + writerPassword},
		"password in stdin":   {Method: MethodExec, Path: "list-refs sdoprobe", Body: "pw " + writerPassword},
	}
	for name, request := range cases {
		scenario := sshScenario()
		scenario.Steps[0].Endpoint = fixedEndpoint{request: request}
		execer := &fakeExecer{}
		engine := sshEngine(t, scenario, &keyAPI{}, execer)
		_, sample := engine.Iterate(context.Background(), 0)
		if sample.Outcome != OutcomeInvalid || len(execer.calls) != 0 {
			t.Errorf("%s: sample = %+v, %d execs", name, sample, len(execer.calls))
		}
		if strings.Contains(sample.Error+sample.Request, writerPassword) {
			t.Errorf("%s: password leaked into the sample: %+v", name, sample)
		}
	}
	scenario := sshScenario()
	scenario.Steps[0].Target = nil
	scenario.Steps[0].Endpoint = fixedEndpoint{request: Request{Method: MethodExec, Path: "list-refs sdoprobe"}}
	engine := sshEngine(t, scenario, &keyAPI{}, &fakeExecer{})
	if _, sample := engine.Iterate(context.Background(), 0); sample.Outcome != OutcomeInvalid {
		t.Fatalf("EXEC on an HTTP target must be invalid: %+v", sample)
	}
}

type fixedEndpoint struct{ request Request }

func (f fixedEndpoint) Build(context.Context, *rand.Rand, State) (Request, error) { return f.request, nil }
func (f fixedEndpoint) Check(Response, State) Verdict                           { return Pass() }

func TestSSHScenarioValidation(t *testing.T) {
	cases := map[string]func(*Scenario){
		"ssh target without user": func(s *Scenario) { s.Steps[0].Target.User = "" },
		"user on http target":     func(s *Scenario) { s.Target.User = "svc" },
		"bad user":                func(s *Scenario) { s.Steps[0].Target.User = "svc user" },
		"unknown scheme":          func(s *Scenario) { s.Steps[0].Target.Scheme = "telnet" },
		"step service off path":   func(s *Scenario) { s.DependsOn = nil },
		"bad step port":           func(s *Scenario) { s.Steps[0].Target.Port = 0 },
		"no key user":             func(s *Scenario) { s.Users[0].SSHKey = "" },
		"key equals As":           func(s *Scenario) { s.Users[0].SSHKey = s.Users[0].As },
		"bad key name":            func(s *Scenario) { s.Users[0].SSHKey = "{key}" },
		"two key users": func(s *Scenario) {
			s.Users = append(s.Users, ProbeUser{Name: "sdoprobe-other", As: "other_password", SSHKey: "other_key"})
		},
		"ssh step in read scenario": func(s *Scenario) {
			s.SideEffect = SideEffectRead
			s.Steps = append(s.Steps, Step{Name: "read", Endpoint: GET("/", nil)})
		},
	}
	if err := sshScenario().Validate(); err != nil {
		t.Fatalf("valid scenario rejected: %v", err)
	}
	for name, mutate := range cases {
		scenario := sshScenario()
		target := *scenario.Steps[0].Target
		scenario.Steps[0].Target = &target
		scenario.Users = append([]ProbeUser(nil), scenario.Users...)
		mutate(&scenario)
		if err := scenario.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A read scenario may run an SSH step in its setup.
	read := sshScenario()
	read.SideEffect = SideEffectRead
	read.Setup = append(read.Setup, Step{Name: "ensure-ssh", Target: sshTarget(), Endpoint: execEndpoint{command: "ensure sdoprobe"}})
	read.Steps = []Step{{Name: "read", Endpoint: GET("/", nil)}}
	if err := read.Validate(); err != nil {
		t.Fatalf("SSH setup step in a read scenario rejected: %v", err)
	}
}

// The public key line is State for setup and steps, but the shared setup
// record keeps only what steps saved; planned requests show a placeholder.
func TestSSHPublicKeyIsNotKeptInTheSetupRecord(t *testing.T) {
	engine := sshEngine(t, sshScenario(), &keyAPI{}, &fakeExecer{result: ExecResult{Stdout: []byte("refs/heads/main")}})
	if _, sample := engine.Iterate(context.Background(), 0); sample.Outcome != OutcomeOK {
		t.Fatalf("sample = %+v", sample)
	}
	for key, value := range engine.setups.progress["ssh-journey"].saved {
		if value == syntheticPublicKey || key == "writer_key" {
			t.Fatalf("setup record holds the key under %q", key)
		}
	}
	requests, err := engine.Plan(context.Background(), 0)
	if err != nil || len(requests) != 1 || requests[0].Method != MethodExec {
		t.Fatalf("plan = %+v, %v", requests, err)
	}
}

// Conformance simulates SSH faults, so a generated memory test passes a
// scenario with SSH steps whose checks detect them.
func TestConformanceSimulatesSSHFaults(t *testing.T) {
	if failures := CheckCatalog(context.Background(), Catalog{sshScenario()}); len(failures) > 0 {
		t.Fatalf("failures = %v", failures)
	}
	// Without HTTP setup (which the HTTP fault would fail first), only the
	// SSH step's check can detect the fault.
	lax := sshScenario()
	lax.Setup = nil
	lax.Steps[0].Endpoint = fixedEndpoint{request: Request{Method: MethodExec, Path: "list-refs sdoprobe"}}
	if failures := CheckCatalog(context.Background(), Catalog{lax}); len(failures) == 0 {
		t.Fatal("a check that accepts any exit must fail conformance")
	}
}

// The SDK never links an SSH implementation: the transport and the private
// key live in the prober.
func TestTrafficSDKDoesNotImportSSH(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "golang.org/x/crypto") || strings.Contains(string(data), "crypto/ed25519") {
			t.Errorf("%s imports a key or SSH package", file)
		}
	}
}

