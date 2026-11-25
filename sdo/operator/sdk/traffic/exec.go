package traffic

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

// MaxExecStderr bounds the standard error of an EXEC kept for a failure
// reason.
const MaxExecStderr = 512

// Exec is one command the engine asks the prober's SSH transport to run.
// ProbeUser names the probe user whose key signs the session; the engine
// never holds the private key.
type Exec struct {
	// Address is host:port of the target Service.
	Address string
	// Login is the target's SSH User.
	Login     string
	ProbeUser string
	Command   string
	Stdin     string
}

// ExecResult is a finished command: its exit status, standard output
// (at most MaxResponseBody bytes) and standard error (at most
// MaxExecStderr bytes).
type ExecResult struct {
	Exit   int
	Stdout []byte
	Stderr string
}

// Execer runs EXEC requests over SSH; the prober supplies it, like the HTTP
// Doer. A failure to connect returns the dial error (a *net.OpError with Op
// "dial"), a deadline returns an error wrapping the context's error, and a
// session that ends without an exit status is an error.
type Execer interface {
	Exec(ctx context.Context, exec Exec) (ExecResult, error)
}

// UseExec lets scenarios run EXEC steps on ssh targets through execer. An
// engine without one records such steps as invalid.
func (e *Engine) UseExec(execer Execer) { e.execer = execer }

// execStep sends an EXEC request and judges its result.
func (e *Engine) execStep(ctx context.Context, scenario Scenario, step Step, target Target, request Request, state State, line string) stepResult {
	if e.execer == nil {
		return stepResult{outcome: OutcomeInvalid, request: line, err: "this prober has no SSH transport, so it cannot run ssh steps"}
	}
	user, _ := scenario.keyUser()
	address, err := hostPort(e.baseURL(target))
	if err != nil {
		return stepResult{outcome: OutcomeInvalid, request: line, err: err.Error()}
	}
	requestCtx, cancel := e.withTimeout(ctx, e.workload.Timeout.Duration())
	defer cancel()
	result, err := e.execer.Exec(requestCtx, Exec{Address: address, Login: target.User, ProbeUser: user.Name, Command: request.Path, Stdin: request.Body})
	if err != nil {
		outcome := OutcomeError
		if isTimeout(requestCtx, err) {
			outcome = OutcomeTimeout
		}
		return stepResult{outcome: outcome, sent: 1, request: line, err: err.Error(), dial: isDialError(err)}
	}
	exit := result.Exit
	stdout := result.Stdout
	if len(stdout) > MaxResponseBody {
		stdout = stdout[:MaxResponseBody]
	}
	verdict := step.Endpoint.Check(Response{Status: exit, Body: stdout}, state)
	if !verdict.OK {
		reason := verdict.Reason
		if strings.TrimSpace(reason) == "" {
			reason = fmt.Sprintf("exit %d rejected by check", exit)
		}
		if stderr := strings.TrimSpace(result.Stderr); stderr != "" {
			if len(stderr) > MaxExecStderr {
				stderr = stderr[:MaxExecStderr]
			}
			reason += "; stderr: " + stderr
		}
		return stepResult{outcome: OutcomeError, exit: &exit, sent: 1, request: line, err: reason}
	}
	return stepResult{outcome: OutcomeOK, exit: &exit, sent: 1, request: line}
}

func hostPort(base string) (string, error) {
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" {
		return "", fmt.Errorf("target address %q is not scheme://host:port", base)
	}
	return parsed.Host, nil
}

// carriesPassword reports whether request sends a probe-user password; an
// EXEC never may, since nothing on the remote side redacts it.
func carriesPassword(request Request, passwords State) bool {
	for _, password := range passwords {
		if password != "" && (strings.Contains(request.Path, password) || strings.Contains(request.Body, password)) {
			return true
		}
	}
	return false
}
