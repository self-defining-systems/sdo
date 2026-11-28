package runtime

import (
	"context"
	"errors"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// codedExecClient answers every observation with one exit code and records
// whether an Ensure ran.
type codedExecClient struct {
	slowExecClient
	observe int32
}

func (c *codedExecClient) RunOperation(_ context.Context, op program.ExternalOperation) (int32, error) {
	args := []string{}
	if op.Exec != nil {
		args = op.Exec.Args
	} else {
		args = []string{op.HTTP.Method}
	}
	c.calls = append(c.calls, args[0])
	if args[0] == "ensure" || args[0] == "PUT" {
		return 0, nil
	}
	return c.observe, nil
}

func httpEnsureAction() program.EnsureHTTP {
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "toolbox"}
	return program.EnsureHTTP{
		Observe: program.HTTPRequest{Pod: pod, Container: "c", Method: "GET", URL: "http://127.0.0.1:8080/state"},
		Ensure:  program.HTTPRequest{Pod: pod, Container: "c", Method: "PUT", URL: "http://127.0.0.1:8080/state"},
		Reason:  "synthetic",
	}
}

// A read-only check that failed (an uncaught script error, a missing command,
// an unreachable server) is no evidence a write is needed: the executor runs
// no Ensure, writes nothing, and fails the action with the check's exit code.
// Only the needs-change answer leads to Ensure.
func TestEnsureRunsOnlyWhenItsCheckSaysTheStateNeedsTheChange(t *testing.T) {
	for _, c := range []struct {
		name       string
		action     program.Action
		observe    int32
		wantEnsure bool
	}{
		{"exec needs change", slowExecAction(), 1, true},
		{"exec script error", slowExecAction(), 252, false},
		{"exec missing command", slowExecAction(), 127, false},
		{"http error status", httpEnsureAction(), program.CurlHTTPErrorExit, true},
		{"http unreachable", httpEnsureAction(), 7, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			client := &codedExecClient{observe: c.observe}
			r, err := (PlanExecutor{Client: client}).externalStep(context.Background(), c.action, false)
			ensured := len(client.calls) > 1
			if ensured != c.wantEnsure {
				t.Fatalf("calls %v: ensure ran %v, want %v", client.calls, ensured, c.wantEnsure)
			}
			if c.wantEnsure {
				if err != nil {
					t.Fatalf("err %v", err)
				}
				return
			}
			var failed *ExternalCheckFailedError
			if !errors.As(err, &failed) || failed.Code != c.observe {
				t.Fatalf("err %v, want the failed check with exit %d", err, c.observe)
			}
			if r.Applied || len(r.ActionsApplied) != 0 || len(r.Patches) != 0 {
				t.Fatalf("a failed check wrote: %+v", r)
			}
		})
	}
}
