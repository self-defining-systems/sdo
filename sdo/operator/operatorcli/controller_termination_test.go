package operatorcli

import (
	"context"
	"errors"
	"os/exec"
	"testing"
)

func exitWith(t *testing.T, script string) error {
	t.Helper()
	err := exec.Command("sh", "-c", script).Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("%q did not exit with a status: %v", script, err)
	}
	return err
}

// A signal stop ends the parent with the terminated status whatever the child
// reports for the stop it was asked to make (a clean cancellation, the
// terminated status, or the kill after the grace); a child that failed for its
// own reason is still a failure; and without a signal nothing is rewritten.
func TestTerminationResult(t *testing.T) {
	boom := errors.New("controller run: boom")
	for _, tc := range []struct {
		name      string
		signalled bool
		err       error
		terminate bool
	}{
		{"no signal, clean", false, nil, false},
		{"no signal, child terminated", false, exitWith(t, "exit 143"), false},
		{"no signal, failure", false, boom, false},
		{"signal, clean", true, nil, true},
		{"signal, cancellation", true, context.Canceled, true},
		{"signal, child exited as terminated", true, exitWith(t, "exit 143"), true},
		{"signal, child killed", true, exitWith(t, "kill -9 $$"), true},
		{"signal, child failed on its own", true, exitWith(t, "exit 1"), false},
		{"signal, other failure", true, boom, false},
	} {
		got := terminationResult(tc.signalled, tc.err)
		if errors.Is(got, ErrTerminated) != tc.terminate {
			t.Errorf("%s: terminated = %v, want %v (result %v)", tc.name, errors.Is(got, ErrTerminated), tc.terminate, got)
		}
		if !tc.terminate && got != tc.err {
			t.Errorf("%s: result %v, want the error unchanged %v", tc.name, got, tc.err)
		}
	}
}
