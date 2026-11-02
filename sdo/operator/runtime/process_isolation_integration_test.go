//go:build integration

package runtime

import (
	"os"
	"os/exec"
	"testing"
)

// Process-global state (klog verbosity and output) is read by every informer
// reflector in the process, so a test that changes it races with any
// goroutine another test left behind. Such a test runs alone in a child
// process of the test binary: nothing else can be running there.

// inIsolatedProcess returns true in the child, where the test body must run.
// In the parent it runs this one test in a child process, fails t with the
// child's output if it fails, and returns false.
func inIsolatedProcess(t *testing.T) bool {
	t.Helper()
	if os.Getenv(isolatedChildEnv) == t.Name() {
		return true
	}
	command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1", "-test.timeout=5m")
	command.Env = append(os.Environ(), isolatedChildEnv+"="+t.Name())
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("isolated child process failed: %v\n%s", err, output)
	}
	return false
}
