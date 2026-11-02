package runtime

import (
	"fmt"
	"os"
	"testing"
)

// Tests create many throwaway repositories under t.TempDir. Git's automatic
// maintenance (`gc --auto`) may detach and keep writing into .git/objects after
// the command that triggered it returns, racing TempDir cleanup with
// "directory not empty". Disable it for every git process the tests spawn.
func TestMain(m *testing.M) {
	os.Setenv("GIT_CONFIG_COUNT", "2")
	os.Setenv("GIT_CONFIG_KEY_0", "gc.auto")
	os.Setenv("GIT_CONFIG_VALUE_0", "0")
	os.Setenv("GIT_CONFIG_KEY_1", "maintenance.auto")
	os.Setenv("GIT_CONFIG_VALUE_1", "false")
	code := m.Run()
	// Backstop for informers started where the leak-guarded test constructors
	// do not reach (for example through Run): none may outlive the package.
	if code == 0 && awaitInformerGoroutines(0, informerStopGrace) != 0 {
		fmt.Fprintln(os.Stderr, "FAIL: informer goroutines still running after all tests; some test started informers it never stopped")
		code = 1
	}
	if cliFixture.directory != "" {
		os.RemoveAll(cliFixture.directory)
	}
	os.Exit(code)
}
