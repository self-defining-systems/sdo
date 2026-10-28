package broker_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

var cliFixture struct {
	once              sync.Once
	binary, directory string
	err               error
}

// One immutable CLI per test process; CI supplies its source-verified prepared CLI.
func operatorCLIFixture(t *testing.T) string {
	t.Helper()
	cliFixture.once.Do(func() {
		if prepared := os.Getenv("SDO_TEST_OPERATOR_BINARY"); prepared != "" {
			cliFixture.binary = prepared
			_, cliFixture.err = os.Stat(prepared)
			return
		}
		cliFixture.directory, cliFixture.err = os.MkdirTemp("", "sdo-test-cli-")
		if cliFixture.err != nil {
			return
		}
		cliFixture.binary = filepath.Join(cliFixture.directory, "sdo-operator")
		command := exec.Command("go", "build", "-o", cliFixture.binary, "../cmd/sdo-operator")
		if output, err := command.CombinedOutput(); err != nil {
			cliFixture.err = fmt.Errorf("build CLI: %w: %s", err, output)
		}
	})
	if cliFixture.err != nil {
		t.Fatal(cliFixture.err)
	}
	return cliFixture.binary
}

func TestOperatorCLIFixtureReusesExecutable(t *testing.T) {
	first := operatorCLIFixture(t)
	if first != operatorCLIFixture(t) {
		t.Fatal("CLI fixture must reuse the same executable")
	}
	if _, err := os.Stat(first); err != nil {
		t.Fatal(err)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	if cliFixture.directory != "" {
		os.RemoveAll(cliFixture.directory)
	}
	os.Exit(code)
}
