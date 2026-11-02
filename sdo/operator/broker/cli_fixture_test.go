package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"sdo.dev/operator/internal/testutil/toolio"
)

var cliFixture struct {
	once              sync.Once
	binary, directory string
	err               error
}

// OperatorCLIFixture shares one immutable CLI per test process; CI supplies its source-verified prepared CLI.
func OperatorCLIFixture(t *testing.T) string {
	t.Helper()
	cliFixture.once.Do(func() {
		if prepared := toolio.PreparedOperatorBinary(); prepared != "" {
			cliFixture.binary = prepared
			_, cliFixture.err = os.Stat(prepared)
			return
		}
		cliFixture.directory, cliFixture.err = os.MkdirTemp("", "sdo-test-cli-")
		if cliFixture.err != nil {
			return
		}
		cliFixture.binary = filepath.Join(cliFixture.directory, "sdo-operator")
		if output, err := toolio.BuildPackage(cliFixture.binary, "../cmd/sdo-operator"); err != nil {
			cliFixture.err = fmt.Errorf("build CLI: %w: %s", err, output)
		}
	})
	if cliFixture.err != nil {
		t.Fatal(cliFixture.err)
	}
	return cliFixture.binary
}

func TestMain(m *testing.M) {
	code := m.Run()
	if cliFixture.directory != "" {
		os.RemoveAll(cliFixture.directory)
	}
	os.Exit(code)
}
