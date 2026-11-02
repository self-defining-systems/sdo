// Package toolio builds the one real operator binary that subprocess tests
// share and reads the CI-prepared replacement. Tests use the real compiler here
// and nowhere else; stage-level contracts run over testutil.FakeGo.
package toolio

import (
	"os"
	"os/exec"
)

// PreparedOperatorBinary returns the source-verified CLI supplied by CI, or "".
func PreparedOperatorBinary() string { return os.Getenv("SDO_TEST_OPERATOR_BINARY") }

// BuildPackage compiles one package to output with the real Go toolchain.
func BuildPackage(output, pkg string) ([]byte, error) {
	return exec.Command("go", "build", "-o", output, pkg).CombinedOutput()
}
