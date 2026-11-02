package validate

import (
	"path/filepath"
	"testing"
)

// compileTree is a repository whose only memory is the committed example
// detector package, which builds against the SDK and passes its own test.
func compileTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	fixture := filepath.Join("..", "..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app", ".sdo")
	copyTree(t, fixture, filepath.Join(root, ".sdo"))
	return root
}

func compileRequest(root string) Request {
	return Request{Root: root, Layers: Compile, OperatorRoot: filepath.Join("..", "..")}
}

const detectorSource = ".sdo/diagnostics/detectors/incidents/missing-configmap/detector.go"
