package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

const syntheticLogSignal = `components: [web]
signals:
  - {id: denied, re2: 'permission denied'}
expect:
  - {id: served, re2: 'request served'}
`

func writeLogSignal(t *testing.T, root, id, body string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(layout.LogSignalPath(id)))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// Each log signal becomes a Go-installed health detector built from the file
// and the map's workload of each component; deleting the file removes it.
func TestPrepareHealthInstallsADetectorPerLogSignal(t *testing.T) {
	root := t.TempDir()
	writeMap(t, root, syntheticNoData)
	writeLogSignal(t, root, "web-auth", syntheticLogSignal)
	writeLogSignal(t, root, "unmapped", strings.Replace(syntheticLogSignal, "[web]", "[nowhere]", 1))
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	source := detectorSource(t, root, "logs-web-auth")
	for _, want := range []string{"logsignal.NewDetector(", `"web": "Deployment/web"`, "permission denied", "logs.Watch"} {
		if !strings.Contains(source, want) {
			t.Fatalf("detector source lacks %q:\n%s", want, source)
		}
	}
	if detectorExists(root, "logs-unmapped") {
		t.Fatal("a log signal naming an unmapped component must be left to the shared check")
	}
	if err := os.Remove(filepath.Join(root, filepath.FromSlash(layout.LogSignalPath("web-auth")))); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	if detectorExists(root, "logs-web-auth") {
		t.Fatal("the deleted log signal's detector must go")
	}
}
