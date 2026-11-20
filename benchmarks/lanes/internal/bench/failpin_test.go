//go:build integration

package bench

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// An unknown pin, and a fuzzops binary that does not report its pinned revision (unstamped), never get as far as a
// lane: `add` refuses them, and nothing is written.
func TestBadSpecIsRefusedAtAddAndNeverReachesALane(t *testing.T) {
	l := newFlab(t, "h1=c1")
	write := func(name, body string) string {
		p := filepath.Join(l.specs, name+".yaml")
		must(t, os.WriteFile(p, []byte(body), 0o644))
		return p
	}
	spec := func(name, pin string) string {
		return fmt.Sprintf("name: %s\napp: examples/shop\nfuzzops: %s\noperator: {builtin: none}\nnamespace: app1\nfaults:\n  instances: instances.json\n  ids: [a@x]\nsequences: [[1]]\nrun: {window: 1s}\nready_for: 100ms\nready_timeout: 1s\ncap: 10m\nlanes: 1\nhosts: [h1]\nretries: 0\npause_after_wave: false\n", name, pin)
	}
	if err := Add(context.Background(), l.root, write("unknown", spec("unknown", "ffffff0")), os.Stderr); err == nil {
		t.Fatal("a spec with an unknown pin was accepted")
	}

	// A host whose fuzzops binary is unstamped: it reports another revision than the pin.
	bad := filepath.Join(l.dir, "badfuzzops")
	must(t, os.MkdirAll(bad, 0o755))
	if out, err := execCmd("cp", "-r", testdata("fakefuzzops")+"/.", bad); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	script := filepath.Join(bad, "bin", "fuzzops")
	b, _ := os.ReadFile(script)
	must(t, os.WriteFile(script, []byte(strings.Replace(string(b), `{"revision":"0b528ac0123456","measure_version":12}`, `{"revision":"unknown","modified":true,"measure_version":12}`, 1)), 0o755))
	inv, _ := os.ReadFile(l.root.path("hosts.yaml"))
	must(t, os.WriteFile(l.root.path("hosts.yaml"), []byte(strings.Replace(string(inv), testdata("fakefuzzops"), bad, -1)), 0o644))
	err := Add(context.Background(), l.root, write("unstamped", spec("unstamped", "0b528ac")), os.Stderr)
	if err == nil {
		t.Fatal("a spec whose host binary is not stamped with the pinned revision was accepted")
	}
	if !strings.Contains(err.Error(), "h1") {
		t.Fatalf("the refusal does not name the host: %v", err)
	}
	if fileExists(l.root.LockPath("unstamped")) || fileExists(l.root.SpecPath("unstamped")) {
		t.Fatal("a refused spec left files in the root")
	}
	l.pass()
	if l.w.nStarted() != 0 {
		t.Fatal("a lane was used for a refused spec")
	}
}

func execCmd(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return string(out), err
}
