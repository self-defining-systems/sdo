//go:build integration

package bench

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// LB14: the root helper's PATH must keep /usr/local/bin and /usr/local/sbin: lane-exec runs every step with it, and a
// host whose kubectl lives there lost its lanes with "kubectl not found".
func TestLaneReserveHelperPathKeepsUsrLocal(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "deploy", "host", "lane-reserve"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*PATH=(\S+)\s*$`).FindSubmatch(b)
	if m == nil {
		t.Fatal("the helper sets no PATH")
	}
	have := map[string]bool{}
	for _, d := range strings.Split(string(m[1]), ":") {
		have[d] = true
	}
	for _, d := range []string{"/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin"} {
		if !have[d] {
			t.Errorf("helper PATH %s lacks %s", m[1], d)
		}
	}
}

// LB14: preflight names every external tool that does not resolve in the steps' environment.
func TestToolsScriptNamesMissingTools(t *testing.T) {
	dir := t.TempDir()
	for _, tool := range []string{"docker", "kind", "ip"} {
		os.WriteFile(filepath.Join(dir, tool), []byte("#!/bin/sh\n"), 0o755)
	}
	for _, h := range []Host{{}, {Provider: ProviderVcluster}} {
		cmd := exec.Command("sh", "-c", toolsScript(h))
		cmd.Env = []string{"PATH=" + dir}
		out, err := cmd.CombinedOutput()
		ok, detail := judgeTools(string(out), err)
		if ok || !strings.Contains(detail, "kubectl") || strings.Contains(detail, "docker") {
			t.Fatalf("provider %q: ok=%v detail=%q (out %q)", h.Provider, ok, detail, out)
		}
	}
	if ok, detail := judgeTools("", nil); !ok {
		t.Fatalf("no missing tool must pass: %q", detail)
	}
}
