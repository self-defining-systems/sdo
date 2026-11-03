//go:build integration && controller_e2e

package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Bind is one extra mount an "image" contributes to every container.
type Bind struct {
	Src, Dst string
	RO       bool
}

// Images describes the one container image every Pod runs: the host's Debian
// userland plus the SDO operator binary, Go toolchain, Python environment and
// source tree at the paths the production images use. Env holds the image's
// ENV defaults (a Pod's own env overrides them); Force replaces Pod values for
// host adaptations such as the persistent compiler cache.
type Images struct {
	Env      map[string]string
	Force    map[string]string
	Binds    []Bind
	SkipInit map[string]bool
}

// DefaultImages builds the profile for a source tree and a built sdo-operator binary.
func DefaultImages(t testing.TB, repoRoot, operatorBinary, cacheDir string) *Images {
	t.Helper()
	goroot := runOutput(t, "go", "env", "GOROOT")
	modcache := runOutput(t, "go", "env", "GOMODCACHE")
	venv := filepath.Join(repoRoot, ".venv")
	if _, err := os.Stat(filepath.Join(venv, "bin", "python3")); err != nil {
		t.Fatalf("python environment missing (run `uv sync --extra test`): %v", err)
	}
	buildCache := filepath.Join(cacheDir, "go-build")
	if err := os.MkdirAll(buildCache, 0o755); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(cacheDir, "empty-seed")
	if err := os.MkdirAll(empty, 0o755); err != nil {
		t.Fatal(err)
	}
	binds := []Bind{
		{Src: repoRoot, Dst: "/opt/sdo", RO: true},
		{Src: operatorBinary, Dst: "/usr/local/bin/sdo-operator", RO: true},
		{Src: goroot, Dst: "/usr/local/go", RO: true},
		{Src: modcache, Dst: "/go/pkg/mod"},
		{Src: buildCache, Dst: "/e2e-go-build"},
		{Src: empty, Dst: "/go/build-cache", RO: true}, // the image's cache seed: empty, the shared cache is warm already
	}
	if kubectl, err := exec.LookPath("kubectl"); err == nil {
		binds = append(binds, Bind{Src: kubectl, Dst: "/usr/local/bin/kubectl", RO: true})
	}
	// The image's `sdo` tool is a two-line shim over the responder tool CLI.
	shim := filepath.Join(cacheDir, "sdo-shim")
	if err := os.WriteFile(shim, []byte("#!/bin/sh\nexec python3 -m sdo.agents.responder.tools_cli \"$@\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	binds = append(binds, Bind{Src: shim, Dst: "/usr/local/bin/sdo", RO: true})
	return &Images{
		Env: map[string]string{
			"HOME": "/tmp", "PATH": filepath.Join(venv, "bin") + ":/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin",
			"PYTHONPATH": "/opt/sdo", "GIT_AUTHOR_NAME": "SDO Agent", "GIT_AUTHOR_EMAIL": "sdo-agent@sdo.dev",
			"GIT_COMMITTER_NAME": "SDO Agent", "GIT_COMMITTER_EMAIL": "sdo-agent@sdo.dev",
			"GOCACHE": "/tmp/go-cache", "GOMODCACHE": "/go/pkg/mod", "GOPROXY": "off", "GOSUMDB": "off", "GOMAXPROCS": "4",
			"GOFLAGS": "-mod=mod -p=4 -trimpath", "CGO_ENABLED": "0", "SDO_GO_CACHE_SEED": "/go/build-cache",
			"SDO_PLAYBOOK_PROGRAM_EXAMPLE_DIR": "/opt/sdo/examples/playbook-programs", "GOTOOLCHAIN": "local",
		},
		// A cold compiler cache costs minutes; every container shares one warm cache.
		Force:    map[string]string{"GOCACHE": "/e2e-go-build", "GOMAXPROCS": "4", "GOTOOLCHAIN": "local"},
		Binds:    binds,
		SkipInit: map[string]bool{"prepare-toolchain": true},
	}
}

func runOutput(t testing.TB, name string, args ...string) string {
	t.Helper()
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		t.Fatalf("%s %v: %v", name, args, err)
	}
	return string(out[:len(out)-1])
}
