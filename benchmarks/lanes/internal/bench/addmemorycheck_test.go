package bench

import (
	"strings"
	"testing"
)

// The add-time memory check builds and validates under the limits the controller image sets (GOMAXPROCS and go -p),
// so its Go build memory does not scale with the manager host's core count.
func TestMemoryScriptRunsUnderTheControllersBuildLimits(t *testing.T) {
	s := memoryScript("/b", baselineJSON{Branch: "main", Bundle: "b"}, "/hooks")
	want := `export GOMAXPROCS=2 GOFLAGS="-p=2 -trimpath"; `
	i := strings.Index(s, want)
	if i < 0 || i > strings.Index(s, "go build") || i > strings.Index(s, "memory migrate") {
		t.Fatalf("limits must be exported before the build and the check: %s", s)
	}
}

// The add-time check has no cluster: an unbound telemetry source is no failure there, any other diagnostic is.
func TestSpecMemoryAllowsUnboundTelemetryOnly(t *testing.T) {
	unbound := "HEAD abc123456789\nCHECK .sdo/diagnostics: telemetry.unbound[request-metrics]: query x names no source\nEXIT 1\n"
	if ok, d := judgeMemory(unbound, nil, "abc123456789", specMemoryAllow); !ok {
		t.Fatal(d)
	}
	if ok, _ := judgeMemory(unbound+"CHECK schema.version: 7\n", nil, "abc123456789", specMemoryAllow); ok {
		t.Fatal("another diagnostic must still fail")
	}
}
