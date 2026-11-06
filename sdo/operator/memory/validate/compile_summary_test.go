package validate

import (
	"strings"
	"testing"
)

const nilContextPanic = `ok  	sdo.dev/diagnostics/detectors/incident/a	0.01s
ok  	sdo.dev/diagnostics/detectors/incident/b	0.01s
--- FAIL: TestScenarioDetectsWrongBodyNearMiss (0.00s)
panic: cannot create context from nil parent [recovered]
	panic: cannot create context from nil parent

goroutine 7 [running]:
testing.tRunner.func1.2({0x1, 0x2})
	/usr/local/go/src/testing/testing.go:1631 +0x24a
context.WithDeadlineCause(...)
	/usr/local/go/src/context/context.go:600
sdo.dev/operator/sdk/traffic.(*Engine).Iterate(...)
	/home/x/sdk/traffic/engine.go:195
sdo.dev/diagnostics/detectors/incident/c.TestScenarioDetectsWrongBodyNearMiss(0x1)
	/tmp/sdo-operator-build-123/detectors/incident/c/detector_test.go:16 +0x1d
FAIL	sdo.dev/diagnostics/detectors/incident/c	0.02s
FAIL
`

func TestSummarizeGoTestKeepsPanicAndAuthoredFrame(t *testing.T) {
	message, path := summarizeGoTest(nilContextPanic)
	for _, want := range []string{"cannot create context from nil parent", "detectors/incident/c/detector_test.go:16", "--- FAIL: TestScenarioDetectsWrongBodyNearMiss"} {
		if !strings.Contains(message, want) {
			t.Errorf("summary lacks %q:\n%s", want, message)
		}
	}
	for _, drop := range []string{"ok  \t", "goroutine", "testing.go:1631", "context.go"} {
		if strings.Contains(message, drop) {
			t.Errorf("summary keeps noise %q:\n%s", drop, message)
		}
	}
	if len(message) > len(nilContextPanic)/2 {
		t.Errorf("summary is not shorter: %d of %d bytes", len(message), len(nilContextPanic))
	}
	if !strings.HasSuffix(path, "detectors/incident/c/detector_test.go") {
		t.Errorf("path = %q", path)
	}
}

func TestSummarizeGoTestKeepsFailedAssertions(t *testing.T) {
	message, _ := summarizeGoTest("--- FAIL: TestX (0.00s)\n    detector_test.go:9: want firing\nFAIL\nFAIL\tpkg\t0.1s\n")
	if !strings.Contains(message, "detector_test.go:9: want firing") {
		t.Fatalf("summary = %q", message)
	}
	if got, _ := summarizeGoTest("build noise only\n"); got != "" {
		t.Fatalf("unrecognised output must fall back to raw, got %q", got)
	}
}
