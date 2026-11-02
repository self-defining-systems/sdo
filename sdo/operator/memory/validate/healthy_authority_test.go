package validate

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/internal/capturefixture"
)

func TestReflectionBaselineUsesControllerCaptureInsteadOfAuthoredHealthy(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "base")
	write(t, root, ".sdo/diagnostics/detectors/incidents/cache/detector.go", "package cache\n"+"\n// changed\n")
	capture := t.TempDir()
	if err := os.MkdirAll(filepath.Join(capture, "healthy"), 0700); err != nil {
		t.Fatal(err)
	}
	write(t, capture, "healthy/healthy.json", string(capturefixture.FromView(t, `{"namespace":"app","services":[{"metadata":{"name":"consul","namespace":"app"},"spec":{"ports":[{"port":8300},{"port":8400},{"port":8500}]}}]}`)))
	authored := filepath.Join(root, ".sdo", "corpus", "consul-refused-service-ports")
	// The agent's healthy reconstruction is its post-repair state, with the
	// ordinary healthy app's unused ports removed. It supplies no authority.
	write(t, authored, "healthy.json", string(capturefixture.FromView(t, `{"namespace":"app","services":[{"metadata":{"name":"consul","namespace":"app"},"spec":{"ports":[{"port":8500}]}}]}`)))
	got, err := selectCompileBaseline(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, CaptureDirectory: capture, HealthyBaseline: authored})
	if err != nil || got.HealthyBaseline != filepath.Join(capture, "healthy") {
		t.Fatalf("baseline=%q err=%v, want controller-owned capture", got.HealthyBaseline, err)
	}
}

// A missing controller capture is a recorded gap, never a refusal. The agent's
// own healthy.json still supplies no authority: the healthy replay is skipped.
func TestMissingControllerCaptureSkipsHealthyReplayAndRecordsAGap(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "base")
	write(t, root, ".sdo/diagnostics/detectors/incidents/cache/detector.go", "package cache\n")
	for _, capture := range []string{"", filepath.Join(t.TempDir(), "controller-session")} {
		var gaps []Diagnostic
		got, err := selectCompileBaseline(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, CaptureDirectory: capture, HealthyBaseline: t.TempDir(), CoverageGaps: &gaps})
		if err != nil || got.HealthyBaseline != "" {
			t.Fatalf("missing capture refused or kept the agent baseline: %q %v", got.HealthyBaseline, err)
		}
		if len(gaps) != 1 || gaps[0].Rule != RuleDetectorHealthyBaseline {
			t.Fatalf("missing capture not recorded: %v", gaps)
		}
	}
}
