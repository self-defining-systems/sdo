package memory

import (
	"os"
	"path/filepath"
	"testing"
)

// The fixtures are the documents the Python writer's test pins, so the writer and this reader share one shape.
func TestSetupUsageReadsTheWorkersTaskCheckpoint(t *testing.T) {
	for _, phase := range []string{"deploy", "design"} {
		t.Run(phase, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", "setup-usage", phase+".json"))
			if err != nil {
				t.Fatal(err)
			}
			task := t.TempDir()
			directory := filepath.Join(task, "usage", phase)
			if err := os.MkdirAll(directory, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, phase+".json"), data, 0o600); err != nil {
				t.Fatal(err)
			}
			usage, problems, err := SetupUsage(task, "setup-task-"+phase, phase)
			if err != nil || len(problems) != 0 || usage.GetInputTokens() != 120 || usage.GetPartial() {
				t.Fatalf("usage = %v, problems = %v, err = %v", usage, problems, err)
			}
			if _, _, err := SetupUsage(task, "another-task", phase); err == nil {
				t.Fatal("another task's checkpoint was accepted")
			}
			if _, _, err := PhaseUsage(t.TempDir(), "", "inc-1", phase); err == nil {
				t.Fatal("a setup phase was accepted as incident usage")
			}
		})
	}
	if _, _, err := SetupUsage(t.TempDir(), "task", "respond"); err == nil {
		t.Fatal("an incident phase was accepted as setup usage")
	}
}
