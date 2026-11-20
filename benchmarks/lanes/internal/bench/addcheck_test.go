package bench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// `plan add` runs the checks a submit runs: a fault ID missing from the instances file, or a missing instances file or
// baseline directory, is refused at add time (and by --dry-run), not retried at every manager pass.
func TestSpecInputsAreCheckedAtAddTime(t *testing.T) {
	d := t.TempDir()
	inst := filepath.Join(d, "instances.json")
	if err := os.WriteFile(inst, []byte(`{"ids": ["f1@a", "f2@b"]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	s := Spec{Faults: Faults{Instances: inst, IDs: []string{"f1@a", Healthy}}}
	if err := checkSpecInputs(s); err != nil {
		t.Fatalf("good inputs refused: %v", err)
	}
	s.Faults.IDs = []string{"f1@a", "nope@x"}
	if err := checkSpecInputs(s); err == nil || !strings.Contains(err.Error(), "fault IDs not in the instances file: nope@x") {
		t.Fatalf("a fault ID that is not in the instances file: %v", err)
	}
	s.Faults = Faults{Instances: filepath.Join(d, "missing.json"), IDs: []string{"f1@a"}}
	if err := checkSpecInputs(s); err == nil {
		t.Fatal("a missing instances file passed")
	}
	s.Faults = Faults{Instances: inst, IDs: []string{"f1@a"}}
	s.Operator.Baseline = filepath.Join(d, "no-baseline")
	if err := checkSpecInputs(s); err == nil || !strings.Contains(err.Error(), "baseline") {
		t.Fatalf("a missing baseline: %v", err)
	}
}
