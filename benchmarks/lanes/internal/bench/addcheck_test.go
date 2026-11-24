package bench

import (
	"os"
	"path/filepath"
	"slices"
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

func TestSpecInputsCheckCompositeIDs(t *testing.T) {
	const composite = "pair[one@a&two@b?n=2]"
	inst := filepath.Join(t.TempDir(), "instances.json")
	for _, tc := range []struct {
		name, catalog, wantError string
		ids                      []string
	}{
		{"mixed", `{"ids":["one@a"],"composite_ids":["` + composite + `"],"composites":[{"composite":"pair"}]}`, "", []string{Healthy, "one@a", composite}},
		{"wrong member", `{"composite_ids":["` + composite + `"],"composites":[{"composite":"pair"}]}`, "fault IDs not in", []string{"pair[one@b&two@b?n=2]"}},
		{"ambiguous", `{"ids":["` + composite + `"],"composite_ids":["` + composite + `"],"composites":[{"composite":"pair"}]}`, "both", []string{composite}},
		{"old expansion", `{"composites":[{"composite":"pair"}]}`, "composite_ids", []string{composite}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(inst, []byte(tc.catalog), 0o644); err != nil {
				t.Fatal(err)
			}
			err := checkSpecInputs(Spec{Faults: Faults{Instances: inst, IDs: tc.ids}})
			if tc.wantError == "" && err != nil || tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
				t.Fatalf("checkSpecInputs: %v, want error containing %q", err, tc.wantError)
			}
		})
	}
}

func TestFaultFlagsFollowTheCatalog(t *testing.T) {
	const composite = "pair[one@a&two@b?n=2]"
	path := filepath.Join(t.TempDir(), "instances with spaces.json")
	if err := os.WriteFile(path, []byte(`{"ids":["one@a"],"composite_ids":["`+composite+`"],"composites":[{}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string][]string{
		Healthy:   {"--fault", "none"},
		"one@a":   {"--instance-file", path, "--instance", "one@a"},
		composite: {"--composite-file", path, "--composite", composite},
	} {
		args, err := (RunSpec{Fault: id, Instances: path}).faultFlags()
		if err != nil || !slices.Equal(args, want) {
			t.Errorf("faultFlags(%q) = %q, %v, want %q", id, args, err, want)
		}
	}
}
