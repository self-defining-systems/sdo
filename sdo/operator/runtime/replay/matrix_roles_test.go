package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type roleDetector struct {
	class sdk.DetectorClass
	mode  string
	links []string
}

func (d roleDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "role-" + string(d.class) + "-" + d.mode, Class: d.class, Playbooks: d.links}
}
func (d roleDetector) Detect(_ context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.mode == "error" {
		return nil, fmt.Errorf("synthetic observation unavailable")
	}
	active := d.mode == "always"
	if d.mode == "faulted" {
		for _, s := range v.Services() {
			active = active || s.Spec.Selector["app"] == "wrong"
		}
	}
	if active {
		return []sdk.Finding{{Status: sdk.FindingActive}}, nil
	}
	return nil, nil
}

// Check serialized categories so these regressions fail behaviorally before the
// report fields exist, and cover the JSONL consumer contract.
func category(t *testing.T, r Result, name string) []string {
	t.Helper()
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	var out []string
	if value, ok := fields[name]; ok {
		if err = json.Unmarshal(value, &out); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func detectorRow(t *testing.T, d sdk.Detector, c Case) Result {
	t.Helper()
	registry, _ := program.NewRegistry(nil, nil)
	rows := Matrix(context.Background(), registry, []sdk.Detector{d}, []Case{c})
	if len(rows) != 1 {
		t.Fatalf("got %d rows", len(rows))
	}
	return rows[0]
}

func TestMatrixHealthFalsePositive(t *testing.T) {
	r := detectorRow(t, roleDetector{class: sdk.DetectorClassHealth, mode: "always", links: []string{"field"}}, fixture("field"))
	if r.Status != "failed" || len(category(t, r, "health_false_positives")) != 1 || len(category(t, r, "incident_false_positives")) != 0 {
		t.Fatalf("health FP misclassified: %+v", r)
	}
}

func TestMatrixHealthFalseNegative(t *testing.T) {
	// Health expectation follows visible impact, even without a family link.
	r := detectorRow(t, roleDetector{class: sdk.DetectorClassHealth}, fixture("field"))
	if r.Status != "failed" || !r.ExpectedMatch || len(category(t, r, "health_false_negatives")) != 1 {
		t.Fatalf("health FN missed: %+v", r)
	}
}

func TestMatrixHealthOverlap(t *testing.T) {
	for _, links := range [][]string{nil, {"other-family"}} {
		r := detectorRow(t, roleDetector{class: sdk.DetectorClassHealth, mode: "faulted", links: links}, fixture("field"))
		if r.Status != "detected" || len(r.FalsePositives) != 0 || len(category(t, r, "overlap")) != 1 {
			t.Fatalf("health overlap became failure: %+v", r)
		}
	}
}

func TestMatrixLinkedHealthUsesRoleAcrossFamilies(t *testing.T) {
	c := fixture("field")
	c.Corpus.RootCauseProgramIDs = []string{"other-family"}
	d := roleDetector{class: sdk.DetectorClassHealth, mode: "faulted", links: []string{"field"}}
	// This program's Diagnose stays negative, but its linked symptom detector
	// still sees the other family's impact. Check must agree with Matrix.
	p := roleDiagnoser{result: program.NoMatch}
	r := Check(context.Background(), p, nil, []sdk.Detector{d}, c)
	if r.Status != "negative" || len(r.FalsePositives) != 0 || len(category(t, r, "overlap")) != 1 {
		t.Fatalf("linked health overlap became failure: %+v", r)
	}
}

func TestMatrixIncidentFalsePositive(t *testing.T) {
	r := detectorRow(t, roleDetector{class: sdk.DetectorClassIncident, mode: "faulted", links: []string{"other-family"}}, fixture("field"))
	if r.Status != "failed" || len(category(t, r, "incident_false_positives")) != 1 || len(category(t, r, "overlap")) != 0 {
		t.Fatalf("incident FP misclassified: %+v", r)
	}
}

func TestMatrixIncidentFalseNegative(t *testing.T) {
	r := detectorRow(t, roleDetector{class: sdk.DetectorClassIncident, links: []string{"field"}}, fixture("field"))
	if r.Status != "failed" || len(category(t, r, "incident_false_negatives")) != 1 {
		t.Fatalf("incident FN missed: %+v", r)
	}
}

type roleDiagnoser struct{ result program.Result }

func (roleDiagnoser) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "field"} }
func (p roleDiagnoser) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	for _, s := range v.Services() {
		if s.Spec.Selector["app"] == "wrong" {
			return program.Verdict{Result: p.result}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func TestMatrixDiagnoseFalsePositive(t *testing.T) {
	c := fixture("field")
	c.Corpus.RootCauseProgramIDs = []string{"other-family"}
	p := roleDiagnoser{result: program.Match}
	r := Check(context.Background(), p, nil, nil, c)
	if r.Status != "failed" || len(category(t, r, "incident_false_positives")) != 1 {
		t.Fatalf("Diagnose FP missed: %+v", r)
	}
}

func TestMatrixDiagnoseFalseNegative(t *testing.T) {
	p := roleDiagnoser{result: program.NoMatch}
	r := Check(context.Background(), p, nil, nil, fixture("field"))
	if r.Status != "failed" || len(category(t, r, "incident_false_negatives")) != 1 {
		t.Fatalf("Diagnose FN missed: %+v", r)
	}
}

func writeRoleCase(t *testing.T, labels string) string {
	t.Helper()
	dir := t.TempDir()
	c := fixture("field")
	for name, value := range map[string]any{"open.json": c.Corpus.Open, "healthy.json": c.Corpus.Healthy, "fixed.json": c.Corpus.Fixed, "recordings.json": c.Corpus.Recordings, "accepted.json": c.Accepted} {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, name), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "labels.json"), []byte(labels), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestMatrixHealthImpactLabels(t *testing.T) {
	for _, label := range []struct {
		name, json string
		wantFN     bool
	}{
		{"default", `{"root_cause_program_ids":["field"]}`, true},
		{"visible", `{"root_cause_program_ids":["field"],"health_impact":true}`, true},
		{"latent", `{"root_cause_program_ids":["field"],"health_impact":false}`, false},
	} {
		t.Run(label.name, func(t *testing.T) {
			cases, err := LoadCases(writeRoleCase(t, label.json), "")
			if err != nil {
				t.Fatal(err)
			}
			r := detectorRow(t, roleDetector{class: sdk.DetectorClassHealth}, cases[0])
			if got := len(category(t, r, "health_false_negatives")) > 0; got != label.wantFN || (r.Status == "failed") != label.wantFN {
				t.Fatalf("health impact scoring: %+v", r)
			}
			// A latent fault still cannot excuse firing on Healthy. Its incident
			// detector and Diagnose must still recognize their own family.
			if !label.wantFN {
				r = detectorRow(t, roleDetector{class: sdk.DetectorClassHealth, mode: "always"}, cases[0])
				if r.Status != "failed" || len(category(t, r, "health_false_positives")) != 1 {
					t.Fatalf("latent label excused health FP: %+v", r)
				}
				r = detectorRow(t, roleDetector{class: sdk.DetectorClassIncident, links: []string{"field"}}, cases[0])
				if len(category(t, r, "incident_false_negatives")) != 1 {
					t.Fatalf("latent label excused incident FN: %+v", r)
				}
				p := roleDiagnoser{result: program.NoMatch}
				r = Check(context.Background(), p, nil, nil, cases[0])
				if len(category(t, r, "incident_false_negatives")) != 1 {
					t.Fatalf("latent label excused Diagnose FN: %+v", r)
				}
			}
		})
	}
}

func TestLoadCasesRejectsInvalidHealthImpact(t *testing.T) {
	if _, err := LoadCases(writeRoleCase(t, `{"health_impact":"false"}`), ""); err == nil {
		t.Fatal("accepted a non-boolean health_impact")
	}
}

func TestMatrixUnknownDiagnoseIsFailure(t *testing.T) {
	for _, positive := range []bool{false, true} {
		c := fixture("field")
		if !positive {
			c.Corpus.RootCauseProgramIDs = []string{"other-family"}
		}
		p := roleDiagnoser{result: program.Unknown}
		r := Check(context.Background(), p, nil, nil, c)
		if r.Status != "failed" || len(r.Errors) == 0 || len(category(t, r, "incident_false_negatives")) != 0 {
			t.Fatalf("Unknown is not a classified negative: %+v", r)
		}
	}
}

func TestMatrixHealthObservationErrorIsFailure(t *testing.T) {
	r := detectorRow(t, roleDetector{class: sdk.DetectorClassHealth, mode: "error"}, fixture("field"))
	if r.Status != "failed" || len(category(t, r, "health_false_negatives")) != 0 {
		t.Fatalf("observation error is not a silent negative: %+v", r)
	}
}

func TestRunSeparatesMatrixOutcomes(t *testing.T) {
	dir := writeRoleCase(t, `{"root_cause_program_ids":["field"]}`)
	registry, _ := program.NewRegistry(nil, nil)
	detectors := []sdk.Detector{
		roleDetector{class: sdk.DetectorClassHealth, mode: "always"},
		roleDetector{class: sdk.DetectorClassHealth},
		roleDetector{class: sdk.DetectorClassIncident, mode: "faulted"},
		roleDetector{class: sdk.DetectorClassIncident, links: []string{"field"}},
	}
	var out, summary bytes.Buffer
	if code := Run(context.Background(), registry, detectors, []string{"-fixtures", dir}, &out, &summary); code != 1 {
		t.Fatalf("exit %d: %s", code, summary.String())
	}
	for _, name := range []string{"HEALTH FALSE POSITIVES", "HEALTH FALSE NEGATIVES", "INCIDENT AND DIAGNOSE FALSE POSITIVES", "INCIDENT AND DIAGNOSE FALSE NEGATIVES", "OVERLAP"} {
		if !strings.Contains(summary.String(), name) {
			t.Errorf("summary lacks %s: %s", name, summary.String())
		}
	}
	for _, field := range []string{"health_false_positives", "health_false_negatives", "incident_false_positives", "incident_false_negatives", "overlap"} {
		if !strings.Contains(out.String(), `"`+field+`"`) {
			t.Errorf("JSONL lacks %s", field)
		}
	}
}

func TestRunHealthOverlapPasses(t *testing.T) {
	dir := writeRoleCase(t, `{"root_cause_program_ids":["field"]}`)
	registry, _ := program.NewRegistry(nil, nil)
	var out, summary bytes.Buffer
	code := Run(context.Background(), registry, []sdk.Detector{roleDetector{class: sdk.DetectorClassHealth, mode: "faulted"}}, []string{"-fixtures", dir}, &out, &summary)
	if code != 0 || !strings.Contains(summary.String(), "0 health FP, 0 health FN, 0 incident/Diagnose FP, 0 incident/Diagnose FN, 1 overlap") {
		t.Fatalf("overlap affected exit: %d\n%s", code, summary.String())
	}
}

func TestRunDoesNotDoubleCountLinkedDetectors(t *testing.T) {
	dir := writeRoleCase(t, `{"root_cause_program_ids":["field"]}`)
	p := roleDiagnoser{result: program.NoMatch}
	registry, _ := program.NewRegistry([]program.Diagnoser{p}, nil)
	var out, summary bytes.Buffer
	code := Run(context.Background(), registry, []sdk.Detector{roleDetector{class: sdk.DetectorClassIncident, links: []string{"field"}}}, []string{"-fixtures", dir, "-repeat", "2"}, &out, &summary)
	if code != 1 || !strings.Contains(summary.String(), "0 health FP, 0 health FN, 0 incident/Diagnose FP, 4 incident/Diagnose FN, 0 overlap") {
		t.Fatalf("detector and Diagnose should each count once per repetition: %d\n%s", code, summary.String())
	}
}
