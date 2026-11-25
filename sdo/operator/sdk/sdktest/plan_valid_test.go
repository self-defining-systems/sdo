package sdktest

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type planMitigator struct {
	plan program.Plan
	err  error
}

func (planMitigator) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "fix"} }

func (m planMitigator) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return m.plan, m.err
}

func repairPlan(statement string) program.Plan {
	target := program.StoreTarget{Component: "db", Engine: "postgres", Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db-0"}, Container: "db"}
	scope := program.DataScope{Database: "app", Collection: "packages", Selector: "status = 2", Limit: 32}
	return program.Plan{Actions: []program.Action{program.RepairData{Target: target, Scope: scope, Statement: statement, ExpectMin: 1, ExpectMax: 32}}}
}

// A data-writing plan is checked as the host will apply it, so a learned program cannot fail on apply for a reason
// the check could have caught.
func TestCheckPlanValidRefusesAPlanTheHostWillRefuse(t *testing.T) {
	if err := CheckPlanValid(planMitigator{plan: repairPlan("SET status = 0")}, CorpusCase{}); err != nil {
		t.Fatalf("valid plan: %v", err)
	}
	err := CheckPlanValid(planMitigator{plan: repairPlan("UPDATE packages")}, CorpusCase{})
	if err == nil || !strings.Contains(err.Error(), "program.plan-invalid") || !strings.Contains(err.Error(), "SET clause") {
		t.Fatalf("invalid statement: %v", err)
	}
	if err := CheckPlanValid(planMitigator{err: errors.New("no store in the recorded reads")}, CorpusCase{}); err != nil {
		t.Fatalf("a Mitigate that cannot run is unverified, not invalid: %v", err)
	}
}

// The closing turn's check has the incident's captured open state before Go writes any case, so a program that is
// new in this incident is still run on real state; one that cannot run there is reported as unverified.
func TestCheckPlansOnIncidentRunsMitigateOnTheCapturedOpenState(t *testing.T) {
	dir := t.TempDir()
	raw, err := json.Marshal(Snapshot{NamespaceName: "app"})
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"open@detect.json": raw, "open@detect-recordings.json": []byte("[]")} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if unverified, err := CheckPlansOnIncident(planMitigator{plan: repairPlan("SET status = 0")}, dir, nil); err != nil || len(unverified) != 0 {
		t.Fatalf("valid plan: %v, %v", unverified, err)
	}
	_, err = CheckPlansOnIncident(planMitigator{plan: repairPlan("UPDATE packages")}, dir, nil)
	if err == nil || !strings.Contains(err.Error(), "program.plan-invalid") || !strings.Contains(err.Error(), "open@detect.json") {
		t.Fatalf("invalid plan: %v", err)
	}
	if unverified, err := CheckPlansOnIncident(planMitigator{plan: repairPlan("UPDATE packages")}, t.TempDir(), nil); err != nil || len(unverified) != 0 {
		t.Fatalf("no capture, nothing to check: %v, %v", unverified, err)
	}
}

// A playbook that was never run on its own incident must be visible as such, not silently passed.
func TestCheckPlansOnIncidentSaysWhyAPlanWasUnverified(t *testing.T) {
	dir := t.TempDir()
	raw, _ := json.Marshal(Snapshot{NamespaceName: "app"})
	if err := os.WriteFile(filepath.Join(dir, "open@detect.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	unverified, err := CheckPlansOnIncident(planMitigator{err: errors.New("binding resource missing")}, dir, nil)
	if err != nil || len(unverified) != 1 || !strings.Contains(unverified[0], "open@detect.json") || !strings.Contains(unverified[0], "binding resource missing") {
		t.Fatalf("unverified = %v, %v", unverified, err)
	}
	unverified, err = CheckPlansOnIncident(planMitigator{}, dir, nil)
	if err != nil || len(unverified) != 1 || !strings.Contains(unverified[0], "no action") {
		t.Fatalf("empty plan: %v, %v", unverified, err)
	}
}

type escalatingProgram struct {
	planMitigator
	reach string
}

func (p escalatingProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Match, CauseClass: "object-missing", Reach: p.reach}, nil
}

func escalatePlan() program.Plan {
	return program.Plan{Actions: []program.Action{program.Escalate{Reason: "run the DDL by hand"}}}
}

// A sole Escalate under an in-scope diagnosis is a legal partial playbook, reported as an unverified-plan note.
func TestSoleEscalateUnderAnInScopeDiagnosisIsANoteNotAFailure(t *testing.T) {
	dir := t.TempDir()
	raw, _ := json.Marshal(Snapshot{NamespaceName: "app"})
	if err := os.WriteFile(filepath.Join(dir, "open@detect.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, reach := range []string{"", program.ReachInScope} {
		unverified, err := CheckPlansOnIncident(escalatingProgram{planMitigator{plan: escalatePlan()}, reach}, dir, nil)
		if err != nil || len(unverified) != 1 || !strings.Contains(unverified[0], "EnsureExec") {
			t.Fatalf("reach %q: %v, %v", reach, unverified, err)
		}
	}
	unverified, err := CheckPlansOnIncident(escalatingProgram{planMitigator{plan: escalatePlan()}, program.ReachOutOfReach}, dir, nil)
	if err != nil || len(unverified) != 0 {
		t.Fatalf("out-of-reach escalation needs no note: %v, %v", unverified, err)
	}
}

type panickingMitigator struct{ planMitigator }

func (panickingMitigator) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	panic("index out of range")
}

// A Mitigate that panics on the capture is unverified, like one that returns an error there.
func TestCheckPlansOnIncidentReportsAPanicAsUnverified(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "open@detect.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	unverified, err := CheckPlansOnIncident(panickingMitigator{}, dir, nil)
	if err != nil || len(unverified) != 1 || !strings.Contains(unverified[0], "panicked: index out of range") {
		t.Fatalf("unverified = %v, %v", unverified, err)
	}
}

type mapMitigator struct{ planMitigator }

func (m mapMitigator) Mitigate(_ context.Context, v program.ReadView, _ program.Bindings) (program.Plan, error) {
	_, err := v.AppMap()
	return m.plan, err
}

// The application map is the repository's, so a program that reads it is judged on a capture that did not record it.
func TestCheckPlansOnIncidentServesTheApplicationMap(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "open@detect.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	m := mapMitigator{planMitigator{plan: repairPlan("UPDATE packages")}}
	if unverified, err := CheckPlansOnIncident(m, dir, nil); err != nil || len(unverified) != 1 {
		t.Fatalf("without a map: %v, %v", unverified, err)
	}
	if _, err := CheckPlansOnIncident(m, dir, &program.AppMap{}); err == nil || !strings.Contains(err.Error(), "program.plan-invalid") {
		t.Fatalf("with the map: %v", err)
	}
}
