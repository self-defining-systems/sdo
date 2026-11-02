package replay

import (
	"context"
	"errors"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
)

func repeatRegistry(t *testing.T, p repeatRoleProgram, withMitigator bool) program.ProgramRegistry {
	t.Helper()
	var mitigators []program.Mitigator
	if withMitigator {
		mitigators = []program.Mitigator{p}
	}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, mitigators)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func repeatCaseWithBinding(name string) Case {
	c := databaseCase()
	c.Corpus.Bindings = program.Bindings{Objects: program.ObjectBindings{"database": {Kind: "Pod", Namespace: "app", Name: name}}}
	return c
}

func TestRepeatReplayChoosesBindingsFromDetectorFindings(t *testing.T) {
	p := repeatRoleProgram{}
	// The case's authored binding is deliberately wrong. Only a finding may
	// choose the binding used for diagnosis and the production executor replay.
	got := CheckRepeat(context.Background(), repeatRegistry(t, p, true), []sdk.Detector{repeatCatalogDetector{bind: true}}, p.Spec().ID, repeatCaseWithBinding("invented"))
	if got.Status != "replayed" || got.BoundObjects["database"].Name != "database" || len(got.Notes) != 0 {
		t.Fatalf("repeat path failed: %+v", got)
	}
}

// A finding that never selects the playbook is information; the mitigation is
// still replayed with the case bindings and must repair.
func TestRepeatUnreachableIsANoteAndTheMitigationStillMustRepair(t *testing.T) {
	for _, mode := range []string{"no-detector", "unbound-role"} {
		t.Run(mode, func(t *testing.T) {
			var detectors []sdk.Detector
			if mode != "no-detector" {
				detectors = []sdk.Detector{repeatCatalogDetector{}}
			}
			p := repeatRoleProgram{}
			got := CheckRepeat(context.Background(), repeatRegistry(t, p, true), detectors, p.Spec().ID, repeatCaseWithBinding("database"))
			notes := strings.Join(got.Notes, " ")
			if got.Status != "replayed" || len(got.Errors) != 0 || !strings.Contains(notes, RuleRepeatUnreachable) {
				t.Fatalf("unreachable repeat was not a replayed note: %+v", got)
			}
			if mode == "unbound-role" && !strings.Contains(notes, "database") {
				t.Fatalf("missing role not named: %+v", got)
			}
			broken := CheckRepeat(context.Background(), repeatRegistry(t, repeatRoleProgram{databaseProgram{wrong: true}}, true), detectors, p.Spec().ID, repeatCaseWithBinding("database"))
			if broken.Status == "replayed" || len(broken.Errors) == 0 {
				t.Fatalf("unreachable repeat skipped the mitigation repair check: %+v", broken)
			}
		})
	}
}

// Program and detector replay defects remain observable under their own rules.
func TestRepeatObservationsNameProgramAndDetectorDefects(t *testing.T) {
	for _, test := range []struct {
		name          string
		detector      repeatCatalogDetector
		withMitigator bool
		rule          string
	}{
		{"diagnose does not match its own fixture", repeatCatalogDetector{bind: true, otherDatabase: true}, true, RuleDiagnoseNoMatch},
		{"missing mitigator", repeatCatalogDetector{bind: true}, false, RuleMitigatorMissing},
		{"invalid finding", repeatCatalogDetector{bind: true, invalid: true}, true, RuleDetectorInvalid},
		{"detector fires on the healthy fixture", repeatCatalogDetector{bind: true, always: true}, true, RuleDetectorFiresHealthy},
		{"detector unavailable", repeatCatalogDetector{bind: true, fail: true}, true, RuleRepeatUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := repeatRoleProgram{}
			got := CheckRepeat(context.Background(), repeatRegistry(t, p, test.withMitigator), []sdk.Detector{test.detector}, p.Spec().ID, repeatCaseWithBinding("database"))
			if got.Status == "replayed" || !strings.Contains(strings.Join(got.Errors, " "), test.rule+":") || len(got.Notes) != 0 {
				t.Fatalf("want observation %s, got %+v", test.rule, got)
			}
		})
	}
}

type repeatRoleProgram struct{ databaseProgram }

func (repeatRoleProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "database-user", Params: []program.ParamSpec{{Name: "database", Kind: "Pod", Required: true}}}
}
func (p repeatRoleProgram) Diagnose(ctx context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	if b.Objects["database"].Name != "database" {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return p.databaseProgram.Diagnose(ctx, v, b)
}
func (p repeatRoleProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	if b.Objects["database"].Name != "database" {
		return program.Plan{}, nil
	}
	return p.databaseProgram.Mitigate(ctx, v, b)
}

type repeatCatalogDetector struct{ bind, always, otherDatabase, invalid, fail bool }

func (repeatCatalogDetector) Spec() sdk.DetectorSpec { return databaseCatalogDetector{}.Spec() }
func (d repeatCatalogDetector) Detect(ctx context.Context, s sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := (databaseCatalogDetector{}).Detect(ctx, s)
	if d.fail && len(findings) > 0 {
		return nil, errors.New("detector failed on the faulted fixture")
	}
	if d.always && err == nil && len(findings) == 0 {
		findings = []sdk.Finding{{DetectorID: d.Spec().ID, RuleID: "unexpected", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "Unexpected healthy finding", Evidence: "fixture forces an invalid baseline", PrimaryResource: dbAction().Target.Pod}}
	}
	for i := range findings {
		findings[i].PrimaryResource = sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "catalog"}
		if d.bind {
			name := "database"
			if d.otherDatabase {
				name = "other"
			}
			findings[i].ParameterBindings = map[string]sdk.ObjectRef{"database": {Kind: "Pod", Namespace: "app", Name: name}}
		}
		if d.invalid {
			findings[i].Summary = ""
		}
	}
	return findings, err
}

func BenchmarkRecordedRepeatCheck(b *testing.B) {
	p := repeatRoleProgram{}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		b.Fatal(err)
	}
	c := databaseCase()
	c.Corpus.Bindings = program.Bindings{Objects: program.ObjectBindings{"database": {Kind: "Pod", Namespace: "app", Name: "database"}}}
	detectors := []sdk.Detector{repeatCatalogDetector{bind: true}}
	b.Run("repair", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if r := CheckRepair(context.Background(), p, p, c); r.Status != "replayed" {
				b.Fatal(r)
			}
		}
	})
	b.Run("repeat", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if r := CheckRepeat(context.Background(), registry, detectors, p.Spec().ID, c); r.Status != "replayed" {
				b.Fatal(r)
			}
		}
	})
}
