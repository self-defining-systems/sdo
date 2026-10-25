package replay

import (
	"context"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
)

func TestRepeatReplayRequiresDetectorFindingAndRuntimeBindings(t *testing.T) {
	for _, mode := range []string{"no-detector", "unbound-role", "correct", "healthy-finding"} {
		t.Run(mode, func(t *testing.T) {
			p := repeatRoleProgram{}
			registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
			if err != nil {
				t.Fatal(err)
			}
			c := databaseCase()
			// The case's authored binding is deliberately wrong. Only a finding may
			// choose the binding used for diagnosis and the production executor replay.
			c.Corpus.Bindings = program.Bindings{Objects: program.ObjectBindings{"database": {Kind: "Pod", Namespace: "app", Name: "invented"}}}
			var detectors []sdk.Detector
			if mode != "no-detector" {
				detectors = []sdk.Detector{repeatCatalogDetector{bind: mode != "unbound-role", always: mode == "healthy-finding"}}
			}
			got := CheckRepeat(context.Background(), registry, detectors, p.Spec().ID, c)
			if mode == "correct" {
				if got.Status != "replayed" || got.BoundObjects["database"].Name != "database" {
					t.Fatalf("repeat path failed: %+v", got)
				}
				return
			}
			if got.Status == "replayed" || !strings.Contains(strings.Join(got.Errors, " "), "program.repeat-unreachable") {
				t.Fatalf("broken %s accepted: %+v", mode, got)
			}
			if mode == "unbound-role" && !strings.Contains(strings.Join(got.Errors, " "), "database") {
				t.Fatalf("missing role not named: %+v", got)
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

type repeatCatalogDetector struct{ bind, always bool }

func (repeatCatalogDetector) Spec() sdk.DetectorSpec { return databaseCatalogDetector{}.Spec() }
func (d repeatCatalogDetector) Detect(ctx context.Context, s sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := (databaseCatalogDetector{}).Detect(ctx, s)
	if d.always && err == nil && len(findings) == 0 {
		findings = []sdk.Finding{{DetectorID: d.Spec().ID, RuleID: "unexpected", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "Unexpected healthy finding", Evidence: "fixture forces an invalid baseline", PrimaryResource: dbAction().Target.Pod}}
	}
	for i := range findings {
		findings[i].PrimaryResource = sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "catalog"}
		if d.bind {
			findings[i].ParameterBindings = map[string]sdk.ObjectRef{"database": {Kind: "Pod", Namespace: "app", Name: "database"}}
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
