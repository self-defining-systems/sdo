package validate

import (
	"testing"
)

// A program deciding on a diff's Text (the p024 image-drift predicate) still
// loads, but memory check notes it so reflection rewrites it with typed fields.
// Text used only as Evidence is not noted.
func TestProgramDiffTextMatchIsNotedNotRejected(t *testing.T) {
	root := goodTree(t)
	write(t, root, ".sdo/playbooks/base/diagnose.go", `package base

import (
	"context"
	"strings"

	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	diff, err := v.DiffAgainstSource(b.Objects["deployment"])
	if err != nil {
		return program.Verdict{}, err
	}
	if !strings.Contains(diff.Text, "image") {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Observed: diff.Text}}}, nil
}

func changed(d program.Diff) bool { return d.Text != "" }
`)
	var gaps []Diagnostic
	diagnose(t, Request{Root: root, Layers: Structural | Semantic, CoverageGaps: &gaps})
	var lines []int
	for _, gap := range gaps {
		if gap.Rule == RuleProgramDiffText {
			if gap.Path != ".sdo/playbooks/base/diagnose.go" {
				t.Fatalf("note path = %q", gap.Path)
			}
			lines = append(lines, gap.Line)
		}
	}
	if len(lines) != 2 || lines[0] != 17 || lines[1] != 23 {
		t.Fatalf("Text match notes at lines %v, want [17 23]; gaps: %v", lines, gaps)
	}
}
