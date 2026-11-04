package validate

import (
	"context"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// A healthy-baseline replay judges detector code against a recorded cluster
// sample. When the candidate changed neither the failing detector nor the
// shared code it can import, the same accepted bytes fail the same sample at
// the base: the fault is pre-existing, no edit by this candidate can fix it,
// and Check records it as a note instead of rejecting the candidate.
func TestCheckAttributesHealthyBaselineFaultsOnUnchangedDetectors(t *testing.T) {
	cacheDir := layout.DetectorDir(layout.ClassIncident, "cache")
	violation := Diagnostic{Rule: RuleDetectorHealthyBaseline, Path: cacheDir, Message: `detector "cache" failed on healthy baseline snapshot healthy.json: traffic "health" unavailable`}
	compile := func(context.Context, Request) ([]Diagnostic, error) { return []Diagnostic{violation}, nil }
	for _, tc := range []struct {
		name     string
		edit     func(*testing.T, string)
		blocking bool
	}{
		{"playbook-only change", gatePlaybookEdit, false},
		{"the failing detector changed", func(t *testing.T, r string) {
			gatePlaybookEdit(t, r)
			gateUnscopedBinding("possiblePlaybooks:\n  - base", `Playbooks: []string{"base"},`)(t, r)
		}, true},
		{"shared library changed", func(t *testing.T, r string) {
			gatePlaybookEdit(t, r)
			write(t, r, ".sdo/lib/shared/shared.go", "package shared\n\nconst Name = \"shared\"\n")
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := goodTree(t)
			gateOutcome(gateClean())(t, root)
			commit(t, root, gateOutcomeMessage)
			tc.edit(t, root)
			var notes []Diagnostic
			request := Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, CoverageGaps: &notes,
				Decision: gateUpdated("base"), Publication: &PublicationContext{BaseCommit: "HEAD", IncidentID: gateIncident}}
			diagnostics, err := Check(context.Background(), request, compile)
			if err != nil {
				t.Fatal(err)
			}
			blocked := slices.ContainsFunc(diagnostics, func(d Diagnostic) bool { return d.Rule == RuleDetectorHealthyBaseline })
			if blocked != tc.blocking {
				t.Fatalf("healthy-baseline blocking = %v, want %v: %v", blocked, tc.blocking, diagnostics)
			}
			noted := slices.ContainsFunc(notes, func(d Diagnostic) bool {
				return d.Rule == RuleDetectorHealthyBaseline && d.Path == cacheDir && strings.Contains(d.Message, "pre-existing") && strings.Contains(d.Message, violation.Message)
			})
			if noted == tc.blocking {
				t.Fatalf("pre-existing note = %v, want %v: %v", noted, !tc.blocking, notes)
			}
		})
	}
}
