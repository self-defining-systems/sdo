package validate

import (
	"testing"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

// Memory accepted before the coverage rules (no records marker, an unprobed
// entry) still operates: the rules block only commits that change the map,
// probes or health detectors, and a playbook commit passes.
func TestAcceptedMemoryWithoutTheMarkerStillOperates(t *testing.T) {
	root := mapTree(t, nil)
	commit(t, root, "accepted before the coverage rules")
	write(t, root, layout.PlaybooksDir+"/restart-web/README.md", "# restart-web\n")
	for _, actor := range []Actor{HealthJudge, Responder} {
		request := Request{Root: root, Base: "HEAD", Actor: actor, Phase: InitialPhase, Layers: Structural | Semantic}
		if got := mapRules(diagnose(t, request)); len(got) != 0 {
			t.Fatalf("%s: a playbook commit on an accepted map was held to map rules: %v", actor, got)
		}
	}
}

func TestCoverageRuleScopeFollowsTheDiff(t *testing.T) {
	finding := appmap.Finding{Rule: appmap.RuleEffectUnobserved, Message: "m"}
	for name, tc := range map[string]struct {
		changed []string
		blocks  bool
	}{
		"playbook only":        {[]string{layout.PlaybooksDir + "/x/README.md"}, false},
		"traffic generator":    {[]string{layout.TrafficGeneratorsDir + "/g.go"}, true},
		"map":                  {[]string{layout.ArchitectureFile}, true},
		"health detector":      {[]string{layout.DetectorsDir + "/health/x/detector.go"}, true},
		"no base (onboarding)": {nil, true},
	} {
		s := &run{req: Request{Actor: HealthJudge, Phase: InitialPhase, Layers: Compile}, changed: tc.changed}
		s.mapFinding(Compile, finding, 0)
		if blocked := len(s.diags) == 1; blocked != tc.blocks {
			t.Errorf("%s: blocked=%v", name, blocked)
		}
	}
}

// Reading records users already have is advice: no designer commit is held
// to it, whatever the diff touches.
func TestRecordsUnreadNeverBlocks(t *testing.T) {
	for _, changed := range [][]string{nil, {layout.ArchitectureFile}, {layout.TrafficGeneratorsDir + "/g.go"}} {
		var notes []Diagnostic
		s := &run{req: Request{Actor: HealthJudge, Phase: InitialPhase, Layers: Compile, CoverageGaps: &notes}, changed: changed}
		s.mapFinding(Compile, appmap.Finding{Rule: appmap.RuleRecordsUnread, Message: "m"}, 0)
		if len(s.diags) != 0 || len(notes) != 1 {
			t.Errorf("changed %v: diags=%v notes=%v", changed, s.diags, notes)
		}
	}
}
