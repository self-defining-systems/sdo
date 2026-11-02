package validate

import (
	"sdo.dev/operator/memory/layout"
	"testing"
)

func TestOperatorImportExcludesJudgeObjectiveAndControllerArtifacts(t *testing.T) {
	for _, path := range []string{
		".sdo/playbooks/health-objective/README.md",
		".sdo/playbooks/health-objective/diagnose.go",
		".sdo/playbooks/health-objective/scripts/fix.sh",
		layout.GoalFile, layout.ArchitectureFile,
		".sdo/outcomes/record.json",
	} {
		if err := programOwnership(path, Human, ImportPhase); err == nil {
			t.Errorf("human import allowed protected path %s", path)
		}
	}
}
