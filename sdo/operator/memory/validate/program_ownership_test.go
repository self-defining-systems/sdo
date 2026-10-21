package validate

import (
	"fmt"
	"sdo.dev/operator/memory/layout"
	"testing"
)

func programOwnership(p string, a Actor, phase Phase) error {
	k, id, ok := layout.Classify(p)
	if !ok || !allowed(a, phase, k, id) {
		return fmt.Errorf("denied")
	}
	return nil
}
func TestResponderOwnsProgramLibrariesAndCorpus(t *testing.T) {
	for _, path := range []string{".sdo/lib/ports/ports.go", ".sdo/corpus/case-1/labels.json", ".sdo/playbooks/targetport/diagnose.go"} {
		if err := programOwnership(path, Responder, ReflectionPhase); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if err := programOwnership(path, HealthJudge, InitialPhase); err == nil {
			t.Fatalf("health judge owns %s", path)
		}
	}
}
