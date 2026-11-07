package rehearsal

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBlockersNameEveryProblemOfARelease(t *testing.T) {
	r := Release{
		Name: "r1", Outcome: "held", Hold: "rollout", Cause: "rollout", Owner: "responder", Items: []string{"O1"}, Detail: "Job/x failed",
		Drift:       []Finding{{Detector: "drift.unexplained", Resource: "Job/ns/old", Summary: "absent from accepted source"}},
		Generations: []GenerationChange{{Workload: "Deployment/ns/a", Before: 1, After: 2, RenderedChange: true}, {Workload: "StatefulSet/ns/b", Before: 1, After: 2, Changed: []string{"spec.x: 1 -> 2"}}},
		Operability: &OperabilityCheck{Valid: false, Error: "duplicated", Duplicates: []string{"O1.apis"}},
	}
	got := strings.Join(r.Blockers(), "\n")
	for _, want := range []string{"not adopted: held rollout (cause=rollout owner=responder items=O1): Job/x failed", "drift drift.unexplained Job/ns/old", "generation churn: StatefulSet/ns/b 1 -> 2 with no rendered change; live spec changed: spec.x: 1 -> 2", "operability report invalid: duplicated", "duplicate operability check O1.apis"} {
		if !strings.Contains(got, want) {
			t.Errorf("blockers lack %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "Deployment/ns/a") {
		t.Errorf("a workload whose render changed is not churn:\n%s", got)
	}
	if blockers := (Release{Adopted: true, Outcome: "accepted"}).Blockers(); len(blockers) != 0 {
		t.Errorf("a clean adopted release has blockers %v", blockers)
	}
}

func TestSpecDiffListsChangedPaths(t *testing.T) {
	var before, after any
	_ = json.Unmarshal([]byte(`{"replicas":1,"template":{"spec":{"containers":[{"image":"a:1"}]}}}`), &before)
	_ = json.Unmarshal([]byte(`{"replicas":1,"template":{"spec":{"containers":[{"image":"a:2"}]}}}`), &after)
	got := SpecDiff(before, after, 5)
	if len(got) != 1 || !strings.HasPrefix(got[0], `spec.template.spec.containers[0].image: "a:1" -> "a:2"`) {
		t.Errorf("SpecDiff = %v", got)
	}
	if got := SpecDiff(before, before, 5); len(got) != 0 {
		t.Errorf("equal specs differ: %v", got)
	}
}
