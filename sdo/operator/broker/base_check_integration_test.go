//go:build integration

package broker

import (
	"context"
	"testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
)

// The base check reports what is wrong with the accepted tree before any agent
// edit, and nothing for a valid base: the empty diff is what the agent is for.
func TestCheckBaseReportsAnInvalidBaseAndAcceptsAValidOne(t *testing.T) {
	ctx := context.Background()
	w := newOnboardedWorld(t, false)
	// The judge's base is the accepted tree after the Go-written schema and architecture.
	head := gitOut(t, w.accepted, "rev-parse", "HEAD")
	if err := memory.WriteSchema(w.accepted); err != nil {
		t.Fatal(err)
	}
	if err := memory.WriteArchitecture(w.accepted, "test", head, "synthetic inventory"); err != nil {
		t.Fatal(err)
	}
	gitOut(t, w.accepted, "add", "-A")
	gitOut(t, w.accepted, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "synthetic schema and architecture")
	if _, err := w.broker.CreateProposalForPhase(ctx, "judge-clean", InitialPhase); err != nil {
		t.Fatal(err)
	}
	if got, err := w.broker.CheckBase(ctx, "judge-clean", HealthJudge, InitialPhase); err != nil || len(got) != 0 {
		t.Fatalf("valid base: diagnostics %v, err %v", got, err)
	}

	writeFiles(t, w.accepted, map[string]string{".sdo/stray.txt": "not an sdo artifact\n"})
	gitOut(t, w.accepted, "add", "-A")
	gitOut(t, w.accepted, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "synthetic invalid base")
	if _, err := w.broker.CreateProposalForPhase(ctx, "judge-bad", InitialPhase); err != nil {
		t.Fatal(err)
	}
	got, err := w.broker.CheckBase(ctx, "judge-bad", HealthJudge, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("an invalid base reported no diagnostics")
	}
	for _, d := range got {
		if d.GetRule() == "diff.empty" {
			t.Fatalf("the empty-diff diagnostic is not a base defect: %v", got)
		}
	}
}

// Go seeds the first architecture with a map from the manifests: edges, no
// operations or probes. Covering its entry edges is the detection designer's
// work, so the unedited base the designer starts from is not invalid for
// lacking that coverage. (Synthetic two-component app.)
func TestCheckBaseDoesNotHoldTheBaseToTheDesignersCoverageObligations(t *testing.T) {
	ctx := context.Background()
	w := newOnboardedWorld(t, false)
	head := gitOut(t, w.accepted, "rev-parse", "HEAD")
	if err := memory.WriteSchema(w.accepted); err != nil {
		t.Fatal(err)
	}
	inventory := "# Manifest inventory\n\n" +
		"- Deployment/demo/web (source: k8s/web.yaml)\n- Service/demo/web (source: k8s/web.yaml)\n" +
		"- Deployment/demo/api (source: k8s/api.yaml)\n- Service/demo/api (source: k8s/api.yaml)\n\n" +
		appmap.BeginMarker + "\n```yaml\nversion: 1\ncomponents:\n" +
		"  - {name: api, workload: \"Deployment/api\", services: [\"api\"], holds_data: false, evidence: [manifest]}\n" +
		"  - {name: web, workload: \"Deployment/web\", services: [\"web\"], holds_data: false, evidence: [manifest]}\n" +
		"edges:\n  - {from: web, to: api, evidence: [manifest], source: \"Deployment/web env API_URL\"}\n" +
		"operations: []\nprobes: []\n```\n" + appmap.EndMarker + "\n"
	if err := memory.WriteArchitecture(w.accepted, "test", head, inventory); err != nil {
		t.Fatal(err)
	}
	gitOut(t, w.accepted, "add", "-A")
	gitOut(t, w.accepted, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "synthetic seeded architecture")
	if _, err := w.broker.CreateProposalForPhase(ctx, "judge-seeded", InitialPhase); err != nil {
		t.Fatal(err)
	}
	got, err := w.broker.CheckBase(ctx, "judge-seeded", HealthJudge, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("a seeded map's uncovered entry edge is the designer's work, not a base defect: %v", got)
	}
}
