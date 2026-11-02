package memory

import (
	"context"
	"strings"
	"testing"
)

func TestHealthRefreshPreservesOnlyVerifiedInstallLineage(t *testing.T) {
	const judge = "judge\n\nSDO-Actor: health_judge\nSDO-Phase: initial\nSDO-Validation: passed"
	for _, mode := range []string{"two-judges", "render-only", "forged-render", "source-reverted", "objective-reverted", "merge", "unrelated-install", "advanced-head", "unverified-adoption"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			healthTestGit(t, root, "init", "-q")
			healthTestGit(t, root, "config", "user.name", "Test")
			healthTestGit(t, root, "config", "user.email", "test@example.invalid")
			if err := WriteGoal(root, "app", "Keep application healthy."); err != nil {
				t.Fatal(err)
			}
			healthWrite(t, root, "app.yaml", "original")
			healthCommit(t, root, "inherited seed")
			installed := strings.TrimSpace(healthTestGit(t, root, "rev-parse", "HEAD"))
			if mode == "merge" {
				healthTestGit(t, root, "branch", "side")
			}
			if mode == "source-reverted" {
				healthWrite(t, root, "app.yaml", "temporary")
				healthCommit(t, root, judge)
				healthWrite(t, root, "app.yaml", "original")
			}
			if mode == "objective-reverted" {
				if err := WriteGoal(root, "app", "Temporary objective."); err != nil {
					t.Fatal(err)
				}
				healthCommit(t, root, judge)
				if err := WriteGoal(root, "app", "Keep application healthy."); err != nil {
					t.Fatal(err)
				}
			}
			healthWrite(t, root, ".sdo/diagnostics/detectors/health/objective/detector.go", "first judge")
			healthCommit(t, root, judge)
			healthWrite(t, root, ".sdo/diagnostics/detectors/health/objective/detector.go", "second judge")
			healthCommit(t, root, judge)
			if mode == "merge" {
				branch := strings.TrimSpace(healthTestGit(t, root, "branch", "--show-current"))
				healthTestGit(t, root, "switch", "-q", "side")
				healthWrite(t, root, ".sdo/playbooks/side/README.md", "side")
				healthCommit(t, root, judge)
				healthTestGit(t, root, "switch", "-q", branch)
				healthTestGit(t, root, "merge", "--no-ff", "-qm", judge, "side")
			}
			if mode == "render-only" || mode == "forged-render" {
				healthWrite(t, root, ".sdo/rendered/lock.json", "synthetic lock")
				if mode == "forged-render" {
					healthWrite(t, root, ".sdo/playbooks/unrelated/README.md", "unrelated")
				}
				healthCommit(t, root, "render bookkeeping")
			}
			current := strings.TrimSpace(healthTestGit(t, root, "rev-parse", "HEAD"))
			if mode == "unrelated-install" {
				installed = strings.Repeat("a", 40)
			}
			if mode == "advanced-head" {
				healthWrite(t, root, ".sdo/playbooks/later/README.md", "later")
				healthCommit(t, root, "unrelated")
			}
			if mode == "unverified-adoption" {
				var err error
				current, err = MarkAdoptedExisting(ctx, root)
				if err != nil {
					t.Fatal(err)
				}
			}
			ok, err := HealthRefreshPreservesInstall(ctx, root, installed, current)
			if mode == "two-judges" || mode == "render-only" {
				if err != nil || !ok {
					t.Fatalf("verified lineage refused: %v %v", ok, err)
				}
			} else if ok {
				t.Fatalf("%s authorized installation", mode)
			}
		})
	}
}
