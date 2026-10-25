package runtime

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// A successful startup refresh must not turn the pending installation into a
// failed attempt merely because the judge published new health memory.
func TestSourceInventoryPendingOnboardingAfterVerifiedHealthRefresh(t *testing.T) {
	for _, mode := range []string{"refresh", "unverified", "source", "objective", "intervening-publication"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			root := nativeRepository(t)
			if err := memory.WriteGoal(root, "app", "Keep intended application healthy."); err != nil {
				t.Fatal(err)
			}
			nativeGit(t, root, "add", ".")
			nativeGit(t, root, "commit", "-qm", "inherited unverified seed")
			installed := nativeGit(t, root, "rev-parse", "HEAD")
			client := fake.NewSimpleClientset()
			if err := onboarding.Stage(ctx, client, "control", "app", installed, nil, time.Unix(100, 0)); err != nil {
				t.Fatal(err)
			}
			config := testControllerConfig()
			config.Namespace, config.RepositoryWorktree, config.SourceCommit = "app", root, installed
			c, err := NewController(config, []sdk.Detector{inventoryHealth()}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 10)}, time.Unix(100, 0))
			if err != nil {
				t.Fatal(err)
			}
			if err := c.AttachPendingOnboarding(ctx, client, "control"); err != nil {
				t.Fatal(err)
			}
			if mode == "intervening-publication" {
				tracingWrite(t, root, ".sdo/playbooks/unrelated/README.md", "unrelated publication")
				nativeGit(t, root, "add", ".")
				nativeGit(t, root, "commit", "-qm", "unrelated accepted publication")
			}
			if mode == "source" {
				tracingWrite(t, root, "app.yaml", "changed source")
			}
			if mode == "objective" {
				if err := memory.WriteGoal(root, "app", "Different objective."); err != nil {
					t.Fatal(err)
				}
			}
			tracingWrite(t, root, ".sdo/diagnostics/detectors/health/objective/detector.go", "verified detector")
			message := "judge refresh\n\nSDO-Actor: health_judge\nSDO-Phase: initial\nSDO-Validation: passed"
			if mode == "unverified" {
				message = "compile-only adoption\n\nSDO-Actor: human\nSDO-Phase: adoption\nSDO-Validation: skipped"
			}
			nativeGit(t, root, "add", ".")
			nativeGit(t, root, "commit", "-qm", message)
			c.config.SourceCommit = nativeGit(t, root, "rev-parse", "HEAD")
			if err := c.EvaluateAll(ctx, time.Unix(101, 0)); err != nil {
				t.Fatal(err)
			}
			record, err := onboarding.Load(ctx, client, "control", "app")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "refresh" {
				if record == nil || record.AcceptedCommit != installed || !record.EstablishedAt.AsTime().Equal(time.Unix(100, 0)) {
					t.Fatalf("verified health-only refresh invalidated install evidence: %+v", record)
				}
			} else if record != nil {
				t.Fatalf("%s granted onboarding: %+v", mode, record)
			}
		})
	}
}
