package runtime

import (
	"context"
	"errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// Refresh is before the first health-authorized evaluation, not a failed
// onboarding attempt. Exercise the real judge/broker publication path too.
func TestSourceInventoryPendingOnboardingWaitsForHealthAuthority(t *testing.T) {
	ctx := context.Background()
	root, _, _, worker, coordinator := tracingFixture(t)
	installed := nativeGit(t, root, "rev-parse", "HEAD")
	client := fake.NewSimpleClientset()
	if err := onboarding.Stage(ctx, client, "control", "demo", installed, nil, time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	newController := func(commit string) *Controller {
		t.Helper()
		config := testControllerConfig()
		config.Namespace, config.SourceCommit, config.RepositoryWorktree = "demo", commit, root
		c, err := NewController(config, []sdk.Detector{inventoryHealth()}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 10)}, time.Unix(100, 0))
		if err != nil {
			t.Fatal(err)
		}
		c.CheckHealth = acceptedHealthGuard(root, commit)
		if err := c.AttachPendingOnboarding(ctx, client, "control"); err != nil {
			t.Fatal(err)
		}
		return c
	}
	c := newController(installed)
	for i := 0; i < 2; i++ {
		if err := c.EvaluateAll(ctx, time.Unix(101+int64(i), 0)); !errors.Is(err, memory.ErrHealthRefreshRequired) {
			t.Fatalf("unverified health evaluated: %v", err)
		}
		if _, err := client.CoreV1().ConfigMaps("control").Get(ctx, onboarding.AttemptConfigMapName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("refresh consumed onboarding attempt: %v", err)
		}
	}
	refreshed := tracingRefresh(t, root, coordinator)
	c = newController(refreshed.Commit)
	if err := c.EvaluateAll(ctx, time.Unix(103, 0)); err != nil {
		t.Fatal(err)
	}
	record, err := onboarding.Load(ctx, client, "control", "demo")
	if err != nil || record == nil || record.AcceptedCommit != installed {
		t.Fatalf("real startup refresh invalidated install: %v %v", record, err)
	}
	if worker.calls != 1 {
		t.Fatalf("refresh worker calls: %d", worker.calls)
	}
}
