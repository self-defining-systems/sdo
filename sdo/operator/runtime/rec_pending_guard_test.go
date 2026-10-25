package runtime

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

type recTimeoutRollout struct{}

func (recTimeoutRollout) Wait(context.Context, []sdk.ObjectRef) error {
	return context.DeadlineExceeded
}

type recAmbiguousMenuClient struct {
	*menuB3Cluster
	uncertain bool
}

func (c *recAmbiguousMenuClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, data []byte) error {
	if err := c.menuB3Cluster.Patch(ctx, ref, kind, data); err != nil {
		return err
	}
	c.uncertain = true
	return context.DeadlineExceeded // The server applied it; the response was lost.
}

func (c *recAmbiguousMenuClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if c.uncertain {
		return nil, context.DeadlineExceeded
	}
	return c.menuB3Cluster.Get(ctx, ref)
}

func recActualPendingMenu(t *testing.T, ambiguous bool) *ResponderLoop {
	t.Helper()
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	loop.Executor.Rollouts = recTimeoutRollout{}
	if ambiguous {
		loop.Executor.Client = &recAmbiguousMenuClient{menuB3Cluster: cluster}
	}
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	response := loop.Handle(context.Background(), ToolRequest{
		Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "ScaleWorkload", RoundLimit: 1},
		Parameters: map[string]string{"ref": "Deployment/app/frontend", "replicas": "1"},
	})
	if len(loop.applicationsLog) != 1 {
		t.Fatalf("real menu application missing: %+v", response)
	}
	result := loop.applicationsLog[0].Result
	if ambiguous && result.PendingAction == nil {
		t.Fatalf("real menu did not retain ambiguous write: applied=%v rollback=%v", result.Applied, result.RollingBack)
	}
	if !ambiguous && (response.RolloutPending == nil || result.RolloutPending == nil) {
		t.Fatalf("real menu did not retain pending rollout: %+v", response)
	}
	return loop
}

func TestRECActualMenuPendingGuard(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		name := "rollout"
		if ambiguous {
			name = "ambiguous-menu-write"
		}
		t.Run(name, func(t *testing.T) {
			loop := recActualPendingMenu(t, ambiguous)
			_, release, safe := acquireClosureGuard(loop)
			release()
			if safe {
				t.Fatal("closure admitted a real pending menu application")
			}
		})
	}
}

func TestRECClosureRetainsRealPendingRollout(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		name := "rollout"
		if ambiguous {
			name = "ambiguous-menu-write"
		}
		t.Run(name, func(t *testing.T) {
			c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
			c.responderLoop = recActualPendingMenu(t, ambiguous)
			for second := 2; second <= 30; second++ {
				step(second)
				if !c.IncidentOpen() {
					t.Fatalf("early closure admitted pending executor state at %d", second)
				}
			}
		})
	}
}

func TestRECClosureChecksEveryApplicationResult(t *testing.T) {
	index := 0
	for name, result := range map[string]PlanApplicationResult{
		"rollout":         {RolloutPending: &contracts.RolloutPending{}},
		"ambiguous-write": {PendingAction: &index},
		"rollback":        {RollingBack: true},
	} {
		for _, logged := range []bool{false, true} {
			location := "applications"
			if logged {
				location = "application-log"
			}
			t.Run(name+"/"+location, func(t *testing.T) {
				loop, _ := menuB3Loop(t)
				if logged {
					loop.applicationsLog = []RecordedPlanApplication{{ID: "pending", Result: result}}
				} else {
					loop.applications["pending"] = result
				}
				_, release, safe := acquireClosureGuard(loop)
				release()
				if safe {
					t.Fatal("closure ignored pending executor result")
				}
				loop.applicationsLog = nil
				delete(loop.applications, "pending")
				_, release, safe = acquireClosureGuard(loop)
				release()
				if !safe {
					t.Fatal("settled application still blocks closure")
				}
			})
		}
	}
}
