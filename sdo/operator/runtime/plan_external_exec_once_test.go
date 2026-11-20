package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// slowExecClient stands for an application CLI that costs a full boot per
// call. It counts calls so a test can bound what one action pays.
type slowExecClient struct {
	clock *settlingClock
	mu    sync.Mutex
	fixed bool
	calls []string
}

func (c *slowExecClient) Get(_ context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{Object: map[string]any{}}
	u.SetName(ref.Name)
	u.SetUID(types.UID("synthetic-" + ref.Name))
	return u, nil
}
func (c *slowExecClient) Patch(context.Context, sdk.ObjectRef, types.PatchType, []byte) error {
	return nil
}
func (c *slowExecClient) RunOperation(_ context.Context, op program.ExternalOperation) (int32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, op.Exec.Args[0])
	if c.clock != nil {
		c.clock.advance(context.Background(), 70*time.Second)
	}
	if op.Exec.Args[0] == "ensure" {
		c.fixed = true
		return 0, nil
	}
	if c.fixed {
		return 0, nil
	}
	return 1, nil
}

func slowExecAction() program.EnsureExec {
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "toolbox"}
	return program.EnsureExec{
		Observe: program.Command{Pod: pod, Container: "c", Args: []string{"observe"}},
		Ensure:  program.Command{Pod: pod, Container: "c", Args: []string{"ensure"}},
		Expect:  0, Reason: "synthetic",
	}
}

// An action pays its exec cost at most once for the check and once for the
// change: the verdict that the state holds belongs to the post-apply diagnosis
// and the alerts, not to a third run of the same command.
func TestEnsureExecPaysEachCommandOnce(t *testing.T) {
	t.Run("state to change", func(t *testing.T) {
		clock := &settlingClock{now: time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)}
		client := &slowExecClient{clock: clock}
		r, err := (PlanExecutor{Client: client, Now: clock.Now}).externalStep(context.Background(), slowExecAction(), false)
		if err != nil {
			t.Fatal(err)
		}
		if len(client.calls) != 2 || client.calls[0] != "observe" || client.calls[1] != "ensure" {
			t.Fatalf("exec calls = %v, want [observe ensure]", client.calls)
		}
		if r.ActionReports[0].Status != "applied" || r.ActionReports[0].ExternalEffect == nil {
			t.Fatalf("report lost its evidence: %+v", r.ActionReports[0])
		}
		if e := r.ActionReports[0].ExternalEffect; e.Before == 0 || e.After != 0 || e.ExitCode != 0 || e.AfterObserved || e.ApplyMs != 140000 {
			t.Fatalf("effect %+v: want before!=expect, after assumed, not observed", e)
		}
	})
	t.Run("state already held", func(t *testing.T) {
		client := &slowExecClient{fixed: true}
		if _, err := (PlanExecutor{Client: client}).externalStep(context.Background(), slowExecAction(), false); err != nil {
			t.Fatal(err)
		}
		if len(client.calls) != 1 {
			t.Fatalf("exec calls = %v, want one observation", client.calls)
		}
	})
}
