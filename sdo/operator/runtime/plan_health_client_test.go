package runtime

import (
	"context"
	"errors"
	"testing"
)

func TestXV2HealthGuardForwardsRuntimeActions(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		e, client, ref := runtimeActionFixture()
		gateErr := errors.New("health policy changed")
		guarded := healthGuardedPlanClient{PlanObjectClient: e.Client, Check: func(context.Context) error {
			if blocked {
				return gateErr
			}
			return nil
		}}
		deleter, ok := any(guarded).(planPodDeleter)
		if !ok {
			t.Fatal("health wrapper drops conditional pod deletion")
		}
		executor, ok := any(guarded).(PlanExecClient)
		if !ok {
			t.Fatal("health wrapper drops output-free exec")
		}
		err := deleter.Delete(context.Background(), ref, "pod-uid", "7")
		code, execErr := executor.Exec(context.Background(), ref, "app", []string{"true"})
		if blocked {
			if !errors.Is(err, gateErr) || !errors.Is(execErr, gateErr) || client.deleted || len(client.command) != 0 {
				t.Fatalf("health gate bypassed: delete=%v exec=%v", err, execErr)
			}
		} else if err != nil || execErr != nil || !client.deleted || code != 3 || len(client.command) != 1 {
			t.Fatalf("actions not forwarded: delete=%v exec=%v code=%d", err, execErr, code)
		}
	}
}
