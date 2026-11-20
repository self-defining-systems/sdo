package replay

import (
	"context"
	"encoding/json"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Replay hands back the recorded apply time of the ensure it replays.
func TestReplayKeepsRecordedApplyTime(t *testing.T) {
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "toolbox"}
	action := program.EnsureExec{
		Observe: program.Command{Pod: pod, Container: "c", Args: []string{"observe"}},
		Ensure:  program.Command{Pod: pod, Container: "c", Args: []string{"ensure"}},
	}
	effect := program.ExternalEffect{Observe: program.ExternalOperation{Exec: &action.Observe}, Ensure: program.ExternalOperation{Exec: &action.Ensure}, Before: 1, ApplyMs: 219800}
	args, _ := json.Marshal([]any{effect.Observe, effect.Ensure})
	result, _ := json.Marshal(effect)
	c := &cluster{}
	if err := c.seedExternal([]sdktest.PrimitiveCall{{Method: "ExternalEffect", Arguments: args, Result: result}}); err != nil {
		t.Fatal(err)
	}
	c.operationState[operationKey(effect.Observe)] = 1
	if _, err := c.EnsureOperation(context.Background(), effect.Observe, effect.Ensure); err != nil {
		t.Fatal(err)
	}
	if got := c.recordedApplyMs[operationKey(ensureOperation(action))]; got != 219800 {
		t.Fatalf("recorded apply ms = %d, want 219800", got)
	}
}
