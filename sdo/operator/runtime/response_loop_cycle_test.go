package runtime

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// A response loop is one act→verify cycle (docs/design/playbook-programs.md):
// every write between two verifications shares one loop, and a call that
// writes nothing spends none. run17/run18 lost 13 incidents because each
// mitigate call, including planning failures, was counted as a loop.

func loopCycleMitigate(t *testing.T, loop *ResponderLoop, index int) ToolResponse {
	t.Helper()
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	if menu.Menu == nil {
		t.Fatalf("menu unavailable: %s", menu.Error)
	}
	return loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: plainProgramOption(t, menu.Menu, index%2), RoundLimit: 1}})
}

// SetField without its required path and value fails in planning, before any write.
func loopCycleUnwritable(loop *ResponderLoop) ToolResponse {
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	return loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "SetField", RoundLimit: 1}, Parameters: map[string]string{"ref": "Service/app/backend"}})
}

func loopCycleVerify(loop *ResponderLoop) ToolResponse {
	return loop.Handle(context.Background(), ToolRequest{Command: "verify"})
}
