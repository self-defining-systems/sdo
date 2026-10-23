package runtime

import (
	"context"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
)

func TestResponderMenuPlainNeedsNoCapabilities(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.MenuConfig = &ResponderMenuConfig{}
	before := cluster.reads
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	if menu.Error != "" || menu.Menu == nil || len(menu.Menu.Options) != 2 || cluster.reads != before {
		t.Fatalf("listing must only inspect the registry: %s", menu.Error)
	}
}

func TestResponderMenuPlainFiveRoundsAcrossOptions(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.MenuConfig = productionResponderMenu()
	loop.Executor.Verifier = planFakeVerifier{true}
	for i := 0; i < 5; i++ {
		menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
		if menu.Menu.RemainingBatches != uint32(5-i) {
			t.Fatalf("allowance reset: %v", menu.Menu)
		}
		result := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: menu.Menu.Options[i%2].OptionId, RoundLimit: 1}})
		if result.Report == nil {
			t.Fatalf("missing report: %s", result.Error)
		}
	}
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	result := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: menu.Menu.Options[0].OptionId, RoundLimit: 1}})
	if result.Error == "" || loop.LoopsUsed() != 5 {
		t.Fatal("sixth round executed")
	}
}

func TestResponderMenuRepairEvidenceSatisfiesIncidentContract(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	executed := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: menu.Menu.Options[1].OptionId, RoundLimit: 1}})
	if executed.Report == nil || executed.Report.Status != contracts.MitigationReport_STATUS_SUCCESS {
		t.Fatalf("executor did not succeed: %v", executed)
	}
	result := completedResult(loop.Request.IncidentID)
	result.RepairActions = loop.MenuRepairActions()
	if len(result.RepairActions) != 1 {
		t.Fatal("missing controller repair evidence")
	}
	if err := validateIncidentResultFor(result, loop.Request); err != nil {
		t.Fatalf("menu evidence rejected before broker closure: %v", err)
	}
}
