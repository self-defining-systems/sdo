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
	if menu.Error != "" || menu.Menu == nil || len(menu.Menu.Options) != 2+(&contracts.Action{}).ProtoReflect().Descriptor().Oneofs().Get(0).Fields().Len() || cluster.reads != before {
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
		result := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: plainProgramOption(t, menu.Menu, i%2), RoundLimit: 1}})
		if result.Report == nil {
			t.Fatalf("missing report: %s", result.Error)
		}
	}
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	result := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: plainProgramOption(t, menu.Menu, 0), RoundLimit: 1}})
	if result.Error == "" || loop.LoopsUsed() != 5 {
		t.Fatal("sixth round executed")
	}
}

func TestResponderMenuRepairEvidenceSatisfiesIncidentContract(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	executed := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: plainProgramOption(t, menu.Menu, 1), RoundLimit: 1}})
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

func TestResponderMenuListsActionAndParameterRegistry(t *testing.T) {
	loop, _ := menuB3Loop(t)
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	fields := (&contracts.Action{}).ProtoReflect().Descriptor().Oneofs().Get(0).Fields()
	for i := 0; i < fields.Len(); i++ {
		id := string(fields.Get(i).Message().Name())
		found := false
		for _, option := range response.Menu.Options {
			if option.OptionId == id {
				found = true
			}
		}
		if !found {
			t.Errorf("registered action %s absent from menu", id)
		}
	}
}

func plainProgramOption(t *testing.T, menu *contracts.MitigationMenu, index int) string {
	t.Helper()
	ids := []string{}
	for _, option := range menu.Options {
		if option.ProgramId != "" {
			ids = append(ids, option.OptionId)
		}
	}
	if index >= len(ids) {
		t.Fatal("program option missing")
	}
	return ids[index]
}

func TestResponderMenuRendersDeclaredParameters(t *testing.T) {
	loop, _ := menuB3Loop(t)
	response := menuCall(t, loop, map[string]any{"command": "menu"})
	menu := response["menu"].(map[string]any)
	for _, item := range menu["options"].([]any) {
		option := item.(map[string]any)
		params, ok := option["parameters"].([]any)
		if !ok || len(params) == 0 {
			t.Fatalf("option %s has no declared parameter schema", option["option_id"])
		}
	}
}

func TestResponderMenuFailedVerificationRetainsApplicationForExplicitRollback(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{false}
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	executed := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: plainProgramOption(t, menu.Menu, 1), RoundLimit: 1}})
	if executed.Report == nil || executed.Report.Status != contracts.MitigationReport_STATUS_UNRESOLVED {
		t.Fatalf("want failed health report: %v", executed)
	}
	if executed.ApplicationID == "" {
		t.Fatal("fully applied menu option omitted rollback application ID")
	}
	ports, _, _ := unstructuredPorts(cluster.objects[menuService].Object)
	if ports[0].(map[string]any)["targetPort"] != float64(8080) {
		t.Fatal("failed verification automatically undid the option")
	}
	rolled := loop.Handle(context.Background(), ToolRequest{Command: "rollback", ApplicationID: executed.ApplicationID})
	if rolled.Error != "" {
		t.Fatal(rolled.Error)
	}
	ports, _, _ = unstructuredPorts(cluster.objects[menuService].Object)
	if ports[0].(map[string]any)["targetPort"] != float64(9999) {
		t.Fatal("explicit rollback did not restore the pre-image")
	}
	for _, receipt := range loop.MenuRepairActions() {
		if receipt.Success {
			t.Fatal("rolled-back menu action retained success credit")
		}
	}
}

func TestResponderMenuSessionAndReportsSurviveToolRestore(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	var saved []byte
	loop.saveToolState = func(_ context.Context, payload []byte) error { saved = append([]byte(nil), payload...); return nil }
	listing := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	execution := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: plainProgramOption(t, listing.Menu, 1), RoundLimit: 1}})
	replacement := NewResponderLoop(loop.Request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, 5)
	replacement.MenuConfig = productionResponderMenu()
	if err := replacement.restoreToolSnapshot(saved); err != nil {
		t.Fatal(err)
	}
	opened := replacement.Handle(context.Background(), ToolRequest{Command: "session-open"})
	if opened.Session.SessionId != listing.Menu.SessionId || replacement.LoopsUsed() != 1 {
		t.Fatal("controller restart changed the session or reset rounds")
	}
	report := replacement.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: execution.Report.AttemptId}})
	if report.Report == nil || len(report.MenuExecution) != 1 || report.MenuExecution[0].Plan == nil || len(replacement.MenuRepairActions()) != 1 {
		t.Fatal("controller restart lost report or repair evidence")
	}
}
