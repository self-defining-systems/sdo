package runtime

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func advertisedProgramOption(t *testing.T, loop *ResponderLoop, id string) string {
	t.Helper()
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: id})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	for _, option := range response.Menu.GetOptions() {
		if option.ProgramId == id {
			return option.OptionId
		}
	}
	t.Fatalf("program %s was not advertised", id)
	return ""
}

func TestMenuCatalogRetainsFilteredOptionsForCombinedMitigation(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	first := advertisedProgramOption(t, loop, "routing-then-restart")
	second := advertisedProgramOption(t, loop, "restart-only")
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", SelectionSet: &contracts.MitigationSelectionSet{Version: 1, Options: []*contracts.MitigationOptionSelection{{OptionId: first}, {OptionId: second}}}})
	if response.Error != "" || len(response.OptionReports) != 2 || len(cluster.patches) != 2 {
		t.Fatalf("advertised options failed on first application: %+v; patches=%d", response, len(cluster.patches))
	}
}

func TestMenuCatalogRefusesChangedProgramTargetBeforeAnyBatchWrite(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	second := advertisedProgramOption(t, loop, "routing-then-restart")
	// Both live preconditions have cleared since advertisement. The program's
	// old option must still resolve, but no longer authorizes a repair.
	ports := []any{map[string]any{"port": int64(80), "targetPort": int64(8080)}}
	if err := unstructured.SetNestedSlice(cluster.objects[menuService].Object, ports, "spec", "ports"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(cluster.objects[menuDeployment].Object, int64(1), "status", "readyReplicas"); err != nil {
		t.Fatal(err)
	}
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", SelectionSet: &contracts.MitigationSelectionSet{Version: 1, Options: []*contracts.MitigationOptionSelection{
		{OptionId: "SetField", Parameters: map[string]string{"ref": "Service/app/backend", "path": "spec.selector.app", "value": `"changed"`, "reason": "independent valid action"}},
		{OptionId: second},
	}}})
	if response.Report.GetStatus() != contracts.MitigationReport_STATUS_REFUSED || response.Report.GetReasonCode() != "direct_plan_failed" || !strings.Contains(response.Error, "no longer matches live state") || len(cluster.patches) != 0 {
		t.Fatalf("changed preconditions were not refused before writes: %+v; patches=%d", response, len(cluster.patches))
	}
}
