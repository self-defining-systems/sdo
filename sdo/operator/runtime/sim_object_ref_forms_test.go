package runtime

import (
	"encoding/json"
	"fmt"
	"testing"

	"sdo.dev/operator/sdk"
)

// SDO finding: a binding copied from coverage must survive menu filtering and
// reach the ordinary learned-program executor without an argument-repair turn.
func TestSimSchedulePrintedObjectReferenceSurvivesMenuFilters(t *testing.T) {
	sweepSimSeedsN(t, 8, func(t *testing.T, seed uint64) {
		rng := newSimRNG(seed)
		loop, cluster := menuB3Loop(t)
		loop.Request.RepairPolicy = "recorded-actions"
		loop.Executor.Verifier = PlanHealthVerifier{Provider: cluster, Detectors: []sdk.Detector{productionMenuHealth{}}}
		menu := menuCall(t, loop, map[string]any{"command": "menu", "program_id": "routing-then-restart"})
		request := menuSelection(t, menu["menu"].(map[string]any), "routing-then-restart", 2, fmt.Sprintf("copied-ref-%d", seed))
		selection := request["selection"].(map[string]any)
		if selection["bindings"] == nil {
			delete(selection, "bindings")
		}
		// The same role binding is supplied as structured JSON, JSON text or CLI text.
		parameters := map[string]any{}
		for _, role := range []string{"service", "deployment"} {
			ref := map[string]sdk.ObjectRef{"service": menuService, "deployment": menuDeployment}[role]
			encoded, _ := json.Marshal(ref)
			switch rng.IntN(3) {
			case 0:
				parameters[role] = ref
			case 1:
				parameters[role] = string(encoded)
			case 2:
				parameters[role] = ref.Kind + "/" + ref.Namespace + "/" + ref.Name
			}
		}
		request["parameters"] = parameters
		if rng.IntN(2) == 0 {
			menuCall(t, loop, map[string]any{"command": "menu", "program_id": "restart-only"})
		}
		response := menuCall(t, loop, request)
		report, ok := response["report"].(map[string]any)
		if !ok || report["status"] != "STATUS_SUCCESS" {
			t.Fatalf("copied binding cost a correction turn: %+v", response)
		}
		port, _ := testField(cluster.objects[menuService], "spec.ports.0.targetPort")
		if port != float64(8080) || cluster.forbiddenReads != 0 {
			t.Fatalf("repair invariant: port=%v forbidden=%d", port, cluster.forbiddenReads)
		}
	})
}
