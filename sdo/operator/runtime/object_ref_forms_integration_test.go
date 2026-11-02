//go:build integration

package runtime

import (
	"encoding/json"
	"strings"
	"testing"


)

// Hand-written role fixtures exercise the public request decoder and real menu
// plan codec; no API, cluster or model is needed to detect a reference mismatch.
func TestResponderObjectReferenceForms(t *testing.T) {
	for _, raw := range []string{`Service/app/backend`, `svc/app/backend`, `{"api_version":"v1","kind":"Service","namespace":"app","name":"backend"}`, `{"kind":"svc","namespace":"app","name":"backend"}`} {
		t.Run(raw, func(t *testing.T) {
			loop, _ := menuB3Loop(t)
			loop.Request.RepairPolicy = "recorded-actions"
			loop.Executor.Verifier = planFakeVerifier{true}
			menuCall(t, loop, map[string]any{"command": "menu"})
			response := menuCall(t, loop, map[string]any{"command": "mitigate", "selection": map[string]any{"version": 1, "option_id": "SetField", "round_limit": 1}, "parameters": map[string]any{"ref": raw, "path": "spec.ports[0].targetPort", "value": 8082, "reason": "restore route"}})
			if response["error"] != nil || response["report"] == nil || response["targeted_healthy"] != true {
				t.Fatalf("reference did not reach normal execution: %+v", response)
			}
		})
	}
}

func TestMenuRejectsAmbiguousObjectBeforeWrites(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	menuCall(t, loop, map[string]any{"command": "menu"})
	before, _ := json.Marshal(cluster.objects[menuService])
	response := menuCall(t, loop, map[string]any{"command": "mitigate", "selection": map[string]any{"version": 1, "option_id": "SetField", "round_limit": 1}, "parameters": map[string]any{"ref": `{"kind":"Service","namespace":"app","name":"backend","names":["peer"]}`, "path": "spec.ports[0].targetPort", "value": 8082}})
	after, _ := json.Marshal(cluster.objects[menuService])
	encoded, _ := json.Marshal(response)
	if !strings.Contains(string(encoded), "Kind/name") || !strings.Contains(string(encoded), "Service/app/backend") || string(before) != string(after) {
		t.Fatalf("ambiguous target must fail before writes: %+v", response)
	}
}

