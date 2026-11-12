package operatorcli

import (
	"testing"

	"sdo.dev/operator/kubeproxy"
	"sdo.dev/operator/sdk"
)

func TestProxyScopeParsing(t *testing.T) {
	scope, err := parseProxyScope("incident:inc-1")
	if err != nil || scope.Kind != "incident" || scope.ID != "inc-1" {
		t.Fatalf("incident scope = %+v, %v", scope, err)
	}
	scope, err = parseProxyScope("task:agent-task-7")
	if err != nil || scope.Kind != "task" || scope.ID != "agent-task-7" {
		t.Fatalf("task scope = %+v, %v", scope, err)
	}
	for _, bad := range []string{"", "incident:", "task:", "responder:x", "inc-1"} {
		if _, err := parseProxyScope(bad); err == nil {
			t.Errorf("scope %q accepted", bad)
		}
	}
}

func TestOnlyTheIncidentScopeCanWriteAndItsWritesAreForcedToTheResponderManager(t *testing.T) {
	incident := proxyScope{Kind: "incident", ID: "inc-1"}.config(nil, nil)
	if incident.ReadOnly || incident.FieldManager != sdk.ResponderFieldManager {
		t.Fatalf("incident config = %+v", incident)
	}
	task := proxyScope{Kind: "task", ID: "k"}.config(nil, nil)
	if !task.ReadOnly || task.FieldManager != "" {
		t.Fatalf("task config must be read-only: %+v", task)
	}
}

func TestAnUnopenableStoreRefusesWritesInsteadOfForwardingThemUnrecorded(t *testing.T) {
	var recorder kubeproxy.Recorder = unavailableRecorder{}
	admitter, ok := recorder.(kubeproxy.Admitter)
	if !ok || admitter.Admit() == nil {
		t.Fatal("an unopenable store must fail admission")
	}
}
