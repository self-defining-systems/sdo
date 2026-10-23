package program_test

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestStructuredSetFieldSchema(t *testing.T) {
	cases := []struct {
		kind, api, path string
		good, bad       any
	}{
		{"ConfigMap", "v1", "data[flags.json]", `{"enabled":false}`, false},
		{"Deployment", "apps/v1", "spec.template.spec.containers[name=app].command", []any{"/app"}, "/app"},
		{"Deployment", "apps/v1", "spec.template.spec.containers[name=app].args", []any{"--safe"}, []any{1}},
		{"Deployment", "apps/v1", "spec.template.spec.containers[name=app].env[name=MODE].value", "safe", true},
		{"Deployment", "apps/v1", "spec.template.spec.containers[name=app].volumeMounts", []any{map[string]any{"name": "tmp", "mountPath": "/tmp"}}, []any{map[string]any{"mountPath": 3}}},
		{"Service", "v1", "spec.selector", map[string]any{"app": "api"}, map[string]any{"app": 2}},
		{"Service", "v1", "spec.ports[name=http].targetPort", 8080, true},
		{"NetworkPolicy", "networking.k8s.io/v1", "spec", map[string]any{"podSelector": map[string]any{}}, map[string]any{"unknown": true}},
		{"Deployment", "apps/v1", "spec.template.spec.nodeSelector", map[string]any{"pool": "app"}, map[string]any{"pool": 1}},
		{"Deployment", "apps/v1", "spec.template.spec.tolerations", []any{map[string]any{"key": "app", "operator": "Exists"}}, "app"},
		{"Deployment", "apps/v1", "spec.template.spec.affinity", map[string]any{"podAffinity": map[string]any{}}, map[string]any{"unknown": true}},
		{"Deployment", "apps/v1", "spec.template.spec.serviceAccountName", "app", 2},
		{"HorizontalPodAutoscaler", "autoscaling/v2", "spec.maxReplicas", 5, "5"},
		{"HorizontalPodAutoscaler", "autoscaling/v2", "spec.minReplicas", 1, 1.5},
		{"PodDisruptionBudget", "policy/v1", "spec.minAvailable", 1, true},
		{"PodDisruptionBudget", "policy/v1", "spec.maxUnavailable", 1, map[string]any{}},
	}
	for _, c := range cases {
		t.Run(c.kind+"/"+c.path, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: c.api, Kind: c.kind, Namespace: "app", Name: "app"}
			if err := (program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: c.path, Value: c.good}}}).Validate(); err != nil {
				t.Fatalf("valid value: %v", err)
			}
			if err := (program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: c.path, Value: c.bad}}}).Validate(); err == nil {
				t.Fatal("accepted wrong schema kind")
			}
		})
	}
}

func TestStructuredSetFieldNamedIdentity(t *testing.T) {
	object := map[string]any{"spec": map[string]any{"ports": []any{map[string]any{"name": "other", "targetPort": float64(90)}, map[string]any{"name": "http", "targetPort": float64(9999)}}}}
	if err := program.ApplySetField(object, "spec.ports[name=http].targetPort", float64(8080)); err != nil {
		t.Fatal(err)
	}
	ports := object["spec"].(map[string]any)["ports"].([]any)
	if ports[0].(map[string]any)["targetPort"] != float64(90) || ports[1].(map[string]any)["targetPort"] != float64(8080) {
		t.Fatal("wrong item updated")
	}
	ports = append(ports, ports[1])
	object["spec"].(map[string]any)["ports"] = ports
	if err := program.ApplySetField(object, "spec.ports[name=http].targetPort", float64(80)); err == nil {
		t.Fatal("ambiguous name accepted")
	}
}

func TestApplySetFieldActionDocument(t *testing.T) {
	object := map[string]any{"data": map[string]any{"flags": `{"enabled":true,"unrelated":7}`}}
	action := program.SetField{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap"}, Path: "data[flags]", DocumentFormat: "json", DocumentPath: []string{"enabled"}, Value: false}
	if err := program.ApplySetFieldAction(object, action, nil); err != nil {
		t.Fatal(err)
	}
	if object["data"].(map[string]any)["flags"] != `{"enabled":false,"unrelated":7}` {
		t.Fatal("unrelated flags changed")
	}
	action.Value = "wrong kind"
	if err := program.ApplySetFieldAction(object, action, nil); err == nil {
		t.Fatal("document schema kind mismatch accepted")
	}
}
