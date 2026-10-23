package runtime

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// The production menu must not require evidence beyond the executor policy.
func TestProductionResponderMenuStreamFamilyParity(t *testing.T) {
	for _, family := range []string{"missing_configmap", "wrong_service_selector", "resource_request_too_large", "readiness_probe_misconfiguration"} {
		t.Run(family, func(t *testing.T) {
			loop, cluster := menuB3Loop(t)
			ref := menuService
			before := cluster.objects[ref].DeepCopy()
			after := before.DeepCopy()
			action := program.Action(program.SetField{Ref: ref, Path: "spec.selector", ValueFromSource: true})
			if family == "missing_configmap" {
				ref = sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "missing"}
				after = planObject(ref, nil)
				delete(after.Object, "spec")
				after.SetUID("")
				after.Object["data"] = map[string]any{"mode": "safe-placeholder"}
				action = program.CreateFromSource{Ref: ref, Reason: "restore accepted config"}
			} else if family != "wrong_service_selector" {
				ref = menuDeployment
				before = cluster.objects[ref].DeepCopy()
				after = before.DeepCopy()
				path := "spec.template.spec.containers[0].resources"
				value := any(map[string]any{"requests": map[string]any{"cpu": "100m"}})
				if family == "readiness_probe_misconfiguration" {
					path = "spec.template.spec.containers[0].readinessProbe"
					value = map[string]any{"httpGet": map[string]any{"path": "/ready", "port": float64(8080)}}
				}
				action = program.SetField{Ref: ref, Path: path, Value: value}
				// Legacy numeric/string workload fields use the executor's universal delta.
			}
			policy := fixturePlanSafety(t, ref, before, after)
			policy.SourceCommit = loop.Request.SourceCommit
			proof := policy.Objects[ref]
			proof.UID = string(before.GetUID())
			proof.CreateAllowed = family == "missing_configmap"
			policy.Objects[ref] = proof
			objects := map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}
			if family == "missing_configmap" {
				delete(objects, ref)
			}
			client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: objects}}
			executor := PlanExecutor{Client: client, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: after}}, Safety: policy, AcceptedRevision: func() string { return loop.Request.SourceCommit }}
			if _, err := executor.Apply(context.Background(), loop.Request, program.Plan{Actions: []program.Action{action}}, true); err != nil {
				t.Fatalf("direct refused fixture: %v", err)
			}
			loop.MenuConfig = productionResponderMenu()
			loop.Executor = executor
			loop.MenuConfig.Plans = []ResponderMenuPlan{{ID: family, ProgramID: family, Plan: program.Plan{Actions: []program.Action{action}}}}
			response := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
			if response.Error != "" || response.Menu == nil {
				t.Fatalf("direct accepted %s; menu failed: %s", family, response.Error)
			}
			found := false
			for _, option := range response.Menu.Options {
				if option.OptionId == family {
					found = true
					if option.DisabledReason != "" {
						t.Fatal("menu imposed an execution precondition")
					}
				}
			}
			if !found {
				t.Fatal("plan missing from menu")
			}

		})
	}
}
