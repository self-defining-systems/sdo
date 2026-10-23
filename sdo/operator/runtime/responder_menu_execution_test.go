package runtime

import (
	"context"
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Test-only adapters provide genuine CAS/checkpoint/provenance edges. The B3
// assertions and negative fixtures themselves are deliberately unchanged.
func configureSyntheticMenu(t *testing.T, loop *ResponderLoop, cluster *menuB3Cluster) {
	t.Helper()
	loop.Request.RepairPolicy = "recorded-actions"
	loop.selectionStore = &selectionTestStore{}
	deps := []sdk.WatchKind{}
	for _, kind := range []struct{ version, kind string }{{"v1", "Service"}, {"apps/v1", "Deployment"}, {"v1", "ConfigMap"}, {"v1", "Pod"}, {"apps/v1", "ReplicaSet"}, {"v1", "Endpoints"}, {"discovery.k8s.io/v1", "EndpointSlice"}, {"networking.k8s.io/v1", "NetworkPolicy"}, {"v1", "Event"}, {"apps/v1", "StatefulSet"}, {"autoscaling/v2", "HorizontalPodAutoscaler"}, {"policy/v1", "PodDisruptionBudget"}} {
		deps = append(deps, sdk.WatchKind{APIVersion: kind.version, Kind: kind.kind, Namespace: "app"})
	}
	loop.MenuConfig = &ResponderMenuConfig{Reader: cluster, Dependencies: deps, Safety: &ResponderMenuSafety{Digest: "synthetic-policy", AuthorizeRead: func(_ context.Context, k sdk.WatchKind) error {
		if k.Kind == "Secret" {
			return errMenuSafety
		}
		return nil
	}, AuthorizeObject: func(_ context.Context, ref sdk.ObjectRef, obj *unstructured.Unstructured) error {
		if ref != menuService && ref != menuDeployment {
			return errMenuSafety
		}
		return nil
	}, AuthorizeEffect: func(context.Context, sdk.ObjectRef, *unstructured.Unstructured, *unstructured.Unstructured) error {
		return nil
	}}, Healthy: func(_ context.Context, view sdk.DetectionContext) (bool, error) {
		return len(view.Deployments()) == 1 && view.Deployments()[0].Status.ReadyReplicas == 1, nil
	}}
	response := loop.Handle(context.Background(), ToolRequest{Command: "session-open"})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	if err := loop.selection.checkpoint(context.Background(), "codex", "synthetic-provider-session", "synthetic-checkpoint"); err != nil {
		t.Fatal(err)
	}
}
func (c *menuB3Cluster) ListCurrent(ctx context.Context, kind sdk.WatchKind) (CurrentObjectList, error) {
	if kind.Kind == "Secret" {
		c.forbiddenReads++
		return CurrentObjectList{}, errMenuSafety
	}
	// Simulate cluster reconciliation on each fresh read, not informer progress.
	if _, err := c.Snapshot(ctx); err != nil {
		return CurrentObjectList{}, err
	}
	result := CurrentObjectList{Complete: true, ResourceVersion: fmt.Sprintf("list-%d", c.reads)}
	for ref, obj := range c.objects {
		if ref.Kind == kind.Kind && ref.APIVersion == kind.APIVersion {
			result.Items = append(result.Items, *obj.DeepCopy())
		}
	}
	return result, nil
}
func TestResponderMenuRequiresSafetyCapabilitiesBeforeReading(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.MenuConfig.Safety.AuthorizeObject = nil
	before := cluster.reads
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	if response.Error == "" || response.Menu != nil || cluster.reads != before {
		t.Fatal("missing capabilities exposed or read menu content")
	}
}
func TestResponderMenuOffersControllerSDKPlan(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.MenuConfig.Plans = []ResponderMenuPlan{{ID: "static-route", ProgramID: "routing-then-restart", Bindings: map[string]sdk.ObjectRef{"service": menuService, "deployment": menuDeployment}, Plan: program.Plan{Actions: []program.Action{program.SetField{Ref: menuService, Path: "spec.ports[0].targetPort", Value: 8080}}}}}
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	found := false
	for _, option := range response.Menu.Options {
		if option.OptionId == "static-route" {
			found = true
			if len(option.Effects) != 1 || option.Effects[0] != contracts.MitigationOption_EFFECT_SDK_PLAN_ACTION {
				t.Fatal("SDK plan needed vocabulary-specific menu support")
			}
		}
	}
	if !found {
		t.Fatal("controller SDK plan missing")
	}
}

func TestResponderMenuConfiguredIncidentRoundCap(t *testing.T) {
	loop, _ := menuB3Loop(t)
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	if response.Menu == nil || response.Menu.RoundLimit != 5 {
		t.Fatalf("default incident cap must be five: %+v", response.Menu)
	}
}
