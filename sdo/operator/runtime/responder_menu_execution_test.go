package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
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
	approvedPodSpec, _, _ := unstructured.NestedMap(cluster.objects[menuDeployment].Object, "spec", "template", "spec")
	loop.MenuConfig = &ResponderMenuConfig{Reader: cluster, Dependencies: deps, Safety: &ResponderMenuSafety{Digest: "synthetic-policy", AuthorizeRead: func(_ context.Context, k sdk.WatchKind) error {
		if k.Kind == "Secret" {
			return errMenuSafety
		}
		return nil
	}, AuthorizeObject: func(_ context.Context, ref sdk.ObjectRef, obj *unstructured.Unstructured) error {
		if ref == menuDeployment {
			podSpec, _, _ := unstructured.NestedMap(obj.Object, "spec", "template", "spec")
			if !reflect.DeepEqual(approvedPodSpec, podSpec) {
				return errMenuSafety
			}
		}
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

func TestResponderMenuConfiguredCapSurvivesAlternateOptions(t *testing.T) {
	template, cluster := menuB3Loop(t)
	loop := NewResponderLoop(template.Request, template.Programs, cluster, nil, template.Executor, 3)
	loop.MenuConfig = template.MenuConfig
	loop.selectionStore = &selectionTestStore{}
	opened := loop.Handle(context.Background(), ToolRequest{Command: "session-open"})
	if opened.Error != "" {
		t.Fatal(opened.Error)
	}
	if err := loop.selection.checkpoint(context.Background(), "codex", "synthetic-session", "synthetic-checkpoint"); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 3; round++ {
		response := menuCall(t, loop, map[string]any{"command": "menu"})
		menu := response["menu"].(map[string]any)
		if menu["round_limit"] != float64(3-round) {
			t.Fatal("remaining allowance did not follow the single incident cap")
		}
		result := menuCall(t, loop, menuSelection(t, menu, "restart-only", 1, fmt.Sprintf("bounded-%d", round)))
		if result["report"] == nil {
			t.Fatalf("missing report: %+v", result)
		}
	}
	writes := len(cluster.patches)
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	if response.Menu.RemainingBatches != 0 || len(response.Menu.Options) == 0 || response.Menu.Options[0].DisabledReason != "incident_budget_exhausted" || len(cluster.patches) != writes {
		t.Fatal("alternate options reset configured cap")
	}
}
func TestResponderMenuActLeaseExcludesOtherMutations(t *testing.T) {
	loop, _ := menuB3Loop(t)
	acquired := loop.Handle(context.Background(), ToolRequest{Command: "act-start"})
	if acquired.Error != "" || acquired.MutationToken == "" {
		t.Fatal("missing mutation owner")
	}
	blocked := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{"unused"}})
	if blocked.Error != "recorded mutation still in progress" {
		t.Fatal("parallel mutation escaped external owner")
	}
	if result := loop.Handle(context.Background(), ToolRequest{Command: "act-finish", MutationToken: acquired.MutationToken}); result.Error != "" {
		t.Fatal(result.Error)
	}
	if loop.LoopsUsed() != 1 {
		t.Fatal("recorded fallback did not share incident allowance")
	}
}

func TestResponderMenuRecoveryCapturesCurrentReadOnlyEvidence(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	response := menuCall(t, loop, map[string]any{"command": "menu"})
	payload, _ := json.Marshal(menuSelection(t, response["menu"].(map[string]any), "routing-then-restart", 3, "recovery-observation"))
	var request ToolRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		t.Fatal(err)
	}
	attempt, _, err := loop.selection.accept(context.Background(), loop.menu, request.Selection)
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := loop.selectionStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	replacement := NewResponderLoop(loop.Request, loop.Programs, cluster, nil, loop.Executor, 99)
	replacement.MenuConfig = loop.MenuConfig
	if err := replacement.attachSelectionJournal(context.Background(), loop.Request, loop.selectionStore, state, false); err != nil {
		t.Fatal(err)
	}
	restored, _ := selectionAttempt(replacement.selection.snapshot(), attempt.AttemptId)
	if restored.Report == nil || restored.Report.Status != contracts.MitigationReport_STATUS_INTERRUPTED {
		t.Fatal("recovery did not visibly settle interruption")
	}
	found := false
	for _, barrier := range restored.Report.Barriers {
		if barrier.Phase == "recovery" && barrier.Complete {
			found = true
		}
	}
	if !found || len(cluster.patches) != 0 {
		t.Fatal("recovery omitted direct observation or replayed mutations")
	}
}
