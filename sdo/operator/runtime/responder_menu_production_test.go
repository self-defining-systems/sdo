package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestProductionResponderMenuMissingEvidenceRefusesBeforeRead(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	client := fake.NewSimpleClientset()
	policy := fixturePlanSafety(t, menuService, cluster.objects[menuService])
	policy.SourceCommit = loop.Request.SourceCommit
	proof := policy.Objects[menuService]
	proof.UID = string(cluster.objects[menuService].GetUID())
	policy.Objects[menuService] = proof
	loop.MenuConfig = productionResponderMenu(loop.Request, client, nil, policy, nil)
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	if response.Error != "" || response.Menu == nil || len(response.Menu.Options) == 0 {
		t.Fatalf("launch must expose a refusal menu: %s %+v", response.Error, response.Menu)
	}
	for _, option := range response.Menu.Options {
		if option.DisabledReason != "collection_consumer_admission_provenance_unavailable" {
			t.Fatalf("missing evidence hidden: %s", option.DisabledReason)
		}
	}
	if len(client.Actions()) != 0 {
		t.Fatal("missing collection provenance fetched objects")
	}
	if _, ok := loop.MenuConfig.Reader.(KubernetesCurrentReader); !ok {
		t.Fatal("production does not reuse direct reader")
	}
}

func TestProductionResponderMenuReusesSourceBoundPolicy(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	object := cluster.objects[menuService]
	policy := fixturePlanSafety(t, menuService, object)
	policy.SourceCommit = loop.Request.SourceCommit
	proof := policy.Objects[menuService]
	proof.UID = string(cluster.objects[menuService].GetUID())
	policy.Objects[menuService] = proof
	evidence := &ResponderMenuSafety{Digest: "independently-reviewed-closure", AuthorizeRead: func(context.Context, sdk.WatchKind) error { return nil }, AuthorizeObject: func(context.Context, sdk.ObjectRef, *unstructured.Unstructured) error { return nil }, AuthorizeEffect: func(context.Context, sdk.ObjectRef, *unstructured.Unstructured, *unstructured.Unstructured) error {
		return nil
	}}
	config := productionResponderMenu(loop.Request, fake.NewSimpleClientset(), nil, policy, evidence)
	ctx := context.Background()
	if err := config.Safety.AuthorizeObject(ctx, menuService, object); err != nil {
		t.Fatal(err)
	}
	changed := object.DeepCopy()
	changed.SetUID("replacement")
	if config.Safety.AuthorizeObject(ctx, menuService, changed) == nil {
		t.Fatal("UID replacement authorized")
	}
	if config.Safety.AuthorizeEffect(ctx, menuService, object, nil) == nil {
		t.Fatal("unapproved deletion authorized")
	}
	if config.Safety.AuthorizeEffect(ctx, menuService, nil, object) == nil {
		t.Fatal("unapproved creation authorized")
	}
	policy.SourceCommit = "another-accepted-source"
	if config.Safety.AuthorizeRead(ctx, sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}) == nil {
		t.Fatal("source-bound authorization widened")
	}
	policy.SourceCommit = loop.Request.SourceCommit
	policy.ExpiresAt = time.Now().Add(-time.Second)
	if config.Safety.AuthorizeObject(ctx, menuService, object) == nil {
		t.Fatal("expired authorization used")
	}
}

func TestRunRejectsInvalidMenuLaunchInput(t *testing.T) {
	t.Setenv("SDO_RESPONDER_MENU", "yes")
	err := RunWithOptions(context.Background(), nil, RuntimeOptions{Args: []string{}})
	if err == nil || !strings.Contains(err.Error(), "SDO_RESPONDER_MENU") {
		t.Fatalf("invalid explicit input silently ignored: %v", err)
	}
}

func TestResponderMenuPreflightsWholeClosureBeforeFirstRead(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.MenuConfig.Dependencies = []sdk.WatchKind{{APIVersion: "v1", Kind: "Service", Namespace: "app"}, {APIVersion: "v1", Kind: "ConfigMap", Namespace: "app"}}
	loop.MenuConfig.Safety.AuthorizeRead = func(_ context.Context, kind sdk.WatchKind) error {
		if kind.Kind == "ConfigMap" {
			return errMenuSafety
		}
		return nil
	}
	reads := 0
	loop.MenuConfig.Reader = &causalTestReader{lists: map[sdk.WatchKind]CurrentObjectList{loop.MenuConfig.Dependencies[0]: {Items: []unstructured.Unstructured{*cluster.objects[menuService]}, ResourceVersion: "current", Complete: true}}, afterRead: func() { reads++ }}
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	if response.Error == "" || reads != 0 {
		t.Fatal("incomplete dependency authorization fetched the first collection")
	}
}

func TestResponderMenuPreflightsAllEffectsBeforeFirstWrite(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.MenuConfig.Plans = []ResponderMenuPlan{{ID: "requires-missing-proof", ProgramID: "routing-then-restart", Bindings: map[string]sdk.ObjectRef{"service": menuService, "deployment": menuDeployment}, Plan: program.Plan{Actions: []program.Action{program.SetField{Ref: menuService, Path: "spec.ports[0].targetPort", Value: 8080}, program.RestartRollout{Ref: menuDeployment}}}}}
	loop.MenuConfig.Safety.AuthorizeEffect = func(_ context.Context, ref sdk.ObjectRef, _, _ *unstructured.Unstructured) error {
		if ref == menuDeployment {
			return errMenuSafety
		}
		return nil
	}
	response := menuCall(t, loop, map[string]any{"command": "menu"})
	menu := response["menu"].(map[string]any)
	for _, option := range menu["options"].([]any) {
		if option.(map[string]any)["option_id"] == "requires-missing-proof" {
			menu["options"] = []any{option}
			break
		}
	}
	report := menuCall(t, loop, menuSelection(t, menu, "routing-then-restart", 1, "missing-second-effect"))
	if len(cluster.patches) != 0 {
		t.Fatal("missing second-action evidence allowed the first write")
	}
	if report["report"].(map[string]any)["reason_code"] != "plan_preflight_refused" {
		t.Fatal("missing effect evidence did not refuse in preflight")
	}
}

func TestProductionResponderMenuRefusesUnpinnedCreatedIdentity(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	object := cluster.objects[menuService].DeepCopy()
	object.SetUID("")
	policy := fixturePlanSafety(t, menuService, object)
	policy.SourceCommit = loop.Request.SourceCommit
	proof := policy.Objects[menuService]
	proof.UID = ""
	proof.CreateAllowed = true
	policy.Objects[menuService] = proof
	evidence := &ResponderMenuSafety{Digest: "reviewed-closure", AuthorizeRead: func(context.Context, sdk.WatchKind) error { return nil }, AuthorizeObject: func(context.Context, sdk.ObjectRef, *unstructured.Unstructured) error { return nil }, AuthorizeEffect: func(context.Context, sdk.ObjectRef, *unstructured.Unstructured, *unstructured.Unstructured) error {
		return nil
	}}
	config := productionResponderMenu(loop.Request, fake.NewSimpleClientset(), nil, policy, evidence)
	if config.Safety.AuthorizeEffect(context.Background(), menuService, nil, object) == nil {
		t.Fatal("create admitted without a source for post-create UID provenance")
	}
}
