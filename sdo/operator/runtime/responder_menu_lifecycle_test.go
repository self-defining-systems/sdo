package runtime

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type menuLifecycleCluster struct{ *lifecycleTestClient }

func (c menuLifecycleCluster) ListCurrent(_ context.Context, kind sdk.WatchKind) (CurrentObjectList, error) {
	result := CurrentObjectList{Complete: true, ResourceVersion: "synthetic-current-list"}
	for ref, object := range c.objects {
		if ref.Kind == kind.Kind && ref.APIVersion == kind.APIVersion && ref.Namespace == kind.Namespace {
			result.Items = append(result.Items, *object.DeepCopy())
		}
	}
	return result, nil
}

type menuLifecycleDiagnoser struct{ operation string }

func (p menuLifecycleDiagnoser) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "object-option"}
}
func (p menuLifecycleDiagnoser) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	matched := false
	if p.operation == "create" {
		matched = len(view.Services()) == 0
	} else {
		matched = len(view.NetworkPolicies()) > 0
	}
	if matched {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func menuLifecycleLoop(t *testing.T, operation string) (*ResponderLoop, *lifecycleTestClient) {
	t.Helper()
	ref := menuService
	object := planObject(ref, map[string]any{"selector": map[string]any{"app": "synthetic"}, "ports": []any{map[string]any{"port": float64(80), "targetPort": float64(8080)}}})
	action := program.Action(program.CreateFromSource{Ref: ref, Reason: "restore independently approved missing service"})
	objects := map[sdk.ObjectRef]*unstructured.Unstructured{}
	source := lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}
	if operation == "delete" {
		ref = sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "synthetic-temporary"}
		object = planObject(ref, map[string]any{"podSelector": map[string]any{"matchLabels": map[string]any{"app": "synthetic"}}})
		object.SetUID("fixture-uid")
		object.SetResourceVersion("1")
		objects[ref] = object
		action = program.DeleteObject{Ref: ref, Reason: "remove independently approved source-absent test object"}
		source = lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}
	}
	client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: objects}}
	safety := fixturePlanSafety(t, ref, object)
	proof := safety.Objects[ref]
	proof.CreateAllowed = operation == "create"
	proof.DeleteAllowed = operation == "delete"
	if operation == "create" {
		proof.UID = ""
	}
	safety.Objects[ref] = proof
	registry, err := program.NewRegistry([]program.Diagnoser{menuLifecycleDiagnoser{operation}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := IncidentRequest{IncidentID: "synthetic-object-menu", Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions", ResponseDeadline: time.Now().Add(10 * time.Minute)}
	loop := NewResponderLoop(request, registry, nil, nil, PlanExecutor{Client: client, Source: source, Safety: safety, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}, 0)
	loop.selectionStore = &selectionTestStore{}
	loop.MenuConfig = &ResponderMenuConfig{Reader: menuLifecycleCluster{client}, Dependencies: []sdk.WatchKind{{APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace}}, Plans: []ResponderMenuPlan{{ID: operation, ProgramID: "object-option", Plan: program.Plan{Actions: []program.Action{action}}}}, Safety: &ResponderMenuSafety{Digest: "synthetic-object-policy", AuthorizeRead: func(_ context.Context, k sdk.WatchKind) error {
		if k.Kind != ref.Kind || k.Namespace != ref.Namespace {
			return errMenuSafety
		}
		return nil
	}, AuthorizeObject: func(_ context.Context, got sdk.ObjectRef, object *unstructured.Unstructured) error {
		if got != ref {
			return errMenuSafety
		}
		approved := object.DeepCopy()
		if operation == "create" {
			approved.SetUID("")
		}
		return safety.checkObject(approved, got)
	}, AuthorizeEffect: func(context.Context, sdk.ObjectRef, *unstructured.Unstructured, *unstructured.Unstructured) error {
		return nil
	}}, Healthy: func(_ context.Context, view sdk.DetectionContext) (bool, error) {
		if operation == "create" {
			return len(view.Services()) == 1, nil
		}
		return len(view.NetworkPolicies()) == 0, nil
	}}
	opened := loop.Handle(context.Background(), ToolRequest{Command: "session-open"})
	if opened.Error != "" {
		t.Fatal(opened.Error)
	}
	if err := loop.selection.checkpoint(context.Background(), "codex", "synthetic-object-session", "synthetic-object-checkpoint"); err != nil {
		t.Fatal(err)
	}
	return loop, client
}
func TestResponderMenuExecutesXV1LifecycleOptions(t *testing.T) {
	for _, operation := range []string{"create", "delete"} {
		t.Run(operation, func(t *testing.T) {
			loop, client := menuLifecycleLoop(t, operation)
			response := menuCall(t, loop, map[string]any{"command": "menu"})
			menu, ok := response["menu"].(map[string]any)
			if !ok {
				t.Fatalf("missing object-action menu: %+v", response)
			}
			result := menuCall(t, loop, menuSelection(t, menu, "object-option", 1, "object-effect"))
			report, ok := result["report"].(map[string]any)
			if !ok || report["status"] != "STATUS_SUCCESS" {
				t.Fatalf("existing executor lifecycle path unavailable: %+v", result)
			}
			if (operation == "create" && client.creates != 1) || (operation == "delete" && client.deletes != 1) {
				t.Fatal("effect did not execute exactly once")
			}
			round := report["rounds"].([]any)[0].(map[string]any)
			action := round["actions"].([]any)[0].(map[string]any)
			if action["action_kind"] != operation || action["state"] != "STATE_APPLIED" {
				t.Fatal("typed lifecycle action receipt missing")
			}
			if len(loop.PlanApplications()) != 0 {
				t.Fatal("lifecycle menu option entered legacy trust path")
			}
		})
	}
}
func TestResponderMenuDoesNotRequireUnobservedOpaqueKinds(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.MenuConfig.Dependencies = loop.MenuConfig.Dependencies[:2]
	response := menuCall(t, loop, map[string]any{"command": "menu"})
	menu := response["menu"].(map[string]any)
	result := menuCall(t, loop, menuSelection(t, menu, "routing-then-restart", 2, "bounded-read-scope"))
	report, ok := result["report"].(map[string]any)
	if !ok || report["status"] != "STATUS_SUCCESS" {
		t.Fatalf("menu unnecessarily read an unobserved opaque kind: %+v", result)
	}
}
func TestResponderMenuRechecksSourceBeforeCompensation(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	source := loop.Request.SourceCommit
	loop.Executor.AcceptedRevision = func() string { return source }
	response := menuCall(t, loop, map[string]any{"command": "menu"})
	menu := response["menu"].(map[string]any)
	cluster.afterPatch = func(sdk.ObjectRef) { source = "superseding-source" }
	result := menuCall(t, loop, menuSelection(t, menu, "routing-then-restart", 1, "source-fence"))
	if result["report"] == nil || len(cluster.patches) != 1 {
		t.Fatal("compensation crossed accepted-source fence")
	}
}

func TestResponderMenuPolicyRotationFencesAllRemainingWrites(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	response := menuCall(t, loop, map[string]any{"command": "menu"})
	menu := response["menu"].(map[string]any)
	cluster.afterPatch = func(sdk.ObjectRef) { loop.MenuConfig.Safety.Digest = "rotated-policy" }
	result := menuCall(t, loop, menuSelection(t, menu, "routing-then-restart", 2, "policy-fence"))
	report, ok := result["report"].(map[string]any)
	if !ok || report["status"] == "STATUS_SUCCESS" || len(cluster.patches) != 1 {
		t.Fatal("remaining forward/compensation writes crossed a menu policy fence")
	}
}
