package runtime

import (
	"context"
	"os"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// An already accepted/imported registry entry needs no incident corpus to
// derive authority. Its application identity is the deployed chart's name.
type adoptedFlagProgram struct{}

func (adoptedFlagProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "synthetic-adopted-high-cpu", Params: []program.ParamSpec{{Name: "config", Kind: "ConfigMap", Required: true}}}
}
func (adoptedFlagProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Match}, nil
}
func (adoptedFlagProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	ref := b.Objects["config"]
	return program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[flags.json]", DocumentFormat: "json", DocumentPath: []string{"flags", "highCpu", "defaultVariant"}, Value: program.FromSource(ref, "data[flags.json]")}}}, nil
}

func TestAdoptedFlagPlanUsesReviewedDeployedApplicationIdentity(t *testing.T) {
	raw, err := os.ReadFile("../config/plan-policies/mixed10.json")
	if err != nil {
		t.Fatal(err)
	}
	config, err := LoadSourcePlanPolicy(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	adopted := adoptedFlagProgram{}
	registry, err := program.NewRegistry([]program.Diagnoser{adopted}, []program.Mitigator{adopted})
	if err != nil {
		t.Fatal(err)
	}
	mitigator, ok := registry.Mitigator(adopted.Spec().ID)
	if !ok {
		t.Fatal("adopted registry entry missing")
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "astronomy-shop", Name: "synthetic-flags"}
	accepted := planObject(ref, nil)
	delete(accepted.Object, "spec")
	accepted.Object["data"] = map[string]any{"flags.json": `{"flags":{"highCpu":{"defaultVariant":"off"}},"untouched":true}`}
	source := sourcePolicyFixture{forbidInventory: true, accepted: []*unstructured.Unstructured{accepted}, lifecycleTestSource: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: accepted}}}
	for _, test := range []struct {
		name, app, ns string
		allowed       bool
	}{
		{"adopted chart identity", "opentelemetry-demo", "astronomy-shop", true},
		{"canonical application", "astronomy_shop", "astronomy-shop", true},
		{"control namespace", "opentelemetry-demo", "astronomy-shop-sdo", false},
		{"foreign namespace", "opentelemetry-demo", "foreign", false},
		{"other reviewed application namespace", "opentelemetry-demo", "hotel-res", false},
		{"unknown application", "unreviewed", "astronomy-shop", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			current := accepted.DeepCopy()
			current.SetUID("fixture-uid")
			current.SetResourceVersion("1")
			current.Object["data"] = map[string]any{"flags.json": `{"flags":{"highCpu":{"defaultVariant":"on"}},"untouched":true}`}
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: current}}
			incident := IncidentRequest{IncidentID: "adopted-flags", Application: test.app, Namespace: test.ns, SourceCommit: "accepted", RepairPolicy: "recorded-actions", ResponseDeadline: time.Now().Add(time.Minute)}
			authority := &SourcePlanAuthority{Config: config, Source: source, UID: func(context.Context, sdk.ObjectRef) (types.UID, error) { return "fixture-uid", nil }}
			executor := PlanExecutor{Client: client, Source: source, SourceAuthority: authority, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			plan, err := mitigator.Mitigate(ctx, nil, program.Bindings{Objects: program.ObjectBindings{"config": ref}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.Apply(ctx, incident, plan, false)
			if !test.allowed {
				if err == nil || len(client.patches) > 0 {
					t.Fatalf("out-of-scope plan executed: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("adopted app flag plan refused: %v", err)
			}
			if !result.VerificationPassed || len(client.patches) != 1 || client.objects[ref].Object["data"].(map[string]any)["flags.json"] != accepted.Object["data"].(map[string]any)["flags.json"] {
				t.Fatalf("flag repair did not preserve accepted document: %+v", result)
			}
		})
	}
}

func TestMenuProgramParametersBindOneOptionWithoutFindingCandidates(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.Request.Findings = nil // imported memory with no fresh candidate corpus
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: "routing-then-restart", Parameters: map[string]string{"service": "Service/app/chosen", "deployment": "Deployment/app/frontend", "port": "8081"}})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	count := 0
	for _, option := range response.Menu.Options {
		if option.ProgramId == "routing-then-restart" {
			count++
			for _, binding := range option.Bindings {
				if binding.Name == "service" && binding.Object.GetName() != "chosen" {
					t.Fatal("explicit service binding ignored")
				}
			}
			if len(option.Bindings) != 3 {
				t.Fatalf("explicit program parameters missing: %v", option.Bindings)
			}
		}
	}
	if count != 1 {
		t.Fatalf("program option count=%d", count)
	}
}

func TestReviewedApplicationCannotAuthorizeForeignNamespaceAction(t *testing.T) {
	raw, err := os.ReadFile("../config/plan-policies/mixed10.json")
	if err != nil {
		t.Fatal(err)
	}
	config, err := LoadSourcePlanPolicy(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "foreign", Name: "synthetic-flags"}
	accepted := planObject(ref, nil)
	delete(accepted.Object, "spec")
	accepted.Object["data"] = map[string]any{"flags.json": `{"flags":{"highCpu":{"defaultVariant":"off"}}}`}
	source := sourcePolicyFixture{accepted: []*unstructured.Unstructured{accepted}, lifecycleTestSource: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: accepted}}}
	current := accepted.DeepCopy()
	current.SetUID("foreign-uid")
	current.SetResourceVersion("1")
	current.Object["data"] = map[string]any{"flags.json": `{"flags":{"highCpu":{"defaultVariant":"on"}}}`}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: current}}
	authority := &SourcePlanAuthority{Config: config, Source: source, UID: func(context.Context, sdk.ObjectRef) (types.UID, error) {
		t.Fatal("foreign namespace reached UID reader")
		return "", nil
	}}
	executor := PlanExecutor{Client: client, Source: source, SourceAuthority: authority, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	plan, err := (adoptedFlagProgram{}).Mitigate(context.Background(), nil, program.Bindings{Objects: program.ObjectBindings{"config": ref}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Apply(context.Background(), IncidentRequest{IncidentID: "foreign-plan", Application: "opentelemetry-demo", Namespace: "astronomy-shop", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, plan, false)
	if err == nil || len(client.patches) != 0 {
		t.Fatalf("foreign action authorized: %v", err)
	}
}

func TestMenuProgramParametersRejectUnknownAndIncompleteBindings(t *testing.T) {
	for _, parameters := range []map[string]string{
		{"service": "Service/app/backend"},
		{"service": "Service/app/backend", "deployment": "Deployment/app/frontend", "port": "invalid-int"},
		{"service": "Service/app/backend", "deployment": "Deployment/app/frontend", "unknown": "value"},
	} {
		loop, _ := menuB3Loop(t)
		response := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: "routing-then-restart", Parameters: parameters})
		if response.Error == "" {
			t.Fatalf("invalid parameters created an option: %v", parameters)
		}
		if loop.LoopsUsed() != 0 {
			t.Fatal("menu binding consumed mitigation budget")
		}
	}
}
