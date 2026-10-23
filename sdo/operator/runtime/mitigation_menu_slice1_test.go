package runtime

// Executable MI1 acceptance assertions. Synthetic durable/provenance adapters
// wire the real menu interfaces; an unknown command is not a safety refusal.
// All workloads, images and canaries in this file are synthetic.
import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

var menuService = sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "backend"}
var menuDeployment = sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "frontend"}

type menuB3Program struct{ id string }

func (p menuB3Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: p.id, Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "deployment", Kind: "Deployment", Required: true}, {Name: "port", ValueKind: program.IntValue, Default: 8080}}}
}
func (p menuB3Program) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	if len(view.Services()) != 1 || len(view.Deployments()) != 1 {
		return program.Verdict{Result: program.Unknown}, nil
	}
	if view.Services()[0].Spec.Ports[0].TargetPort.IntVal != 8080 || view.Deployments()[0].Status.ReadyReplicas != 1 {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (p menuB3Program) Mitigate(_ context.Context, view program.ReadView, b program.Bindings) (program.Plan, error) {
	if p.id == "routing-then-restart" && view.Services()[0].Spec.Ports[0].TargetPort.IntVal != 8080 {
		return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["service"], Path: "spec.ports[0].targetPort", Value: program.FromParam("port"), Reason: "synthetic routing repair"}}}, nil
	}
	return program.Plan{Actions: []program.Action{program.RestartRollout{Ref: b.Objects["deployment"], Reason: "bootstrap after observed backend readiness"}}}, nil
}

type menuB3Cluster struct {
	planFakeClient
	observedReady         bool
	reads, forbiddenReads int
	trace                 []string
	afterPatch            func(sdk.ObjectRef)
}

func (c *menuB3Cluster) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Kind == "Secret" || ref.Kind == "ConfigMap" {
		c.forbiddenReads++
		return nil, fmt.Errorf("opaque content must never be fetched")
	}
	c.reads++
	return c.planFakeClient.Get(ctx, ref)
}
func (c *menuB3Cluster) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, data []byte) error {
	if err := c.planFakeClient.Patch(ctx, ref, kind, data); err != nil {
		return err
	}
	if ref == menuService {
		c.trace = append(c.trace, "route")
	}
	if ref == menuDeployment {
		c.trace = append(c.trace, "restart")
		if c.observedReady {
			_ = unstructured.SetNestedField(c.objects[ref].Object, int64(1), "status", "readyReplicas")
		}
	}
	if c.afterPatch != nil {
		c.afterPatch(ref)
	}
	return nil
}
func (c *menuB3Cluster) Snapshot(context.Context) (sdk.DetectionContext, error) {
	var service corev1.Service
	var deployment appsv1.Deployment
	data, _ := json.Marshal(c.objects[menuService].Object)
	if err := json.Unmarshal(data, &service); err != nil {
		return nil, err
	}
	data, _ = json.Marshal(c.objects[menuDeployment].Object)
	if err := json.Unmarshal(data, &deployment); err != nil {
		return nil, err
	}
	if service.Spec.Ports[0].TargetPort.IntVal == 8080 {
		c.observedReady = true
		c.trace = append(c.trace, "observe-backend-ready")
	}
	return sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{service}, DeploymentList: []appsv1.Deployment{deployment}}, nil
}
func menuB3Loop(t *testing.T) (*ResponderLoop, *menuB3Cluster) {
	t.Helper()
	service := planObject(menuService, map[string]any{"selector": map[string]any{"app": "backend"}, "ports": []any{map[string]any{"port": int64(80), "targetPort": int64(9999)}}})
	deployment := planObject(menuDeployment, map[string]any{"template": map[string]any{"spec": map[string]any{"automountServiceAccountToken": false, "serviceAccountName": "powerless", "containers": []any{map[string]any{"name": "frontend", "image": "synthetic.invalid/frontend@sha256:" + strings.Repeat("a", 64)}}}}})
	service.SetUID("service-uid")
	service.SetResourceVersion("service-rv")
	deployment.SetUID("deployment-uid")
	deployment.SetResourceVersion("deployment-rv")
	cluster := &menuB3Cluster{planFakeClient: planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{menuService: service, menuDeployment: deployment}}}
	programs := []menuB3Program{{"restart-only"}, {"routing-then-restart"}}
	registry, err := program.NewRegistry([]program.Diagnoser{programs[0], programs[1]}, []program.Mitigator{programs[0], programs[1]})
	if err != nil {
		t.Fatal(err)
	}
	finding := sdk.Finding{Status: sdk.FindingActive, PrimaryResource: menuService, ParameterBindings: map[string]sdk.ObjectRef{"service": menuService, "deployment": menuDeployment}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "synthetic-b3", Namespace: "app", SourceCommit: "synthetic-source", ResponseDeadline: time.Now().Add(10 * time.Minute), Findings: []sdk.Finding{finding}}, registry, cluster, nil, PlanExecutor{Client: cluster, Rollouts: &planFakeRollout{}}, 5)
	loop.ReadView = func(view sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: view.(sdktest.Snapshot)}
	}
	loop.DiagnosisSettleDelay = time.Nanosecond
	loop.MenuConfig = &ResponderMenuConfig{}
	return loop, cluster
}
func menuCall(t *testing.T, loop *ResponderLoop, payload map[string]any) map[string]any {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var request ToolRequest
	if err := json.Unmarshal(data, &request); err != nil {
		t.Fatal(err)
	}
	response := loop.Handle(context.Background(), request)
	data, err = json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
func menuSelection(t *testing.T, menu map[string]any, programID string, rounds int, requestID string) map[string]any {
	t.Helper()
	options, ok := menu["options"].([]any)
	if !ok {
		t.Fatalf("missing options: %+v", menu)
	}
	for _, value := range options {
		option := value.(map[string]any)
		if option["program_id"] != programID {
			continue
		}
		if reason, _ := option["disabled_reason"].(string); reason != "" {
			t.Fatalf("fixture option is disabled: %s", reason)
		}
		return map[string]any{"command": "mitigate", "selection": map[string]any{
			"version": 1, "incident_id": menu["incident_id"], "session_id": menu["session_id"], "dispatch_epoch": menu["dispatch_epoch"],
			"menu_id": menu["menu_id"], "option_id": option["option_id"], "request_id": requestID, "round_limit": rounds,
			"program_revision": option["program_revision"], "dependency_revision": option["dependency_revision"],
			"source_commit": menu["source_commit"], "scope_digest": menu["scope_digest"], "policy_digest": menu["policy_digest"], "bindings": menu["bindings"],
		}}
	}
	t.Fatalf("missing option %q: %+v", programID, menu)
	return nil
}
