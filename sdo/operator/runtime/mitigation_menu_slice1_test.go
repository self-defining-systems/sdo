package runtime

// Executable MI1 acceptance fixtures. These intentionally remain red until the
// durable menu executor is wired; an unknown command is not a safety refusal.
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
	return program.ProgramSpec{ID: p.id, Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "deployment", Kind: "Deployment", Required: true}}}
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
		return program.Plan{Actions: []program.Action{program.SetField{Ref: b["service"], Path: "spec.ports[0].targetPort", Value: 8080, Reason: "synthetic routing repair"}}}, nil
	}
	return program.Plan{Actions: []program.Action{program.RestartRollout{Ref: b["deployment"], Reason: "bootstrap after observed backend readiness"}}}, nil
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

func TestMitigationMenuB3RoundControlsAndAlternateOption(t *testing.T) {
	for _, limit := range []int{1, 2} {
		t.Run(fmt.Sprintf("round-limit-%d", limit), func(t *testing.T) {
			loop, cluster := menuB3Loop(t)
			response := menuCall(t, loop, map[string]any{"command": "menu"})
			menu, ok := response["menu"].(map[string]any)
			if !ok {
				t.Fatalf("B3 needs a selectable menu, got %+v", response)
			}
			options, ok := menu["options"].([]any)
			if !ok || len(options) != 2 {
				t.Fatalf("want two selectable options: %+v", menu)
			}
			failed := menuCall(t, loop, menuSelection(t, menu, "restart-only", 1, "restart-control"))
			first, ok := failed["report"].(map[string]any)
			if !ok || first["status"] != "STATUS_UNRESOLVED" {
				t.Fatalf("restart-only must settle an unsuccessful report: %+v", failed)
			}
			response = menuCall(t, loop, map[string]any{"command": "menu"})
			next := response["menu"].(map[string]any)
			if next["session_id"] != menu["session_id"] {
				t.Fatal("alternate option changed session")
			}
			result := menuCall(t, loop, menuSelection(t, next, "routing-then-restart", limit, "alternate"))
			report, ok := result["report"].(map[string]any)
			if !ok {
				t.Fatalf("missing durable report: %+v", result)
			}
			if (report["status"] == "STATUS_SUCCESS") != (limit == 2) {
				t.Fatalf("B3 round control: %+v; trace=%v", report, cluster.trace)
			}
			if rounds, _ := report["rounds"].([]any); len(rounds) != limit {
				t.Fatalf("round receipts missing: %+v", report)
			}
			if limit == 2 {
				// Additional fresh reads are allowed; the three causal steps must
				// occur in order, not necessarily in adjacent calls.
				step := 0
				for _, event := range cluster.trace {
					if step < 3 && event == []string{"route", "observe-backend-ready", "restart"}[step] {
						step++
					}
				}
				if step != 3 {
					t.Fatalf("restart lacked fresh readiness barrier: %v", cluster.trace)
				}
			}
			if report["attempt_id"] == "" || first["attempt_id"] == "" || report["attempt_id"] == first["attempt_id"] {
				t.Fatal("attempt IDs were reused")
			}
			writes := len(cluster.patches)
			retrieved := menuCall(t, loop, map[string]any{"command": "report", "report_query": map[string]any{"version": 1, "incident_id": menu["incident_id"], "session_id": menu["session_id"], "dispatch_epoch": next["dispatch_epoch"], "attempt_id": report["attempt_id"]}})
			want, _ := json.Marshal(report)
			got, _ := json.Marshal(retrieved["report"])
			if string(want) != string(got) || len(cluster.patches) != writes {
				t.Fatalf("report retrieval changed settlement or wrote again: %+v", retrieved)
			}
			if len(loop.PlanApplications()) != 0 {
				t.Fatal("menu attempt entered legacy positive trust path")
			}
		})
	}
}

func TestMitigationMenuCompensationRechecksSecretConsumer(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	response := menuCall(t, loop, map[string]any{"command": "menu"})
	menu, ok := response["menu"].(map[string]any)
	if !ok {
		t.Fatalf("compensation fixture needs an enabled menu: %+v", response)
	}
	selection := menuSelection(t, menu, "routing-then-restart", 1, "compensation-safety")
	cluster.afterPatch = func(ref sdk.ObjectRef) {
		if ref != menuService {
			return
		}
		// The first routing write lands safely. Before the one-round failure
		// unwinds it, a consumer becomes unsafe. Compensation is a new effect
		// and must re-evaluate the whole affected scope, not reuse acceptance.
		_ = unstructured.SetNestedSlice(cluster.objects[menuDeployment].Object, []any{map[string]any{"name": "identity", "secret": map[string]any{"secretName": "SYNTHETIC-COMPENSATION-CANARY"}}}, "spec", "template", "spec", "volumes")
	}
	result := menuCall(t, loop, selection)
	report, ok := result["report"].(map[string]any)
	if !ok || report["status"] == "STATUS_SUCCESS" {
		t.Fatalf("unsafe compensation must retain an unsuccessful report: %+v", result)
	}
	if len(cluster.patches) != 1 || cluster.forbiddenReads != 0 {
		t.Fatalf("unsafe compensation wrote/fetched opaque content: writes=%d reads=%d", len(cluster.patches), cluster.forbiddenReads)
	}
	if residuals, _ := report["residuals"].([]any); len(residuals) == 0 {
		t.Fatalf("retained routing effect needs residual evidence: %+v", report)
	}
	encoded, _ := json.Marshal(loop.ToolCalls())
	if strings.Contains(string(encoded), "SYNTHETIC-COMPENSATION-CANARY") {
		t.Fatal("unsafe compensation persisted credential canary")
	}
}

func TestMitigationMenuSecretNegativeFixtures(t *testing.T) {
	const canary = "SYNTHETIC-MI1-CREDENTIAL-CANARY"
	fixtures := map[string]func(map[string]any){
		"secret-envfrom": func(s map[string]any) {
			s["containers"].([]any)[0].(map[string]any)["envFrom"] = []any{map[string]any{"secretRef": map[string]any{"name": canary}}}
		},
		"secret-keyref": func(s map[string]any) {
			s["containers"].([]any)[0].(map[string]any)["env"] = []any{map[string]any{"name": "TOKEN", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": canary, "key": "token"}}}}
		},
		"projected-token": func(s map[string]any) {
			s["volumes"] = []any{map[string]any{"name": "identity", "projected": map[string]any{"sources": []any{map[string]any{"serviceAccountToken": map[string]any{"path": canary}}}}}}
		},
		"csi": func(s map[string]any) {
			s["volumes"] = []any{map[string]any{"name": "opaque", "csi": map[string]any{"driver": canary}}}
		},
		"hostpath": func(s map[string]any) {
			s["volumes"] = []any{map[string]any{"name": "host", "hostPath": map[string]any{"path": "/" + canary}}}
		},
		"pvc": func(s map[string]any) {
			s["volumes"] = []any{map[string]any{"name": "disk", "persistentVolumeClaim": map[string]any{"claimName": canary}}}
		},
		"implicit-sa": func(s map[string]any) { delete(s, "automountServiceAccountToken") },
		"privileged-init": func(s map[string]any) {
			s["initContainers"] = []any{map[string]any{"name": "init", "image": canary, "securityContext": map[string]any{"privileged": true}}}
		},
		"executable-configmap": func(s map[string]any) {
			s["volumes"] = []any{map[string]any{"name": "script", "configMap": map[string]any{"name": canary}}}
		},
		"copied-secret-literal": func(s map[string]any) {
			s["containers"].([]any)[0].(map[string]any)["env"] = []any{map[string]any{"name": "COPIED", "value": canary}}
		},
	}
	for name, mutate := range fixtures {
		for _, option := range []string{"restart-only", "routing-then-restart"} {
			t.Run(name+"/"+option, func(t *testing.T) {
				loop, cluster := menuB3Loop(t)
				response := menuCall(t, loop, map[string]any{"command": "menu"})
				menu, ok := response["menu"].(map[string]any)
				if !ok {
					t.Fatalf("negative fixture needs an enabled menu: %+v", response)
				}
				selection := menuSelection(t, menu, option, 2, "negative")
				spec, _, _ := unstructured.NestedMap(cluster.objects[menuDeployment].Object, "spec", "template", "spec")
				mutate(spec)
				_ = unstructured.SetNestedMap(cluster.objects[menuDeployment].Object, spec, "spec", "template", "spec")
				// Selecting either enabled primitive must reject the affected consumer
				// before a write or an opaque fetch, with a structured safe report.
				result := menuCall(t, loop, selection)
				report, ok := result["report"].(map[string]any)
				if !ok || report["status"] != "STATUS_REFUSED" || report["failed_phase"] != "safety" {
					t.Errorf("need typed fail-closed refusal, got %+v", result)
				}
				if len(cluster.patches) != 0 || cluster.forbiddenReads != 0 {
					t.Errorf("unsafe effects: writes=%d opaque reads=%d", len(cluster.patches), cluster.forbiddenReads)
				}
				encoded, _ := json.Marshal(loop.ToolCalls())
				if strings.Contains(string(encoded), canary) {
					t.Fatal("credential canary persisted in tool evidence")
				}
			})
		}
	}
}
