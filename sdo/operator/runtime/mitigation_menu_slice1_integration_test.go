//go:build integration

package runtime

// Executable MI1 acceptance assertions. Synthetic durable/provenance adapters
// wire the real menu interfaces; an unknown command is not a safety refusal.
// All workloads, images and canaries in this file are synthetic.
import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

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
	loop.MenuConfig = &ResponderMenuConfig{}
	return loop, cluster
}

