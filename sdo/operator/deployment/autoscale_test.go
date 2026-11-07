package deployment

import (
	"context"
	"testing"

	"sdo.dev/operator/render/rendertest"
)

const autoscaledWeb = `apiVersion: apps/v1
kind: Deployment
metadata: {name: web}
spec:
  replicas: 3
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec:
      containers: [{name: web, image: "web:2"}]
`

const webAutoscaler = `apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {name: web}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: web}
  minReplicas: 2
  maxReplicas: 6
`

// appliedReplicas is the spec.replicas of the applied Deployment document.
func appliedReplicas(t *testing.T, cluster *recreateFake) (replicas any, present bool) {
	t.Helper()
	for _, document := range cluster.applied {
		if document["kind"] != "Deployment" {
			continue
		}
		spec, _ := document["spec"].(map[string]any)
		replicas, present = spec["replicas"]
		return replicas, present
	}
	t.Fatal("no Deployment was applied")
	return nil, false
}

// An autoscaler owns spec.replicas of the workload it targets: applying the
// rendered value on every deploy resets the autoscaler's decision (generation
// churn, replica flapping). The field is applied only when the workload does
// not exist yet.
func TestDeployerLeavesAutoscaledReplicasToTheAutoscaler(t *testing.T) {
	for _, tc := range []struct {
		name        string
		live        bool
		wantPresent bool
	}{
		{"existing workload", true, false},
		{"initial creation", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, source := recFixture(t, rendertest.MapFS{"k8s/web.yaml": autoscaledWeb, "k8s/hpa.yaml": webAutoscaler})
			cluster := &recreateFake{live: map[string]LiveObject{}}
			if tc.live {
				cluster.live["Deployment/web"] = LiveObject{Ref: ref("Deployment", "web"), UID: "web-1"}
			}
			deployer, _ := depDeployer(t, root, &cluster.clusterFake)
			deployer.Applier = cluster
			if _, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "Deployment/demo/web")); err != nil {
				t.Fatal(err)
			}
			replicas, present := appliedReplicas(t, cluster)
			if present != tc.wantPresent {
				t.Fatalf("applied spec.replicas present = %v (%v), want %v", present, replicas, tc.wantPresent)
			}
		})
	}
}

func TestDeployerKeepsReplicasOfAWorkloadNoAutoscalerTargets(t *testing.T) {
	root, source := recFixture(t, rendertest.MapFS{"k8s/web.yaml": autoscaledWeb})
	cluster := &recreateFake{live: map[string]LiveObject{"Deployment/web": {Ref: ref("Deployment", "web"), UID: "web-1"}}}
	deployer, _ := depDeployer(t, root, &cluster.clusterFake)
	deployer.Applier = cluster
	if _, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "Deployment/demo/web")); err != nil {
		t.Fatal(err)
	}
	if replicas, present := appliedReplicas(t, cluster); !present {
		t.Fatalf("spec.replicas dropped without an autoscaler (%v)", replicas)
	}
}
