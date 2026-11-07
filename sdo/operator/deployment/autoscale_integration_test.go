//go:build integration

package deployment

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sigs.k8s.io/yaml"

	"sdo.dev/operator/rendered"
	"sdo.dev/operator/runtime/envtestio"
	"sdo.dev/operator/sdk"
)

const handoffDeployment = `apiVersion: apps/v1
kind: Deployment
metadata: {name: %s, namespace: app}
spec:
  replicas: 3
  selector: {matchLabels: {app: %s}}
  template:
    metadata: {labels: {app: %s}}
    spec:
      containers: [{name: web, image: "web:1"}]
`

const handoffHPA = `apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {name: web, namespace: app}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: web}
  minReplicas: 2
  maxReplicas: 6
`

// SSA deletes a field the applying manager stops sending when it is the only
// owner, and the API server defaults a deleted replica count to 1. A workload
// that an autoscaler targets but has not scaled yet (so the deployer is still
// the sole owner) must keep its count when SDO stops applying it; the count
// never dips, on a real API server.
func TestStoppingToApplyReplicasKeepsTheCountOnARealAPIServer(t *testing.T) {
	env := envtestio.Start(t)
	env.CreateNamespace(t, "app")
	backend := CommandBackend{Run: func(ctx context.Context, name string, args []string, input []byte) ([]byte, error) {
		cmd := exec.CommandContext(ctx, name, append([]string{"--kubeconfig", env.Kubeconfig}, args...)...)
		cmd.Stdin = strings.NewReader(string(input))
		return cmd.CombinedOutput()
	}}
	ctx := context.Background()
	client := env.Client(t)
	replicas := func(name string) int32 {
		t.Helper()
		deployment, err := client.AppsV1().Deployments("app").Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return *deployment.Spec.Replicas
	}
	render := func(name string) string {
		return strings.NewReplacer("%s", name).Replace(handoffDeployment)
	}
	applyWithoutReplicas := func(name string, handoff bool) {
		t.Helper()
		var hpa unstructured.Unstructured
		if err := unstructuredFromYAML(handoffHPA, &hpa); err != nil {
			t.Fatal(err)
		}
		var autoscaled rendered.Autoscaled
		autoscaled.AddObjects("app", &hpa)
		var deployment unstructured.Unstructured
		if err := unstructuredFromYAML(render(name), &deployment); err != nil {
			t.Fatal(err)
		}
		document := deployment.DeepCopy().Object
		applier := Applier(backend)
		if !handoff {
			applier = withoutHandoff{backend}
		}
		run := &applyRun{deployer: Deployer{Applier: applier}}
		object := rendered.DesiredObject{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}}
		if err := run.omitAutoscaledReplicas(ctx, autoscaled, object, document); err != nil {
			t.Fatal(err)
		}
		if _, found := document["spec"].(map[string]any)["replicas"]; found {
			t.Fatal("replicas was not omitted")
		}
		encoded, err := marshalYAML(document)
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.Apply(ctx, encoded); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("premise: omitting a sole-owned count resets it", func(t *testing.T) {
		if err := backend.Apply(ctx, []byte(render("web"))); err != nil {
			t.Fatal(err)
		}
		applyWithoutReplicas("web", false)
		if got := replicas("web"); got != 1 {
			t.Fatalf("replicas = %d after an omitting apply without a hand-off, want the API default 1", got)
		}
	})

	t.Run("hand-off keeps the count, then the autoscaler takes over", func(t *testing.T) {
		if _, err := client.AppsV1().Deployments("app").Get(ctx, "web", metav1.GetOptions{}); err == nil {
			if err := client.AppsV1().Deployments("app").Delete(ctx, "web", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		if err := backend.Apply(ctx, []byte(render("web"))); err != nil {
			t.Fatal(err)
		}
		applyWithoutReplicas("web", true)
		if got := replicas("web"); got != 3 {
			t.Fatalf("replicas = %d after the hand-off, want 3 (the count must never dip)", got)
		}
		// The autoscaler scales through the scale subresource.
		if _, err := client.AppsV1().Deployments("app").UpdateScale(ctx, "web", &autoscalingv1.Scale{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"}, Spec: autoscalingv1.ScaleSpec{Replicas: 5}}, metav1.UpdateOptions{FieldManager: "kube-controller-manager"}); err != nil {
			t.Fatal(err)
		}
		applyWithoutReplicas("web", true)
		if got := replicas("web"); got != 5 {
			t.Fatalf("replicas = %d after the autoscaler scaled and a deploy, want 5", got)
		}
	})
}

// withoutHandoff is the backend as it behaved before the hand-off.
type withoutHandoff struct{ CommandBackend }

func (withoutHandoff) HandOffReplicas(context.Context, sdk.ObjectRef) error { return nil }

func unstructuredFromYAML(text string, into *unstructured.Unstructured) error {
	var object map[string]any
	if err := yaml.Unmarshal([]byte(text), &object); err != nil {
		return err
	}
	into.Object = object
	return nil
}

func marshalYAML(document map[string]any) ([]byte, error) { return yaml.Marshal(document) }
