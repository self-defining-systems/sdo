//go:build integration

package deployment

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"sdo.dev/operator/runtime/envtestio"
)

const coownedDeployment = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: app
  annotations: {%s}
spec:
  replicas: 1
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec:
      containers:
        - name: web
          image: web:1
          %lifecycle%
          volumeMounts: [{name: secrets, mountPath: /secrets}]
      volumes:
        - name: secrets
          %s
`

func coowned(annotations, volume, lifecycle string) []byte {
	document := strings.Replace(coownedDeployment, "{%s}", "{"+annotations+"}", 1)
	document = strings.Replace(document, "%lifecycle%", lifecycle, 1)
	return []byte(strings.Replace(document, "%s", volume, 1))
}

const preStop = `lifecycle: {preStop: {exec: {command: [sleep, "1"]}}}`

// An object an install created (helm, kubectl) is co-owned with the deployer
// for every field the deployer applied with the same value, and a server-side
// apply that stops sending a co-owned field leaves it in place: a head that
// swaps a volume's type (a chart's emptyDir swapped for projected secrets)
// or drops a container's lifecycle hook (the install owns the hook's struct
// markers, the deployer only its leaf, so the emptied hook stays) then fails admission with "may not specify more than 1 volume type" and SDO
// holds the head. The deployer owns what it applies alone, so removing a field
// works; what only the install owns stays the install's and does not roll.
func TestAHeadDeployMayRemoveAFieldTheInstallCoOwned(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a real API server, over the PR-tier slow-test budget; nightly CI runs it without -short")
	}
	env := envtestio.Start(t)
	env.CreateNamespace(t, "app")
	backend := CommandBackend{Run: func(ctx context.Context, name string, args []string, input []byte) ([]byte, error) {
		cmd := exec.CommandContext(ctx, name, append([]string{"--kubeconfig", env.Kubeconfig}, args...)...)
		cmd.Stdin = strings.NewReader(string(input))
		return cmd.CombinedOutput()
	}}
	ctx := context.Background()
	client := env.Client(t)

	// The install: helm created the object (an Update by manager "helm").
	var installed appsv1.Deployment
	if err := yaml.Unmarshal(coowned(`"meta.helm.sh/release-name": release`, "emptyDir: {}", preStop), &installed); err != nil {
		t.Fatal(err)
	}
	if _, err := client.AppsV1().Deployments("app").Create(ctx, &installed, metav1.CreateOptions{FieldManager: "helm"}); err != nil {
		t.Fatal(err)
	}
	// SDO's first deploys re-apply the same source: the deployer co-owns what it sends.
	if err := backend.Apply(ctx, coowned(`"meta.helm.sh/release-name": release`, "emptyDir: {}", preStop)); err != nil {
		t.Fatal(err)
	}
	before, err := client.AppsV1().Deployments("app").Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// A head deploy that swaps the volume's type, as a chart's minor release does.
	next := coowned(`"meta.helm.sh/release-name": release`, "projected: {sources: [{secret: {name: web-secrets}}]}", "")
	if err := backend.Apply(ctx, next); err != nil {
		t.Fatalf("a head deploy that removes a co-owned field was refused: %v", err)
	}
	after, err := client.AppsV1().Deployments("app").Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if after.Spec.Template.Spec.Containers[0].Lifecycle != nil {
		t.Fatalf("lifecycle = %+v, want it gone with the head's removal", after.Spec.Template.Spec.Containers[0].Lifecycle)
	}
	volume := after.Spec.Template.Spec.Volumes[0]
	if volume.Projected == nil || volume.EmptyDir != nil {
		t.Fatalf("volume = %+v, want the projected source alone", volume)
	}
	if after.Annotations["meta.helm.sh/release-name"] != "release" {
		t.Fatalf("annotations = %v: a field only the install owns was removed", after.Annotations)
	}
	if after.Generation != before.Generation+1 {
		t.Fatalf("generation %d -> %d, want exactly the one rollout the head's change asks for", before.Generation, after.Generation)
	}

	// Re-applying the same head changes nothing: the migration is idempotent and rolls nothing.
	if err := backend.Apply(ctx, next); err != nil {
		t.Fatal(err)
	}
	again, err := client.AppsV1().Deployments("app").Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if again.Generation != after.Generation {
		t.Fatalf("generation %d -> %d on an identical apply", after.Generation, again.Generation)
	}
}
