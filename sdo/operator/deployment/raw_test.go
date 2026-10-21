package deployment

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type fakeRawBackend struct {
	applied string
	tree    string
}

func (backend *fakeRawBackend) BuildImage(_ context.Context, _ string, _ *contracts.ImageBuild) (string, error) {
	return "registry.example/app@sha256:" + strings.Repeat("b", 64), nil
}
func (backend *fakeRawBackend) Apply(_ context.Context, documents []byte) error {
	backend.applied = string(documents)
	return nil
}
func (backend *fakeRawBackend) Observe(_ context.Context, id string) (WorkloadObservation, error) {
	return WorkloadObservation{ResourceID: id, Revision: backend.tree, Images: []string{"registry.example/app@sha256:" + strings.Repeat("b", 64)}, Ready: true}, nil
}

func TestRawStrategyStampsCandidateRevisionAndChecksObservedWorkload(t *testing.T) {
	checkout := candidateRepository(t)
	manifest := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
spec:
  replicas: 1
  template:
    metadata:
      labels: {app: app}
    spec:
      containers:
        - name: app
          image: registry.example/app:staging
`
	if err := os.WriteFile(filepath.Join(checkout, "deployment.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	backend := &fakeRawBackend{}
	registry, err := NewRegistry(RawRegistration("raw-manifests", "default", backend))
	if err != nil {
		t.Fatal(err)
	}
	tree, err := broker.ProposalTreeDigest(context.Background(), checkout)
	if err != nil {
		t.Fatal(err)
	}
	backend.tree = tree
	config, err := anypb.New(&contracts.RawManifestStrategy{
		ImageBuilds:   []*contracts.ImageBuild{{Image: "registry.example/app:staging", ContextPath: ".", DockerfilePath: "Dockerfile"}},
		ManifestPaths: []string{"deployment.yaml"}, ExpectedWorkloadIds: []string{"Deployment/default/app"},
	})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := registry.ApplyAndVerify(context.Background(), checkout, &contracts.DeploymentPlan{StrategyId: "raw-manifests", StrategyConfig: config})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ProposalTreeDigest != tree || len(receipt.ManifestPaths) != 1 || receipt.ManifestPaths[0] != "deployment.yaml" || !strings.Contains(backend.applied, tree) || !strings.Contains(backend.applied, "registry.example/app@sha256:") {
		t.Fatalf("unbound deployment: %+v, %s", receipt, backend.applied)
	}
	for _, args := range [][]string{{"add", "-A"}, {"commit", "-qm", "accepted source"}} {
		if output, err := exec.Command("git", append([]string{"-C", checkout}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	commit, err := exec.Command("git", "-C", checkout, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	registry.AcceptedRoot = checkout
	if _, err := registry.VerifyAccepted(context.Background(), strings.TrimSpace(string(commit)), "default", receipt); err != nil {
		t.Fatalf("accepted source not verified: %v", err)
	}
	backend.tree = strings.Repeat("c", 40)
	if _, err := registry.VerifyAccepted(context.Background(), strings.TrimSpace(string(commit)), "default", receipt); err == nil {
		t.Fatal("stale live workload accepted")
	}
}

func TestRawStrategyRejectsEscapingManifestPath(t *testing.T) {
	checkout := candidateRepository(t)
	backend := &fakeRawBackend{}
	registry, err := NewRegistry(RawRegistration("raw-manifests", "default", backend))
	if err != nil {
		t.Fatal(err)
	}
	config, err := anypb.New(&contracts.RawManifestStrategy{ManifestPaths: []string{"../outside.yaml"}, ExpectedWorkloadIds: []string{"Deployment/default/app"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ApplyAndVerify(context.Background(), checkout, &contracts.DeploymentPlan{StrategyId: "raw-manifests", StrategyConfig: config}); err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("escaping plan accepted or rejected for wrong reason: %v", err)
	}
}

func TestRawStrategyAppliesOnlySelectedManifestDirectory(t *testing.T) {
	checkout := candidateRepository(t)
	for path, name := range map[string]string{"kubernetes/app.yaml": "app", "knative/app.yaml": "alternate"} {
		full := filepath.Join(checkout, path)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		manifest := "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: " + name + "\nspec:\n  template:\n    spec:\n      containers:\n      - name: app\n        image: registry.example/app@sha256:" + strings.Repeat("b", 64) + "\n"
		if err := os.WriteFile(full, []byte(manifest), 0644); err != nil {
			t.Fatal(err)
		}
	}
	backend := &fakeRawBackend{}
	tree, err := broker.ProposalTreeDigest(context.Background(), checkout)
	if err != nil {
		t.Fatal(err)
	}
	backend.tree = tree
	_, err = (RawStrategy{Namespace: "default", Backend: backend}).ApplyAndVerify(context.Background(), checkout, tree, &contracts.RawManifestStrategy{ManifestPaths: []string{"kubernetes"}, ExpectedWorkloadIds: []string{"Deployment/default/app"}})
	if err != nil || strings.Contains(backend.applied, "alternate") || !strings.Contains(backend.applied, "name: app") {
		t.Fatalf("selected deployment = %s, %v", backend.applied, err)
	}
}
