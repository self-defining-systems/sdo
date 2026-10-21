package runtime

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type fixedSourceImages struct{ image string }

func (i fixedSourceImages) BaselineImage(sdk.ObjectRef, string, bool) (string, bool) {
	return i.image, true
}
func (i fixedSourceImages) BaselineTree(sdk.ObjectRef) string { return "" }

func TestRenderedSourceRestoresBuiltImageWithoutFalseDrift(t *testing.T) {
	repo := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.name", "Test"}, {"config", "user.email", "test@example.com"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatal(string(output), err)
		}
	}
	manifest := "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: rate\n  namespace: app\nspec:\n  replicas: 1\n  template:\n    spec:\n      containers:\n      - name: rate\n        image: example/app:latest\n"
	if err := os.WriteFile(filepath.Join(repo, "app.yaml"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "app.yaml"}, {"commit", "-m", "source"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatal(string(output), err)
		}
	}
	commit, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "rate"}
	image := "example/app:sregym-src"
	if _, err := (GitPlanSource{Repository: repo}).AcceptedManifest(context.Background(), strings.TrimSpace(string(commit)), ref); err == nil {
		t.Fatal("mutable raw image restored without deployed image evidence")
	}
	source := GitPlanSource{Repository: repo, Images: fixedSourceImages{image}}
	baseline := planObject(ref, map[string]any{"replicas": float64(1), "template": map[string]any{
		"spec": map[string]any{"containers": []any{map[string]any{"name": "rate", "image": image}}},
	}})
	loop := NewResponderLoop(IncidentRequest{SourceCommit: strings.TrimSpace(string(commit)), Namespace: "app"}, nil, nil, nil, PlanExecutor{Source: source}, 1)
	response := loop.Handle(context.Background(), ToolRequest{Command: "source", Ref: &ref})
	if response.Error != "" || response.Source == nil {
		t.Fatalf("source tool: %+v", response)
	}
	if response.Source["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] != image {
		t.Fatalf("source tool lost deployed image: %+v", response.Source)
	}
	current := baseline.DeepCopy()
	containers, _, _ := unstructured.NestedSlice(current.Object, "spec", "template", "spec", "containers")
	containers[0].(map[string]any)["image"] = "example/app:broken"
	if err := unstructured.SetNestedSlice(current.Object, containers, "spec", "template", "spec", "containers"); err != nil {
		t.Fatal(err)
	}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: current}}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	if _, err := executor.Apply(context.Background(), IncidentRequest{SourceCommit: strings.TrimSpace(string(commit)), RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false); err != nil {
		t.Fatal(err)
	}
	got, _ := json.Marshal(comparableFields(client.objects[ref].Object))
	want, _ := json.Marshal(comparableFields(baseline.Object))
	if string(got) != string(want) {
		t.Fatalf("restored = %s; deployed = %s", got, want)
	}
	replicas := int32(1)
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "rate", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Replicas: &replicas, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "rate", Image: image}}}}}}}}, SourceCommit: strings.TrimSpace(string(commit)), Source: source}
	if diff, err := view.DiffAgainstSource(ref); err != nil || diff.Changed {
		t.Fatalf("untouched deployed object drifted: %+v %v", diff, err)
	}
}

func TestGitPlanSourceReadsManifestAtAcceptedCommit(t *testing.T) {
	repo := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.name", "Test"}, {"config", "user.email", "test@example.com"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %s: %v", args, output, err)
		}
	}
	path := filepath.Join(repo, "app.yaml")
	if err := os.WriteFile(path, []byte("apiVersion: v1\nkind: Service\nmetadata:\n  name: api\n  namespace: app\nspec:\n  ports:\n  - port: 80\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "app.yaml"}, {"commit", "-m", "fixture"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %s: %v", args, output, err)
		}
	}
	commit, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	object, err := (GitPlanSource{Repository: repo}).AcceptedManifest(context.Background(), strings.TrimSpace(string(commit)), ref)
	if err != nil || object.GetName() != "api" {
		t.Fatalf("accepted manifest: %+v %v", object, err)
	}
}
