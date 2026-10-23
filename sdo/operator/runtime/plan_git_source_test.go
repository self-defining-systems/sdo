package runtime

import (
	"context"
	"encoding/json"
	"fmt"
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

type fixedSourceImages struct{ image, tree string }

func (i fixedSourceImages) BaselineImage(sdk.ObjectRef, string, bool) (string, bool) {
	return i.image, true
}
func (i fixedSourceImages) BaselineTree(sdk.ObjectRef) string { return i.tree }

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
	image := "example/app@sha256:" + strings.Repeat("b", 64)
	if _, err := (GitPlanSource{Repository: repo, ManifestPaths: []string{"app.yaml"}}).AcceptedManifest(context.Background(), strings.TrimSpace(string(commit)), ref); err == nil {
		t.Fatal("mutable raw image restored without deployed image evidence")
	}
	source := GitPlanSource{Repository: repo, Images: fixedSourceImages{image: image, tree: nativeGit(t, repo, "rev-parse", "HEAD^{tree}")}, ManifestPaths: []string{"app.yaml"}}
	locked := seedRenderedLock(t, source, strings.TrimSpace(string(commit)), "app")
	commit = []byte(locked)
	manifests, err := source.AcceptedManifests(context.Background(), strings.TrimSpace(string(commit)), "app")
	if err != nil || len(manifests) != 1 || manifests[0].GetName() != "rate" || manifests[0].Object["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] != image {
		t.Fatalf("adoption source lost rendered image: %+v, %v", manifests, err)
	}
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
	locked := seedRenderedLock(t, GitPlanSource{Repository: repo, ManifestPaths: []string{"app.yaml"}}, strings.TrimSpace(string(commit)), "app")
	commit = []byte(locked)
	if err := os.WriteFile(path, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	object, err := (GitPlanSource{Repository: repo, ManifestPaths: []string{"app.yaml"}}).AcceptedManifest(context.Background(), strings.TrimSpace(string(commit)), ref)
	if err != nil || object.GetName() != "api" {
		t.Fatalf("accepted manifest: %+v %v", object, err)
	}
}

func TestGitPlanSourceUsesOnlyDeployedManifestSet(t *testing.T) {
	repo := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.name", "Test"}, {"config", "user.email", "test@example.com"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %s: %v", args, output, err)
		}
	}
	for _, path := range []string{"kubernetes/geo/service.yaml", "kubernetes/rate/service.yaml", "knative/geo-service.yaml", "failures/buggy-geo-service.yaml"} {
		full := filepath.Join(repo, path)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		manifest, err := os.ReadFile(filepath.Join("testdata", "deployed_variants", path))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, manifest, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"add", "."}, {"commit", "-m", "seed"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %s: %v", args, output, err)
		}
	}
	commit, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	selection := GitPlanSource{Repository: repo, ManifestPaths: []string{"kubernetes"}}
	commit = []byte(seedRenderedLock(t, selection, strings.TrimSpace(string(commit)), "app"))
	objects, err := selection.AcceptedManifests(context.Background(), strings.TrimSpace(string(commit)), "app")
	if err != nil || len(objects) != 2 {
		t.Fatalf("deployed objects = %+v, %v", objects, err)
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "geo"}
	object, err := selection.AcceptedManifest(context.Background(), strings.TrimSpace(string(commit)), ref)
	if err != nil {
		t.Fatal(err)
	}
	ports, _, _ := unstructured.NestedSlice(object.Object, "spec", "ports")
	if len(ports) != 1 || fmt.Sprint(ports[0].(map[string]any)["targetPort"]) != "8080" {
		t.Fatalf("selected variant ports = %+v", ports)
	}
	if _, err := (GitPlanSource{Repository: repo}).AcceptedManifests(context.Background(), strings.TrimSpace(string(commit)), "app"); err == nil {
		t.Fatal("ambiguous deployed set was guessed")
	}
}

func TestGitPlanSourceSelectsDeployedHotelSeed(t *testing.T) {
	cases := []struct {
		name, seed string
		count      int
		targetPort string
	}{
		{"portable deployed variants", "testdata/deployed_variants", 2, "8080"},
	}
	if seed := os.Getenv("SDO_DRIFT_SEED"); seed != "" {
		cases = append(cases, struct {
			name, seed string
			count      int
			targetPort string
		}{"deployed hotel seed", seed, 39, "8083"})
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			seed := test.seed
			repo := t.TempDir()
			for _, args := range [][]string{{"init"}, {"config", "user.name", "Test"}, {"config", "user.email", "test@example.com"}} {
				if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
					t.Fatalf("git %v: %s: %v", args, output, err)
				}
			}
			for _, variant := range []string{"kubernetes", "knative", "failures"} {
				copyFastloopTree(t, filepath.Join(seed, variant), filepath.Join(repo, variant))
			}
			for _, args := range [][]string{{"add", "."}, {"commit", "-m", "hotel seed"}} {
				if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
					t.Fatalf("git %v: %s: %v", args, output, err)
				}
			}
			commit, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
			if err != nil {
				t.Fatal(err)
			}
			source := GitPlanSource{Repository: repo, ManifestPaths: []string{"kubernetes"}, Images: fixedSourceImages{image: "example/app@sha256:" + strings.Repeat("a", 64), tree: nativeGit(t, repo, "rev-parse", "HEAD^{tree}")}}
			commit = []byte(seedRenderedLock(t, source, strings.TrimSpace(string(commit)), "hotel-res"))
			objects, err := source.AcceptedManifests(context.Background(), strings.TrimSpace(string(commit)), "hotel-res")
			if err != nil {
				t.Fatal(err)
			}
			if len(objects) != test.count {
				t.Fatalf("deployed object count = %d, want %d", len(objects), test.count)
			}
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "hotel-res", Name: "geo"}
			geo, err := source.AcceptedManifest(context.Background(), strings.TrimSpace(string(commit)), ref)
			if err != nil {
				t.Fatal(err)
			}
			ports, _, _ := unstructured.NestedSlice(geo.Object, "spec", "ports")
			if len(ports) != 1 || fmt.Sprint(ports[0].(map[string]any)["targetPort"]) != test.targetPort {
				t.Fatalf("selected geo Service ports = %+v", ports)
			}
		})
	}
}
