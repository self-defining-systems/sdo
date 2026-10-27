package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// A real deployment render and healthy informer snapshot, followed by a memory
// commit and maintenance reset: neither may change the deployed image identity.
func acceptedSourceDeployment(t *testing.T, withTree bool) (GitPlanSource, *StateTracker, string, *appsv1.Deployment, *corev1.Service) {
	t.Helper()
	source, _ := foreignSource(t)
	manifest := "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: consul\nspec:\n  template:\n    spec:\n      initContainers:\n      - name: setup\n        image: example/setup:latest\n      containers:\n      - name: consul\n        image: consul:1.15\n"
	if err := os.WriteFile(filepath.Join(source.Repository, "consul.yaml"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, source.Repository, "add", ".")
	nativeGit(t, source.Repository, "commit", "-qm", "deploy A")
	commit := nativeGit(t, source.Repository, "rev-parse", "HEAD")
	tree := nativeGit(t, source.Repository, "rev-parse", "HEAD^{tree}")
	if !withTree {
		tree = ""
	}
	source.DeployedCommit = commit
	source.ManifestPaths = []string{"app.yaml", "consul.yaml"}
	// Read raw accepted bytes, then exercise the same renderer as deployment.
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "consul"}
	raw, err := source.Revision(context.Background(), ref, commit)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = deployment.RenderManifest(raw.Object, "app", tree, func(_, _, container string, init bool, _ string) string {
		if init {
			return "example/setup@sha256:" + strings.Repeat("a", 64)
		}
		return "consul@sha256:" + strings.Repeat("b", 64)
	})
	if err != nil {
		t.Fatal(err)
	}
	deployed := &appsv1.Deployment{}
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(raw.Object, deployed); err != nil {
		t.Fatal(err)
	}
	service := driftService(8080)
	service.Spec.Selector = map[string]string{"app": "api"}
	tracker := newTrackerForTest(t, StateTrackerConfig{Client: fake.NewSimpleClientset(deployed, service), Namespace: "app"})
	if err := tracker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tracker.Stop)
	tracker.Observe(time.Unix(10, 0), true)
	tracker.recordDeployedImage(ref, "consul", false, deployed.Spec.Template.Spec.Containers[0].Image, tree)
	tracker.recordDeployedImage(ref, "setup", true, deployed.Spec.Template.Spec.InitContainers[0].Image, tree)
	source.Images = tracker
	commit = seedRenderedLock(t, source, commit, "app")
	return source, tracker, commit, deployed, service
}

func TestHermeticAcceptedSourceAfterMemoryCommit(t *testing.T) {
	for _, withTree := range []bool{true, false} {
		name := "annotated deployment"
		if !withTree {
			name = "legacy deployment"
		}
		t.Run(name, func(t *testing.T) { testAcceptedSourceAfterMemoryCommit(t, withTree) })
	}
}

func testAcceptedSourceAfterMemoryCommit(t *testing.T, withTree bool) {
	source, tracker, deployedCommit, deployed, service := acceptedSourceDeployment(t, withTree)
	// Adoption recorded acceptance at the deployed commit; a memory-only
	// commit leaves accepted source, and so the record, current.
	accepted, err := source.AcceptedManifest(context.Background(), deployedCommit, sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "consul"})
	if err != nil {
		t.Fatal(err)
	}
	deployed.Annotations = withAcceptance(t, deployed, accepted)
	if err := os.MkdirAll(filepath.Join(source.Repository, ".sdo"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source.Repository, ".sdo", "goal.md"), []byte("synthetic memory change\n"), 0600); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, source.Repository, "add", ".sdo")
	nativeGit(t, source.Repository, "commit", "-qm", "memory B")
	commit := nativeGit(t, source.Repository, "rev-parse", "HEAD")
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "consul"}
	serviceRef := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	for _, reset := range []bool{false, true} {
		if reset {
			tracker.Reset()
			tracker.Observe(time.Unix(11, 0), false)
		}
		objects, err := source.AcceptedManifests(context.Background(), commit, "app")
		if err != nil || len(objects) != 2 {
			t.Fatalf("memory commit/reset=%v: objects=%d error=%v", reset, len(objects), err)
		}
		for _, object := range objects {
			if object.GetKind() == "Deployment" && object.GetAnnotations()["sdo.dev/source-tree"] != deployed.GetAnnotations()["sdo.dev/source-tree"] {
				t.Fatal("render changed deployed revision annotations")
			}
		}
		view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{*deployed}, ServiceList: []corev1.Service{*service}}, Source: source, SourceCommit: commit}
		if diff, err := view.DiffAgainstSource(ref); err != nil || diff.Changed {
			t.Fatalf("unchanged deployment reset=%v: %+v %v", reset, diff, err)
		}
		service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
		view.DetectionContext = sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{*deployed}, ServiceList: []corev1.Service{*service}}
		if diff, err := view.DiffAgainstSource(serviceRef); err != nil || !diff.Changed {
			t.Fatalf("lost targetPort diff: %+v %v", diff, err)
		}
		accepted, err := source.AcceptedManifest(context.Background(), commit, serviceRef)
		if err != nil {
			t.Fatal(err)
		}
		change := ObjectChange{Old: accepted, New: driftObject(service, sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}), Adoption: true}
		event, err := (DriftClassifier{}).Classify(context.Background(), change, view, time.Unix(12, 0))
		if err != nil || event == nil || event.Classification != sdk.DriftUnexplained || !hasTargetPortChange(event) {
			t.Fatalf("lost routing event: %+v %v", event, err)
		}
		service.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	}
}

func TestAcceptedSourceRenderFailureIsObjectScoped(t *testing.T) {
	source, _, commit, _, service := acceptedSourceDeployment(t, true)
	source.Images = nil
	commit = removeWorkloadLock(t, source, commit)
	objects, err := source.AcceptedManifests(context.Background(), commit, "app")
	if err == nil || len(objects) != 1 || objects[0].GetKind() != "Service" {
		t.Fatalf("render gap must preserve service with explicit error: %+v %v", objects, err)
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	if _, err := source.AcceptedManifest(context.Background(), commit, ref); err != nil {
		t.Fatalf("unrelated workload poisoned service: %v", err)
	}
	ref.Kind, ref.APIVersion, ref.Name = "Deployment", "apps/v1", "consul"
	if _, err := source.AcceptedManifest(context.Background(), commit, ref); err == nil || errors.Is(err, ErrAcceptedManifestAbsent) {
		t.Fatalf("unrenderable workload became absent: %v", err)
	}
	ref.Kind, ref.APIVersion, ref.Name = "NetworkPolicy", "networking.k8s.io/v1", "foreign"
	if _, err := source.AcceptedManifest(context.Background(), commit, ref); !errors.Is(err, ErrAcceptedManifestAbsent) {
		t.Fatalf("unrelated workload poisoned confirmed absence: %v", err)
	}
	service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	ref.Kind, ref.APIVersion, ref.Name = "Service", "v1", "api"
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*service}}, Source: source, SourceCommit: commit}
	if diff, err := view.DiffAgainstSource(ref); err != nil || !diff.Changed {
		t.Fatalf("lost routing evidence during workload gap: %+v %v", diff, err)
	}
}

func TestAcceptedSourceDoesNotReuseImagesAcrossSourceChange(t *testing.T) {
	for _, path := range []string{"consul.yaml", "main.go"} {
		t.Run(path, func(t *testing.T) {
			source, _, _, _, _ := acceptedSourceDeployment(t, true)
			data := []byte("package main\n")
			if path == "consul.yaml" {
				var err error
				data, err = os.ReadFile(filepath.Join(source.Repository, path))
				if err != nil {
					t.Fatal(err)
				}
				data = []byte(strings.Replace(string(data), "consul:1.15", "consul:2", 1))
			}
			if err := os.WriteFile(filepath.Join(source.Repository, path), data, 0600); err != nil {
				t.Fatal(err)
			}
			nativeGit(t, source.Repository, "add", path)
			nativeGit(t, source.Repository, "commit", "-qm", "source C")
			commit := nativeGit(t, source.Repository, "rev-parse", "HEAD")
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "consul"}
			if obj, err := source.AcceptedManifest(context.Background(), commit, ref); err == nil {
				encoded, _ := json.Marshal(obj)
				t.Fatalf("old image reused after source change: %s", encoded)
			}
		})
	}
}

func hasTargetPortChange(event *sdk.DriftEvent) bool {
	for _, field := range event.Fields {
		if field.Path == "spec.ports[0].targetPort" && field.New == "9999" {
			return true
		}
	}
	return false
}
