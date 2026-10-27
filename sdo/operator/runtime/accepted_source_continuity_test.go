package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// A deployed workload that applies source exactly as written, followed by a
// memory commit and maintenance reset: neither may change what accepted source says.
func acceptedSourceDeployment(t *testing.T) (GitPlanSource, *StateTracker, string, *appsv1.Deployment, *corev1.Service) {
	t.Helper()
	source, _ := foreignSource(t)
	manifest := "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: consul\nspec:\n  template:\n    spec:\n      initContainers:\n      - name: setup\n        image: example/setup:latest\n      containers:\n      - name: consul\n        image: consul:1.15\n"
	if err := os.WriteFile(filepath.Join(source.Repository, "consul.yaml"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, source.Repository, "add", ".")
	nativeGit(t, source.Repository, "commit", "-qm", "deploy A")
	commit := nativeGit(t, source.Repository, "rev-parse", "HEAD")
	source.ManifestPaths = []string{"app.yaml", "consul.yaml"}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "consul"}
	raw, err := source.Revision(context.Background(), ref, commit)
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
	commit = seedRenderedLock(t, source, commit, "app")
	return source, tracker, commit, deployed, service
}

func TestHermeticAcceptedSourceAfterMemoryCommit(t *testing.T) {
	source, tracker, deployedCommit, deployed, service := acceptedSourceDeployment(t)
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

func hasTargetPortChange(event *sdk.DriftEvent) bool {
	for _, field := range event.Fields {
		if field.Path == "spec.ports[0].targetPort" && field.New == "9999" {
			return true
		}
	}
	return false
}
