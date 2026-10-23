package runtime

import (
	"context"
	"errors"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"path/filepath"
	"sdo.dev/operator/rendered"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// The accepted source already pins both images, so no build substitution is possible.
// A controller restart before publication must not bless an unrelated ready image.
func TestRV4RestartMustNotBlessChangedPinnedImage(t *testing.T) {
	for _, init := range []bool{false, true} {
		name := "regular"
		if init {
			name = "init"
		}
		t.Run(name, func(t *testing.T) {
			source, _, _, _, service := acceptedSourceDeployment(t, true)
			withoutRenderedLock(t, source)
			manifestPath := filepath.Join(source.Repository, "consul.yaml")
			body, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			pinned := strings.ReplaceAll(string(body), "consul:1.15", "consul@sha256:"+strings.Repeat("b", 64))
			pinned = strings.ReplaceAll(pinned, "example/setup:latest", "example/setup@sha256:"+strings.Repeat("a", 64))
			pinned = strings.Replace(pinned, "spec:\n  template:\n", "spec:\n  replicas: 1\n  selector:\n    matchLabels: {app: consul}\n  template:\n    metadata:\n      labels: {app: consul}\n", 1)
			if err := os.WriteFile(manifestPath, []byte(pinned), 0600); err != nil {
				t.Fatal(err)
			}
			nativeGit(t, source.Repository, "add", "consul.yaml")
			nativeGit(t, source.Repository, "commit", "-qm", "accepted pinned source")
			revision := nativeGit(t, source.Repository, "rev-parse", "HEAD")
			tree := nativeGit(t, source.Repository, "rev-parse", "HEAD^{tree}")
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "consul"}
			raw, err := source.Revision(context.Background(), ref, revision)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := deployment.RenderManifest(raw.Object, "app", tree, nil); err != nil {
				t.Fatal(err)
			}
			live := &appsv1.Deployment{}
			if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(raw.Object, live); err != nil {
				t.Fatal(err)
			}
			wanted := int32(1)
			live.Spec.Replicas = &wanted
			live.Generation = 2
			live.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, ReadyReplicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1}
			// Equivalent to kubectl set image followed by a successful rollout: the
			// source annotation survives, but this digest was never accepted or deployed by SDO.
			changed := "unrelated/incident@sha256:" + strings.Repeat("c", 64)
			if init {
				live.Spec.Template.Spec.InitContainers[0].Image = changed
			} else {
				live.Spec.Template.Spec.Containers[0].Image = changed
			}
			control := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{*live}}, Source: planFakeSource{source: raw}, SourceCommit: revision}
			if diff, err := control.DiffAgainstSource(ref); err != nil || !diff.Changed {
				t.Fatalf("positive control must detect changed pinned image: %+v %v", diff, err)
			}
			t.Log("positive control: comparing the same live object with pinned source reports Changed=true")
			source.Images = nil
			authority, err := broker.New(source.Repository, t.TempDir(), func(context.Context, string, []string) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			locked, err := (RenderedLockReconciler{Source: source, Client: fake.NewSimpleClientset(live, service), Broker: authority}).Reconcile(context.Background(), revision, "app")
			if err != nil {
				t.Logf("rejected changed pinned image: %v", err)
				return
			}
			accepted, err := source.AcceptedManifest(context.Background(), locked, ref)
			if err != nil {
				t.Fatal(err)
			}
			image, _, _ := unstructuredImageForRV4(accepted.Object, init)
			view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{*live}, ServiceList: []corev1.Service{*service}}, Source: source, SourceCommit: locked}
			diff, err := view.DiffAgainstSource(ref)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("accepted source pinned original image, lock image=%s, DiffAgainstSource.Changed=%t, lock commit=%s", image, diff.Changed, locked)
			if image == changed && !diff.Changed {
				t.Fatal("incident image became accepted source and its drift disappeared")
			}
		})
	}
}
func unstructuredImageForRV4(doc map[string]any, init bool) (string, bool, error) {
	field := "containers"
	if init {
		field = "initContainers"
	}
	spec := doc["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	return spec[field].([]any)[0].(map[string]any)["image"].(string), true, nil
}

func TestRV4PublicationFailureMustKeepAcceptedCommit(t *testing.T) {
	source, _, _, deployed, service := acceptedSourceDeployment(t, true)
	revision := withoutRenderedLock(t, source)
	replicas := int32(1)
	deployed.Spec.Replicas = &replicas
	deployed.Generation = 1
	deployed.Status = appsv1.DeploymentStatus{ObservedGeneration: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	client := fake.NewSimpleClientset(deployed, service)
	authority, err := broker.New(source.Repository, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	// A memory publication between initial Head and the broker write causes a
	// genuine expected-head failure, without altering application source.
	client.PrependReactor("get", "deployments", func(ktesting.Action) (bool, kruntime.Object, error) {
		if err := os.WriteFile(filepath.Join(source.Repository, ".sdo/goal.md"), []byte("concurrent memory publication\n"), 0600); err != nil {
			t.Fatal(err)
		}
		nativeGit(t, source.Repository, "add", ".sdo/goal.md")
		nativeGit(t, source.Repository, "commit", "-qm", "concurrent memory publication")
		return false, nil, nil
	})
	reconciler := RenderedLockReconciler{Source: source, Client: client, Broker: authority}
	controller, err := NewController(ControllerConfig{Application: "app", Namespace: "app", SourceCommit: revision, DeployedCommit: revision, FiringThreshold: 1, ClearThreshold: 1, ResponseTimeout: time.Minute, RepairPolicy: "recorded-actions"}, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	controller.ReconcileRendered = func(ctx context.Context, rev string) (string, error) { return reconciler.Reconcile(ctx, rev, "app") }
	controller.OnError = func(err error) { t.Log(err) }
	if err := controller.Step(context.Background(), time.Unix(100, 0), nil); err != nil {
		t.Fatal(err)
	}
	head, headErr := authority.Head(context.Background())
	if headErr != nil {
		t.Fatal(headErr)
	}
	if actual := controller.CurrentAcceptedCommit(); actual == "" || actual != head {
		t.Fatalf("real broker expected-head error replaced accepted revision %s with %q", revision, actual)
	}
}

func TestRV4OldReadyReplicaMustNotCountAsCompletedRollout(t *testing.T) {
	source, _, _, deployed, service := acceptedSourceDeployment(t, true)
	revision := withoutRenderedLock(t, source)
	desired := int32(1)
	deployed.Spec.Replicas = &desired
	deployed.Generation = 2
	// One old replica is available; the new replica exists but is not ready.
	// KubernetesPlanRollouts.Wait correctly rejects this status (Replicas>desired).
	deployed.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 2, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	authority, err := broker.New(source.Repository, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	next, err := (RenderedLockReconciler{Source: source, Client: fake.NewSimpleClientset(deployed, service), Broker: authority}).Reconcile(context.Background(), revision, "app")
	if err != nil {
		t.Logf("partial rollout correctly rejected: %v", err)
		return
	}
	t.Fatalf("lock %s was published while old replica supplies all readiness; new rollout is incomplete", next)
}

func TestRenderedLockRestartWithoutNamedEvidenceHasObjectGap(t *testing.T) {
	source, _, _, deployed, service := acceptedSourceDeployment(t, true)
	revision := withoutRenderedLock(t, source)
	source.Images = nil
	desired := int32(1)
	deployed.Spec.Replicas = &desired
	deployed.Generation = 1
	deployed.Status = appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	authority, err := broker.New(source.Repository, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	locked, err := (RenderedLockReconciler{Source: source, Client: fake.NewSimpleClientset(deployed, service), Broker: authority}).Reconcile(context.Background(), revision, "app")
	if err != nil {
		t.Fatal(err)
	}
	objects, gap := source.AcceptedManifests(context.Background(), locked, "app")
	if !errors.Is(gap, rendered.ErrEvidenceGap) || len(objects) != 1 || objects[0].GetKind() != "Service" {
		t.Fatalf("lost named evidence must preserve Service and explicitly gap workload: %v %v", objects, gap)
	}
}

func TestRenderedLockJobNeedsRequestedCompletions(t *testing.T) {
	source, _, _, _, service := acceptedSourceDeployment(t, true)
	revision := withoutRenderedLock(t, source)
	body := `apiVersion: batch/v1
kind: Job
metadata: {name: consul}
spec:
  completions: 3
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: work
        image: example/work@sha256:` + strings.Repeat("a", 64) + "\n"
	if err := os.WriteFile(filepath.Join(source.Repository, "consul.yaml"), []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, source.Repository, "add", "consul.yaml")
	nativeGit(t, source.Repository, "commit", "-qm", "accepted Job")
	revision = nativeGit(t, source.Repository, "rev-parse", "HEAD")
	tree := nativeGit(t, source.Repository, "rev-parse", "HEAD^{tree}")
	ref := sdk.ObjectRef{APIVersion: "batch/v1", Kind: "Job", Namespace: "app", Name: "consul"}
	raw, err := source.Revision(context.Background(), ref, revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := deployment.RenderManifest(raw.Object, "app", tree, nil); err != nil {
		t.Fatal(err)
	}
	job := &batchv1.Job{}
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(raw.Object, job); err != nil {
		t.Fatal(err)
	}
	job.Status.Succeeded = 1
	client := fake.NewSimpleClientset(job, service)
	authority, err := broker.New(source.Repository, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	reconciler := RenderedLockReconciler{Source: source, Client: client, Broker: authority}
	if _, err := reconciler.Reconcile(context.Background(), revision, "app"); err == nil {
		t.Fatal("Job published after only one of three completions")
	}
	if head, _ := authority.Head(context.Background()); head != revision {
		t.Fatal("incomplete Job advanced accepted head")
	}
	job.Status.Succeeded = 3
	if _, err := client.BatchV1().Jobs("app").UpdateStatus(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	locked, err := reconciler.Reconcile(context.Background(), revision, "app")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.AcceptedManifest(context.Background(), locked, ref); err != nil {
		t.Fatal(err)
	}
}

func TestRenderedLockEmptyPublicationResultKeepsAcceptedRevision(t *testing.T) {
	now := time.Unix(100, 0)
	controller, err := NewController(ControllerConfig{Application: "app", Namespace: "app", SourceCommit: "last-accepted", FiringThreshold: 1, ClearThreshold: 1, ResponseTimeout: time.Minute, RepairPolicy: "recorded-actions"}, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	controller.ReconcileRendered = func(context.Context, string) (string, error) { return "", errors.New("publication failed") }
	if err := controller.Step(context.Background(), now, nil); err != nil {
		t.Fatal(err)
	}
	if actual := controller.CurrentAcceptedCommit(); actual != "last-accepted" {
		t.Fatalf("empty failed reply replaced accepted revision: %q", actual)
	}
}
